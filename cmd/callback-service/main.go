package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"notification_without_cron/internal/config"
	"notification_without_cron/internal/job"
)

type callbackServer struct {
	mu        sync.Mutex
	processed map[string]bool
	attempts  map[string]int
	failures  int
}

func main() {
	cfg := config.Load()
	s := &callbackServer{
		processed: map[string]bool{},
		attempts:  map[string]int{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /callback", s.callback)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /metrics", s.metrics)

	log.Printf("callback service listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *callbackServer) callback(w http.ResponseWriter, r *http.Request) {
	var req job.CallbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.IdempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key is required")
		return
	}

	s.mu.Lock()
	if s.processed[req.IdempotencyKey] {
		s.mu.Unlock()
		writeJSON(w, map[string]any{"status": "duplicate"})
		return
	}
	s.attempts[req.IdempotencyKey]++
	attempt := s.attempts[req.IdempotencyKey]
	s.mu.Unlock()

	behavior := map[string]any{}
	if len(req.Payload) > 0 {
		_ = json.Unmarshal(req.Payload, &behavior)
	}

	if forceFail, _ := behavior["force_fail"].(bool); forceFail {
		s.recordFailure()
		log.Printf("callback failure id=%s idempotency_key=%s attempt=%d", req.JobID, req.IdempotencyKey, attempt)
		writeError(w, http.StatusInternalServerError, "forced failure")
		return
	}

	if failUntil, ok := numberValue(behavior["fail_until_retry"]); ok && attempt <= failUntil {
		s.recordFailure()
		log.Printf("callback failure id=%s idempotency_key=%s attempt=%d", req.JobID, req.IdempotencyKey, attempt)
		writeError(w, http.StatusInternalServerError, "planned retry failure")
		return
	}

	s.mu.Lock()
	s.processed[req.IdempotencyKey] = true
	s.mu.Unlock()

	log.Printf("callback success id=%s idempotency_key=%s attempt=%d", req.JobID, req.IdempotencyKey, attempt)
	writeJSON(w, map[string]any{"status": "processed", "attempt": attempt})
}

func (s *callbackServer) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *callbackServer) metrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, map[string]int{
		"processed": len(s.processed),
		"attempts":  totalAttempts(s.attempts),
		"failures":  s.failures,
	})
}

func (s *callbackServer) recordFailure() {
	s.mu.Lock()
	s.failures++
	s.mu.Unlock()
}

func totalAttempts(attempts map[string]int) int {
	total := 0
	for _, count := range attempts {
		total += count
	}
	return total
}

func numberValue(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	default:
		return 0, false
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("json response failed: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
