package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"notification_without_cron/internal/config"
	"notification_without_cron/internal/db"
	"notification_without_cron/internal/job"

	"github.com/google/uuid"
)

type server struct {
	store *db.Store
	cfg   config.Config
}

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool, "migrations/001_create_jobs.sql"); err != nil {
		log.Fatal(err)
	}

	s := server{store: db.NewStore(pool), cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("GET /jobs/", s.getJob)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /metrics", s.metrics)

	log.Printf("clockwork api listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s server) createJob(w http.ResponseWriter, r *http.Request) {
	var req job.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := validateCreate(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	partitionID := job.PartitionFor(req.IdempotencyKey, s.cfg.PartitionCount)
	saved, inserted, err := s.store.CreateJob(r.Context(), req, uuid.NewString(), partitionID, s.cfg.MaxRetries)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create job")
		return
	}

	if inserted {
		log.Printf("job created id=%s idempotency_key=%s partition=%d", saved.ID, saved.IdempotencyKey, saved.PartitionID)
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	writeJSON(w, saved)
}

func (s server) getJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	saved, err := s.store.GetJob(r.Context(), id)
	if errors.Is(err, db.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load job")
		return
	}
	writeJSON(w, saved)
}

func (s server) listJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status != "" && !validStatus(status) {
		writeError(w, http.StatusBadRequest, "invalid status")
		return
	}
	jobs, err := s.store.ListJobs(r.Context(), status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	writeJSON(w, jobs)
}

func (s server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s server) metrics(w http.ResponseWriter, r *http.Request) {
	counts, err := s.store.StatusCounts(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load metrics")
		return
	}
	failedCallbacks, err := s.store.FailedCallbackCount(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load metrics")
		return
	}
	counts["failed_callback_count"] = failedCallbacks
	writeJSON(w, counts)
}

func validateCreate(req job.CreateRequest) error {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return errors.New("idempotency_key is required")
	}
	parsed, err := url.ParseRequestURI(req.CallbackURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("callback_url must be absolute")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("callback_url must use http or https")
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		return errors.New("payload must be valid json")
	}
	if req.ExecuteAt.IsZero() {
		return errors.New("execute_at is required")
	}
	if req.ExecuteAt.Before(time.Now().Add(-1 * time.Minute)) {
		return errors.New("execute_at is too far in the past")
	}
	return nil
}

func validStatus(status string) bool {
	switch status {
	case job.StatusScheduled, job.StatusQueued, job.StatusCompleted, job.StatusRetryScheduled, job.StatusDLQ:
		return true
	default:
		return false
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
