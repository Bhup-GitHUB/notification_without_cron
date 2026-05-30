package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification_without_cron/internal/config"
	"notification_without_cron/internal/db"
	"notification_without_cron/internal/job"
	"notification_without_cron/internal/queue"

	amqp "github.com/rabbitmq/amqp091-go"
)

type worker struct {
	store *db.Store
	queue *queue.Client
	http  *http.Client
}

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	q, err := queue.Connect(cfg.RabbitMQURL, cfg.QueueName, cfg.DLQName)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	deliveries, err := q.Consume()
	if err != nil {
		log.Fatal(err)
	}

	w := worker{
		store: db.NewStore(pool),
		queue: q,
		http:  &http.Client{Timeout: 10 * time.Second},
	}

	log.Print("worker started")
	for {
		select {
		case <-ctx.Done():
			log.Print("worker stopped")
			return
		case delivery, ok := <-deliveries:
			if !ok {
				log.Print("worker delivery channel closed")
				return
			}
			w.handle(ctx, delivery)
		}
	}
}

func (w worker) handle(ctx context.Context, delivery amqp.Delivery) {
	msg, err := queue.DecodeMessage(delivery)
	if err != nil {
		log.Printf("decode message failed: %v", err)
		_ = delivery.Ack(false)
		return
	}

	log.Printf("job consumed id=%s", msg.JobID)
	saved, err := w.store.GetJob(ctx, msg.JobID)
	if err != nil {
		log.Printf("load job failed id=%s error=%v", msg.JobID, err)
		_ = delivery.Nack(false, true)
		return
	}
	if saved.Status == job.StatusCompleted || saved.Status == job.StatusDLQ {
		_ = delivery.Ack(false)
		return
	}

	if err := w.callCallback(ctx, msg); err == nil {
		if err := w.store.MarkCompleted(ctx, msg.JobID); err != nil {
			log.Printf("mark completed failed id=%s error=%v", msg.JobID, err)
			_ = delivery.Nack(false, true)
			return
		}
		log.Printf("callback success id=%s", msg.JobID)
		_ = delivery.Ack(false)
		return
	} else {
		w.handleFailure(ctx, delivery, msg, saved, err)
	}
}

func (w worker) handleFailure(ctx context.Context, delivery amqp.Delivery, msg job.QueueMessage, saved job.Job, callbackErr error) {
	lastError := callbackErr.Error()
	nextRetry := saved.RetryCount + 1
	log.Printf("callback failure id=%s retry=%d error=%s", msg.JobID, nextRetry, lastError)

	if nextRetry < saved.MaxRetries {
		nextRunAt := time.Now().Add(backoff(nextRetry))
		updated, err := w.store.ScheduleRetry(ctx, msg.JobID, nextRunAt, lastError)
		if err != nil {
			log.Printf("schedule retry failed id=%s error=%v", msg.JobID, err)
			_ = delivery.Nack(false, true)
			return
		}
		log.Printf("retry scheduled id=%s retry=%d next_run_at=%s", updated.ID, updated.RetryCount, updated.NextRunAt.Format(time.RFC3339))
		_ = delivery.Ack(false)
		return
	}

	updated, err := w.store.MoveToDLQ(ctx, msg.JobID, lastError)
	if err != nil {
		log.Printf("move to dlq failed id=%s error=%v", msg.JobID, err)
		_ = delivery.Nack(false, true)
		return
	}
	if err := w.queue.PublishDLQ(ctx, msg); err != nil {
		log.Printf("publish dlq failed id=%s error=%v", msg.JobID, err)
		_ = delivery.Nack(false, true)
		return
	}
	log.Printf("moved to dlq id=%s retry=%d", updated.ID, updated.RetryCount)
	_ = delivery.Ack(false)
}

func (w worker) callCallback(ctx context.Context, msg job.QueueMessage) error {
	reqBody := job.CallbackRequest{
		JobID:          msg.JobID,
		IdempotencyKey: msg.IdempotencyKey,
		Payload:        msg.Payload,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, msg.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if len(respBody) > 0 {
		return fmt.Errorf("callback returned %d: %s", resp.StatusCode, string(respBody))
	}
	return fmt.Errorf("callback returned %d", resp.StatusCode)
}

func backoff(retry int) time.Duration {
	if retry < 1 {
		retry = 1
	}
	return time.Duration(5*(1<<(retry-1))) * time.Second
}
