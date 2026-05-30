package job

import (
	"encoding/json"
	"time"
)

const (
	StatusScheduled      = "scheduled"
	StatusQueued         = "queued"
	StatusCompleted      = "completed"
	StatusRetryScheduled = "retry_scheduled"
	StatusDLQ            = "dlq"
)

type Job struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	CallbackURL    string          `json:"callback_url"`
	Payload        json.RawMessage `json:"payload"`
	ExecuteAt      time.Time       `json:"execute_at"`
	NextRunAt      time.Time       `json:"next_run_at"`
	PartitionID    int             `json:"partition_id"`
	Status         string          `json:"status"`
	RetryCount     int             `json:"retry_count"`
	MaxRetries     int             `json:"max_retries"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type CreateRequest struct {
	CallbackURL    string          `json:"callback_url"`
	Payload        json.RawMessage `json:"payload"`
	ExecuteAt      time.Time       `json:"execute_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type QueueMessage struct {
	JobID          string          `json:"job_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	CallbackURL    string          `json:"callback_url"`
	Payload        json.RawMessage `json:"payload"`
}

type CallbackRequest struct {
	JobID          string          `json:"job_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}
