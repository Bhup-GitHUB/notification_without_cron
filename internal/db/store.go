package db

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"notification_without_cron/internal/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("job not found")

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) CreateJob(ctx context.Context, create job.CreateRequest, id string, partitionID int, maxRetries int) (job.Job, bool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			id, idempotency_key, callback_url, payload, execute_at, next_run_at,
			partition_id, status, retry_count, max_retries, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $5, $6, $7, 0, $8, now(), now())
		ON CONFLICT (idempotency_key) DO UPDATE
		SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id, idempotency_key, callback_url, payload, execute_at, next_run_at,
			partition_id, status, retry_count, max_retries, last_error, created_at, updated_at,
			(xmax = 0) AS inserted
	`, id, create.IdempotencyKey, create.CallbackURL, create.Payload, create.ExecuteAt, partitionID, job.StatusScheduled, maxRetries)

	var saved job.Job
	var inserted bool
	err := scanJobWithInserted(row, &saved, &inserted)
	return saved, inserted, err
}

func (s *Store) GetJob(ctx context.Context, id string) (job.Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, callback_url, payload, execute_at, next_run_at,
			partition_id, status, retry_count, max_retries, last_error, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, id)
	return scanJob(row)
}

func (s *Store) ListJobs(ctx context.Context, status string) ([]job.Job, error) {
	var rows pgx.Rows
	var err error
	if status == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, idempotency_key, callback_url, payload, execute_at, next_run_at,
				partition_id, status, retry_count, max_retries, last_error, created_at, updated_at
			FROM jobs
			ORDER BY created_at DESC
			LIMIT 100
		`)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, idempotency_key, callback_url, payload, execute_at, next_run_at,
				partition_id, status, retry_count, max_retries, last_error, created_at, updated_at
			FROM jobs
			WHERE status = $1
			ORDER BY created_at DESC
			LIMIT 100
		`, status)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []job.Job
	for rows.Next() {
		saved, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, saved)
	}
	return jobs, rows.Err()
}

func (s *Store) ClaimDueJobs(ctx context.Context, limit int) ([]job.Job, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH due AS (
			SELECT id
			FROM jobs
			WHERE status IN ($1, $2)
				AND next_run_at <= now()
			ORDER BY next_run_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET status = $4, updated_at = now()
		FROM due
		WHERE jobs.id = due.id
		RETURNING jobs.id, jobs.idempotency_key, jobs.callback_url, jobs.payload, jobs.execute_at,
			jobs.next_run_at, jobs.partition_id, jobs.status, jobs.retry_count, jobs.max_retries,
			jobs.last_error, jobs.created_at, jobs.updated_at
	`, job.StatusScheduled, job.StatusRetryScheduled, limit, job.StatusQueued)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var claimed []job.Job
	for rows.Next() {
		saved, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, saved)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Store) MarkCompleted(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $1, last_error = NULL, updated_at = now()
		WHERE id = $2
	`, job.StatusCompleted, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ScheduleRetry(ctx context.Context, id string, nextRunAt time.Time, lastError string) (job.Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = $1,
			retry_count = retry_count + 1,
			next_run_at = $2,
			last_error = $3,
			updated_at = now()
		WHERE id = $4
		RETURNING id, idempotency_key, callback_url, payload, execute_at, next_run_at,
			partition_id, status, retry_count, max_retries, last_error, created_at, updated_at
	`, job.StatusRetryScheduled, nextRunAt, lastError, id)
	return scanJob(row)
}

func (s *Store) MoveToDLQ(ctx context.Context, id string, lastError string) (job.Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = $1,
			retry_count = retry_count + 1,
			last_error = $2,
			updated_at = now()
		WHERE id = $3
		RETURNING id, idempotency_key, callback_url, payload, execute_at, next_run_at,
			partition_id, status, retry_count, max_retries, last_error, created_at, updated_at
	`, job.StatusDLQ, lastError, id)
	return scanJob(row)
}

func (s *Store) StatusCounts(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, count(*)
		FROM jobs
		GROUP BY status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]int{
		job.StatusScheduled:      0,
		job.StatusQueued:         0,
		job.StatusCompleted:      0,
		job.StatusRetryScheduled: 0,
		job.StatusDLQ:            0,
	}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

func scanJob(row pgx.Row) (job.Job, error) {
	var saved job.Job
	var payload []byte
	err := row.Scan(
		&saved.ID,
		&saved.IdempotencyKey,
		&saved.CallbackURL,
		&payload,
		&saved.ExecuteAt,
		&saved.NextRunAt,
		&saved.PartitionID,
		&saved.Status,
		&saved.RetryCount,
		&saved.MaxRetries,
		&saved.LastError,
		&saved.CreatedAt,
		&saved.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	if err != nil {
		return job.Job{}, err
	}
	saved.Payload = json.RawMessage(payload)
	return saved, nil
}

func scanJobWithInserted(row pgx.Row, saved *job.Job, inserted *bool) error {
	var payload []byte
	err := row.Scan(
		&saved.ID,
		&saved.IdempotencyKey,
		&saved.CallbackURL,
		&payload,
		&saved.ExecuteAt,
		&saved.NextRunAt,
		&saved.PartitionID,
		&saved.Status,
		&saved.RetryCount,
		&saved.MaxRetries,
		&saved.LastError,
		&saved.CreatedAt,
		&saved.UpdatedAt,
		inserted,
	)
	if err != nil {
		return err
	}
	saved.Payload = json.RawMessage(payload)
	return nil
}
