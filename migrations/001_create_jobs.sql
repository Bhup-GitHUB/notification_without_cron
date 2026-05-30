CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    callback_url TEXT NOT NULL,
    payload JSONB NOT NULL,
    execute_at TIMESTAMPTZ NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    partition_id INT NOT NULL,
    status TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_next_run_partition
ON jobs (status, next_run_at, partition_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
ON jobs (idempotency_key);
