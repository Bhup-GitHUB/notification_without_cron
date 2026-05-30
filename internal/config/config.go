package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	RabbitMQURL    string
	QueueName      string
	DLQName        string
	PartitionCount int
	BatchSize      int
	ScanInterval   time.Duration
	MaxRetries     int
}

func Load() Config {
	return Config{
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		DatabaseURL:    env("DATABASE_URL", "postgres://clockwork:clockwork@localhost:5432/clockwork?sslmode=disable"),
		RabbitMQURL:    env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:      env("QUEUE_NAME", "scheduled_jobs"),
		DLQName:        env("DLQ_NAME", "scheduled_jobs_dlq"),
		PartitionCount: envInt("PARTITION_COUNT", 16),
		BatchSize:      envInt("BATCH_SIZE", 25),
		ScanInterval:   time.Duration(envInt("SCAN_INTERVAL_SECONDS", 3)) * time.Second,
		MaxRetries:     envInt("MAX_RETRIES", 3),
	}
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
