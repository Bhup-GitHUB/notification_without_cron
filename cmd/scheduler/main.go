package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification_without_cron/internal/config"
	"notification_without_cron/internal/db"
	"notification_without_cron/internal/queue"
)

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

	store := db.NewStore(pool)
	ticker := time.NewTicker(cfg.ScanInterval)
	defer ticker.Stop()

	log.Printf("scheduler started interval=%s batch_size=%d", cfg.ScanInterval, cfg.BatchSize)
	scan(ctx, store, q, cfg.BatchSize)

	for {
		select {
		case <-ctx.Done():
			log.Print("scheduler stopped")
			return
		case <-ticker.C:
			scan(ctx, store, q, cfg.BatchSize)
		}
	}
}

func scan(ctx context.Context, store *db.Store, q *queue.Client, batchSize int) {
	jobs, err := store.ClaimDueJobs(ctx, batchSize)
	if err != nil {
		log.Printf("claim due jobs failed: %v", err)
		return
	}
	for _, saved := range jobs {
		log.Printf("job claimed id=%s partition=%d", saved.ID, saved.PartitionID)
		if err := q.PublishJob(ctx, saved); err != nil {
			log.Printf("publish job failed id=%s error=%v", saved.ID, err)
			continue
		}
		log.Printf("job published id=%s queue=scheduled_jobs", saved.ID)
	}
}
