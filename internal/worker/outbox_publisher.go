package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/ravikirankb/payflow/internal/repository"
)

type OutboxPublisher struct {
	repo *repository.OutboxRepository
}

func NewOutboxPublisher(repo *repository.OutboxRepository) *OutboxPublisher {
	return &OutboxPublisher{repo: repo}
}

func (w *OutboxPublisher) Start(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	slog.Info("outbox publisher started")

	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox publisher stopped")
			return

		case <-ticker.C:
			w.publishBatch(ctx)
		}
	}
}

func (w *OutboxPublisher) publishBatch(ctx context.Context) {
	events, err := w.repo.GetPending(ctx, 10)
	if err != nil {
		slog.Error("failed to fetch outbox events", "error", err)
		return
	}

	for _, event := range events {

		// Simulate publishing to Kafka
		slog.Info(
			"published outbox event",
			"event_id", event.ID,
			"event_type", event.EventType,
			"aggregate_id", event.AggregateID,
			"payload", string(event.Payload),
		)

		if err := w.repo.MarkPublished(ctx, event.ID); err != nil {
			slog.Error(
				"failed to mark event as published",
				"event_id", event.ID,
				"error", err,
			)
		}
	}
}
