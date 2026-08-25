package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/ravikirankb/payflow/internal/messaging"
	"github.com/ravikirankb/payflow/internal/repository"
)

type OutboxPublisher struct {
	repo     *repository.OutboxRepository
	producer *messaging.KafkaProducer
}

func NewOutboxPublisher(
	repo *repository.OutboxRepository,
	producer *messaging.KafkaProducer,
) *OutboxPublisher {

	return &OutboxPublisher{
		repo:     repo,
		producer: producer,
	}
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

		// Map the domain event to its Kafka topic.
		//
		// EventType is "PaymentCreated", while the Kafka topic
		// is "payments.created". They don't have to be identical.
		topic := "payments.created"

		err := w.producer.Publish(
			ctx,
			topic,
			event.AggregateID,
			event.ID,
			event.Payload,
		)

		if err != nil {
			slog.Error(
				"failed to publish event",
				"event_id", event.ID,
				"error", err,
			)
			continue
		}

		if err := w.repo.MarkPublished(ctx, event.ID); err != nil {
			slog.Error(
				"failed to mark event as published",
				"event_id", event.ID,
				"error", err,
			)
		}
	}
}
