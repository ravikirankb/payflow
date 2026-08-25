package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/ravikirankb/payflow/internal/config"
	"github.com/ravikirankb/payflow/internal/database"
	"github.com/ravikirankb/payflow/internal/messaging"
	"github.com/ravikirankb/payflow/internal/repository"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	// ctx represents the lifetime of this consumer process.
	// When SIGINT (Ctrl+C) or SIGTERM is received, ctx is cancelled
	// and the consumer exits gracefully.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	cfg := config.Load()

	db, err := database.Init(cfg.DATABASE_URL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Create the repository used by the Kafka consumer
	// to perform the business operation atomically.
	ledgerRepo := repository.NewLedgerRepository(db)

	client, err := kgo.NewClient(
		// Redpanda exposes the Kafka protocol on port 9092.
		kgo.SeedBrokers("localhost:9092"),

		// Consumers using the same group share the work.
		// Kafka assigns partitions among the group members.
		kgo.ConsumerGroup("payflow-consumer-group"),

		// Subscribe this consumer to our payment-created events.
		kgo.ConsumeTopics("payments.created"),
	)
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	slog.Info("consumer started")

	for {
		fetches := client.PollFetches(ctx)

		// PollFetches returns when records are available,
		// when an error occurs, or when the context is cancelled.
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error(
					"kafka fetch error",
					"topic", e.Topic,
					"partition", e.Partition,
					"error", e.Err,
				)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {

			// ---------------------------------------------------------
			// 1. Extract the event ID from the Kafka header.
			//
			// The producer puts the outbox event ID into the "event_id"
			// header. This ID uniquely identifies this specific event.
			// ---------------------------------------------------------

			var eventID string

			for _, header := range record.Headers {
				if header.Key == "event_id" {
					eventID = string(header.Value)
					break
				}
			}

			if eventID == "" {
				slog.Error(
					"event missing event_id header",
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
				)

				// Don't commit this record.
				// Without an event ID we cannot safely perform idempotent
				// processing.
				return
			}

			// ---------------------------------------------------------
			// 2. Deserialize the PaymentCreated event.
			// ---------------------------------------------------------

			var event messaging.PaymentCreatedEvent

			if err := json.Unmarshal(record.Value, &event); err != nil {
				slog.Error(
					"failed to deserialize payment event",
					"event_id", eventID,
					"error", err,
				)

				// Don't commit the offset.
				return
			}

			// ---------------------------------------------------------
			// 3. Convert the Kafka event into our ledger entry.
			// ---------------------------------------------------------

			paymentID, err := uuid.Parse(event.ID)
			if err != nil {
				slog.Error(
					"invalid payment ID",
					"event_id", eventID,
					"payment_id", event.ID,
					"error", err,
				)
				return
			}

			// ---------------------------------------------------------
			// 4. Process the payment inside PostgreSQL.
			//
			// ProcessPayment performs:
			//
			//   processed_events INSERT
			//          +
			//   payment_ledger INSERT
			//
			// inside ONE transaction.
			//
			// If this returns successfully, PostgreSQL has committed
			// the operation OR determined that this event was already
			// processed.
			// ---------------------------------------------------------

			entry := repository.LedgerEntry{
				ID:        uuid.New(),
				EventID:   eventID,
				PaymentID: paymentID,
				Amount:    event.Amount,
				Currency:  event.Currency,
			}

			if err := ledgerRepo.ProcessPayment(ctx, entry); err != nil {
				slog.Error(
					"failed to process payment event",
					"event_id", eventID,
					"payment_id", paymentID,
					"error", err,
				)

				// IMPORTANT:
				// Do NOT commit the Kafka offset.
				//
				// Kafka will deliver the event again.
				return
			}

			// ---------------------------------------------------------
			// 5. PostgreSQL succeeded.
			//
			// NOW we can commit the Kafka offset.
			// ---------------------------------------------------------

			if err := client.CommitRecords(ctx, record); err != nil {
				slog.Error(
					"failed to commit kafka offset",
					"event_id", eventID,
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"error", err,
				)

				return
			}

			slog.Info(
				"payment event processed successfully",
				"event_id", eventID,
				"payment_id", paymentID,
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
			)
		})

	}
}
