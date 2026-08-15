package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("payflow-consumer-group"),
		kgo.ConsumeTopics("payments.created"),
	)
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	slog.Info("consumer started")

	for {
		select {
		case <-ctx.Done():
			slog.Info("consumer shutting down")
			return

		default:
			fetches := client.PollFetches(ctx)

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
				slog.Info(
					"received payment event",
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"key", string(record.Key),
					"value", string(record.Value),
				)
			})
		}
	}
}
