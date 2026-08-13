package service

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/ravikirankb/payflow/internal/repository"
)

func setupTestService(t *testing.T) (*PaymentService, *sql.DB) {
	t.Helper()

	dsn := os.Getenv("PAYFLOW_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/payflow?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping db: %v", err)
	}

	paymentRepo := repository.NewPaymentRepository(db)
	idempotencyRepo := repository.NewIdempotencyRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)

	svc := NewPaymentService(
		db,
		paymentRepo,
		idempotencyRepo,
		outboxRepo,
	)

	return svc, db
}

func TestCreatePayment_IdempotentConcurrency(t *testing.T) {
	svc, db := setupTestService(t)
	defer db.Close()

	ctx := context.Background()

	_, _ = db.ExecContext(ctx, "DELETE FROM outbox_events")
	_, _ = db.ExecContext(ctx, "DELETE FROM idempotency_keys")
	_, _ = db.ExecContext(ctx, "DELETE FROM payments")

	const workers = 100
	const key = "concurrency-test-key"

	var wg sync.WaitGroup
	wg.Add(workers)

	results := make(chan string, workers)
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			payment, err := svc.CreatePayment(
				ctx,
				5000,
				"INR",
				key,
			)

			if err != nil {
				errs <- err
				return
			}

			results <- payment.ID
		}()
	}

	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	ids := make(map[string]int)

	for id := range results {
		ids[id]++
	}

	if len(ids) != 1 {
		t.Fatalf("expected exactly one payment ID, got %d: %v", len(ids), ids)
	}

	var paymentCount int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM payments",
	).Scan(&paymentCount)
	if err != nil {
		t.Fatalf("failed to count payments: %v", err)
	}

	if paymentCount != 1 {
		t.Fatalf("expected 1 payment row, got %d", paymentCount)
	}

	var outboxCount int
	err = db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM outbox_events",
	).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("failed to count outbox events: %v", err)
	}

	if outboxCount != 1 {
		t.Fatalf("expected 1 outbox event, got %d", outboxCount)
	}
}
