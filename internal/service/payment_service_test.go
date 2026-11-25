package service

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

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

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
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

	const workers = 100
	key := fmt.Sprintf("concurrency-test-%d", time.Now().UnixNano())

	var wg sync.WaitGroup
	wg.Add(workers)

	start := make(chan struct{})
	results := make(chan string, workers)
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			<-start

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

	// Release all workers at approximately the same time.
	close(start)

	wg.Wait()
	close(results)
	close(errs)

	ids := make(map[string]int)

	for id := range results {
		ids[id]++
	}

	for err := range errs {
		t.Errorf("REQUEST ERROR: %v", err)
	}

	t.Logf("Successful results: %d", len(ids))

	for id := range results {
		ids[id]++
	}

	if len(ids) != 1 {
		t.Fatalf("expected exactly one payment ID, got %d: %v", len(ids), ids)
	}

	total := 0
	for _, count := range ids {
		total += count
	}

	if total != workers {
		t.Fatalf("expected %d successful results, got %d", workers, total)
	}

	var paymentID string
	for id := range ids {
		paymentID = id
	}

	var paymentCount int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM payments WHERE id = $1",
		paymentID,
	).Scan(&paymentCount)
	if err != nil {
		t.Fatalf("failed to count payments: %v", err)
	}

	if paymentCount != 1 {
		t.Fatalf("expected 1 payment row for this test, got %d", paymentCount)
	}

	var outboxCount int
	err = db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1",
		paymentID,
	).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("failed to count outbox events: %v", err)
	}

	if outboxCount != 1 {
		t.Fatalf("expected 1 outbox event for this payment, got %d", outboxCount)
	}
}

func TestCreatePayment_ConcurrentDifferentKeys(t *testing.T) {
	svc, db := setupTestService(t)
	defer db.Close()

	ctx := context.Background()

	const workers = 100
	const keyCount = 10

	baseKey := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())

	type result struct {
		key       string
		paymentID string
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	start := make(chan struct{})
	results := make(chan result, workers)
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		key := fmt.Sprintf("%s-key-%d", baseKey, i%keyCount)

		go func(key string) {
			defer wg.Done()

			<-start

			payment, err := svc.CreatePayment(ctx, 5000, "INR", key)
			if err != nil {
				errs <- fmt.Errorf("key %s: %w", key, err)
				return
			}

			results <- result{
				key:       key,
				paymentID: payment.ID,
			}
		}(key)
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("REQUEST ERROR: %v", err)
	}

	paymentsByKey := make(map[string]map[string]int)
	total := 0

	for r := range results {
		total++

		if paymentsByKey[r.key] == nil {
			paymentsByKey[r.key] = make(map[string]int)
		}

		paymentsByKey[r.key][r.paymentID]++
	}

	if total != workers {
		t.Fatalf("expected %d successful requests, got %d", workers, total)
	}

	if len(paymentsByKey) != keyCount {
		t.Fatalf("expected %d keys, got %d", keyCount, len(paymentsByKey))
	}

	for key, ids := range paymentsByKey {
		if len(ids) != 1 {
			t.Errorf("key %s produced %d payment IDs: %v", key, len(ids), ids)
			continue
		}

		var paymentID string
		for id := range ids {
			paymentID = id
		}

		var count int

		err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key = $1 AND payment_id = $2",
			key, paymentID,
		).Scan(&count)
		if err != nil {
			t.Fatalf("failed to count idempotency records: %v", err)
		}
		if count != 1 {
			t.Errorf("key %s: expected 1 idempotency record, got %d", key, count)
		}

		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM payments WHERE id = $1",
			paymentID,
		).Scan(&count)
		if err != nil {
			t.Fatalf("failed to count payments: %v", err)
		}
		if count != 1 {
			t.Errorf("key %s: expected 1 payment, got %d", key, count)
		}

		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1",
			paymentID,
		).Scan(&count)
		if err != nil {
			t.Fatalf("failed to count outbox events: %v", err)
		}
		if count != 1 {
			t.Errorf("key %s: expected 1 outbox event, got %d", key, count)
		}
	}

	t.Logf("Successfully processed %d requests across %d idempotency keys",
		total, keyCount)
}
