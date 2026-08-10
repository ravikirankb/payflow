package repository

import (
	"context"
	"database/sql"
)

type IdempotencyRepository struct {
	db *sql.DB
}

func NewIdempotencyRepository(db *sql.DB) *IdempotencyRepository {
	return &IdempotencyRepository{db: db}
}

func (r *IdempotencyRepository) GetPaymentID(ctx context.Context, tx *sql.Tx, key string) (string, error) {
	var paymentID string

	err := tx.QueryRowContext(
		ctx,
		`SELECT payment_id FROM idempotency_keys WHERE idempotency_key = $1`,
		key,
	).Scan(&paymentID)

	if err == sql.ErrNoRows {
		return "", nil
	}

	return paymentID, err
}

func (r *IdempotencyRepository) Save(ctx context.Context, tx *sql.Tx, key, paymentID string) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO idempotency_keys (idempotency_key, payment_id)
         VALUES ($1, $2)`,
		key,
		paymentID,
	)

	return err
}
