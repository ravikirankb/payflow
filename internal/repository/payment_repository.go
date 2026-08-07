package repository

import (
	"context"
	"database/sql"
)

type PaymentRepository struct {
	db *sql.DB
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}

func (r *PaymentRepository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}
