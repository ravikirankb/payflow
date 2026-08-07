package repository

import (
	"context"
	"database/sql"

	"github.com/ravikirankb/payflow/internal/model"
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

func (r *PaymentRepository) Create(ctx context.Context, p *model.Payment) error {
	query := `INSERT INTO payments(id, amount, currency, status)
	        VALUES ($1,$2,$3,$4)
			`

	_, err := r.db.ExecContext(ctx,
		query,
		p.ID,
		p.Amount,
		p.Currency,
		p.Status,
	)

	return err
}
