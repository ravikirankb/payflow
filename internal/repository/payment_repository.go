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

func (r *PaymentRepository) CreateTx(ctx context.Context, tx *sql.Tx, p *model.Payment) error {
	query := `
        INSERT INTO payments (id, amount, currency, status)
        VALUES ($1, $2, $3, $4)
    `

	_, err := tx.ExecContext(
		ctx,
		query,
		p.ID,
		p.Amount,
		p.Currency,
		p.Status,
	)

	return err
}

func (r *PaymentRepository) GetByIDTx(ctx context.Context, tx *sql.Tx, id string) (*model.Payment, error) {
	query := `
        SELECT id, amount, currency, status, created_at
        FROM payments
        WHERE id = $1
    `

	var p model.Payment

	err := tx.QueryRowContext(ctx, query, id).Scan(
		&p.ID,
		&p.Amount,
		&p.Currency,
		&p.Status,
		&p.CreatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &p, nil
}
