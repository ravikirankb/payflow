package repository

import (
	"context"
	"database/sql"

	"github.com/ravikirankb/payflow/internal/model"
)

type OutboxRepository struct {
	db *sql.DB
}

func NewOutboxRepository(db *sql.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) CreateTx(
	ctx context.Context,
	tx *sql.Tx,
	event *model.OutboxEvent,
) error {

	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO outbox_events
            (id, event_type, aggregate_id, payload)
         VALUES ($1, $2, $3, $4)`,
		event.ID,
		event.EventType,
		event.AggregateID,
		event.Payload,
	)

	return err
}

func (r *OutboxRepository) GetPending(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, event_type, aggregate_id, payload, created_at
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.OutboxEvent

	for rows.Next() {
		var e model.OutboxEvent

		if err := rows.Scan(
			&e.ID,
			&e.EventType,
			&e.AggregateID,
			&e.Payload,
			&e.CreatedAt,
		); err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	return events, rows.Err()
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET published_at = NOW()
		WHERE id = $1
	`, id)

	return err
}
