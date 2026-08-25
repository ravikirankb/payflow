package repository

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

// LedgerEntry represents the business record created from a
// PaymentCreated Kafka event.
type LedgerEntry struct {
	ID        uuid.UUID
	EventID   string
	PaymentID uuid.UUID
	Amount    int64
	Currency  string
}

type LedgerRepository struct {
	db *sql.DB
}

func NewLedgerRepository(db *sql.DB) *LedgerRepository {
	return &LedgerRepository{
		db: db,
	}
}

// ProcessPayment performs the business operation and records the
// processed Kafka event atomically.
//
// PostgreSQL's PRIMARY KEY on processed_events.event_id is used
// as the idempotency gate.
//
// New event:
//
//	processed_events INSERT → ledger INSERT → COMMIT
//
// Duplicate event:
//
//	processed_events INSERT → conflict → skip → COMMIT
//
// Failure:
//
//	ROLLBACK
func (r *LedgerRepository) ProcessPayment(
	ctx context.Context,
	entry LedgerEntry,
) error {

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Any error before Commit rolls the entire transaction back.
	defer tx.Rollback()

	// Try to claim the event.
	//
	// Because event_id is the PRIMARY KEY, only one transaction
	// can successfully claim a particular event.
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO processed_events (event_id)
		 VALUES ($1)
		 ON CONFLICT (event_id) DO NOTHING`,
		entry.EventID,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	// The event already exists.
	//
	// This is expected behaviour with Kafka's at-least-once delivery.
	if rowsAffected == 0 {
		return tx.Commit()
	}

	// This is a new event, so perform the actual business operation.
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO payment_ledger (
			id,
			event_id,
			payment_id,
			amount,
			currency
		)
		VALUES ($1, $2, $3, $4, $5)`,
		entry.ID,
		entry.EventID,
		entry.PaymentID,
		entry.Amount,
		entry.Currency,
	)
	if err != nil {
		return err
	}

	// The processed-event marker and ledger entry are committed
	// together as one atomic transaction.
	return tx.Commit()
}
