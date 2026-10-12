package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"

	"github.com/ravikirankb/payflow/internal/cache"
	"github.com/ravikirankb/payflow/internal/metrics"
	"github.com/ravikirankb/payflow/internal/model"
	"github.com/ravikirankb/payflow/internal/repository"
)

type PaymentService struct {
	db              *sql.DB
	paymentRepo     *repository.PaymentRepository
	idempotencyRepo *repository.IdempotencyRepository
	outboxRepo      *repository.OutboxRepository
	paymentCache    *cache.PaymentCache
}

func NewPaymentService(
	db *sql.DB,
	paymentRepo *repository.PaymentRepository,
	idempotencyRepo *repository.IdempotencyRepository,
	outboxRepo *repository.OutboxRepository,
) *PaymentService {
	return &PaymentService{
		db:              db,
		paymentRepo:     paymentRepo,
		idempotencyRepo: idempotencyRepo,
		outboxRepo:      outboxRepo,
	}
}

func (s *PaymentService) SetPaymentCache(c *cache.PaymentCache) {
	s.paymentCache = c
}

func cacheKey(idempotencyKey string) string {
	return "payflow:idempotency:" + idempotencyKey
}

func (s *PaymentService) cachePaymentID(
	ctx context.Context,
	key string,
	paymentID string,
) {
	if s.paymentCache == nil {
		return
	}

	if err := s.paymentCache.SetPaymentID(ctx, cacheKey(key), paymentID); err != nil {
		slog.Warn("failed to cache payment ID",
			"error", err,
		)
	}
}

func (s *PaymentService) CreatePayment(
	ctx context.Context,
	amount int64,
	currency string,
	idempotencyKey string,
) (*model.Payment, error) {
	// Redis is an optimization. PostgreSQL remains authoritative.
	if s.paymentCache != nil {
		paymentID, err := s.paymentCache.GetPaymentID(
			ctx,
			cacheKey(idempotencyKey),
		)

		if err == nil && paymentID != "" {
			tx, txErr := s.db.BeginTx(ctx, nil)
			if txErr != nil {
				return nil, txErr
			}

			payment, getErr := s.paymentRepo.GetByIDTx(ctx, tx, paymentID)
			_ = tx.Rollback()

			if getErr == nil {
				return payment, nil
			}

			if !errors.Is(getErr, sql.ErrNoRows) {
				return nil, getErr
			}

			// Stale cache entry: continue through PostgreSQL lookup.
		} else if err != nil && !errors.Is(err, redis.Nil) {
			slog.Warn("Redis lookup failed; falling back to PostgreSQL",
				"error", err,
			)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	existingPaymentID, err := s.idempotencyRepo.GetPaymentID(
		ctx, tx, idempotencyKey,
	)
	if err != nil {
		return nil, err
	}

	if existingPaymentID != "" {
		payment, err := s.paymentRepo.GetByIDTx(ctx, tx, existingPaymentID)
		if err != nil {
			return nil, err
		}

		s.cachePaymentID(ctx, idempotencyKey, payment.ID)
		return payment, nil
	}

	payment := &model.Payment{
		ID:       uuid.NewString(),
		Amount:   amount,
		Currency: currency,
		Status:   "PENDING",
	}

	if err := s.paymentRepo.CreateTx(ctx, tx, payment); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(payment)
	if err != nil {
		return nil, err
	}

	event := &model.OutboxEvent{
		ID:          uuid.NewString(),
		EventType:   "PaymentCreated",
		AggregateID: payment.ID,
		Payload:     payload,
	}

	if err := s.outboxRepo.CreateTx(ctx, tx, event); err != nil {
		return nil, err
	}

	if err := s.idempotencyRepo.Save(
		ctx, tx, idempotencyKey, payment.ID,
	); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Another request inserted this key first.
			_ = tx.Rollback()

			tx2, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			defer tx2.Rollback()

			paymentID, err := s.idempotencyRepo.GetPaymentID(
				ctx, tx2, idempotencyKey,
			)
			if err != nil {
				return nil, err
			}
			if paymentID == "" {
				return nil, errors.New(
					"idempotency key conflict but payment ID was not found",
				)
			}

			existingPayment, err := s.paymentRepo.GetByIDTx(
				ctx, tx2, paymentID,
			)
			if err != nil {
				return nil, err
			}

			s.cachePaymentID(ctx, idempotencyKey, existingPayment.ID)
			return existingPayment, nil
		}

		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	metrics.PaymentsCreated.Inc()
	s.cachePaymentID(ctx, idempotencyKey, payment.ID)

	return payment, nil
}
