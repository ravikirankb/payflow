package service

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/ravikirankb/payflow/internal/model"
	"github.com/ravikirankb/payflow/internal/repository"
)

type PaymentService struct {
	db              *sql.DB
	paymentRepo     *repository.PaymentRepository
	idempotencyRepo *repository.IdempotencyRepository
}

func NewPaymentService(
	db *sql.DB,
	paymentRepo *repository.PaymentRepository,
	idempotencyRepo *repository.IdempotencyRepository,
) *PaymentService {
	return &PaymentService{
		db:              db,
		paymentRepo:     paymentRepo,
		idempotencyRepo: idempotencyRepo,
	}
}

func (s *PaymentService) CreatePayment(
	ctx context.Context,
	amount int64,
	currency string,
	idempotencyKey string,
) (*model.Payment, error) {

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	existingPaymentID, err := s.idempotencyRepo.GetPaymentID(ctx, tx, idempotencyKey)
	if err != nil {
		return nil, err
	}

	if existingPaymentID != "" {
		return s.paymentRepo.GetByIDTx(ctx, tx, existingPaymentID)
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

	if err := s.idempotencyRepo.Save(ctx, tx, idempotencyKey, payment.ID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return payment, nil
}
