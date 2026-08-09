package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/ravikirankb/payflow/internal/model"
	"github.com/ravikirankb/payflow/internal/repository"
)

type PaymentService struct {
	repo *repository.PaymentRepository
}

func NewPaymentService(repo *repository.PaymentRepository) *PaymentService {
	return &PaymentService{repo: repo}
}

func (s *PaymentService) CreatePayment(ctx context.Context, amount int64, currency string) (*model.Payment, error) {
	payment := &model.Payment{
		ID:       uuid.NewString(),
		Amount:   amount,
		Currency: currency,
		Status:   "PENDING",
	}

	if err := s.repo.Create(ctx, payment); err != nil {
		return nil, err
	}

	return payment, nil
}
