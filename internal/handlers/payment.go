package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ravikirankb/payflow/internal/service"
)

type CreatePaymentRequest struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

func validateCreatePaymentRequest(req CreatePaymentRequest) string {
	if req.Amount <= 0 {
		return "amount must be greater than zero"
	}

	if len(req.Currency) != 3 {
		return "currency must be a 3-letter ISO code"
	}

	if strings.ToUpper(req.Currency) != req.Currency {
		return "currency must be uppercase"
	}

	return ""
}

func CreatePayment(svc *service.PaymentService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "missing Idempotency-Key header")
			return
		}

		var req CreatePaymentRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if msg := validateCreatePaymentRequest(req); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}

		payment, err := svc.CreatePayment(
			r.Context(),
			req.Amount,
			req.Currency,
			key,
		)

		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create payment")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(payment)
	}
}
