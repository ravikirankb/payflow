CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payments(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);