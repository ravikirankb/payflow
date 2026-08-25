-- Stores the financial ledger entry created from a PaymentCreated event.
--
-- event_id is unique so the same Kafka event can never create
-- multiple ledger entries.
CREATE TABLE payment_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    event_id UUID NOT NULL UNIQUE,

    payment_id UUID NOT NULL,

    amount BIGINT NOT NULL,

    currency VARCHAR(3) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Payment ID will be queried frequently when inspecting a payment's ledger.
CREATE INDEX idx_payment_ledger_payment_id
    ON payment_ledger(payment_id);