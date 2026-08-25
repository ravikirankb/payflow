-- Records Kafka events that have been successfully processed.
-- The primary key prevents the same event from being processed twice.
CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);