-- Remove the consumer idempotency table during rollback.
DROP TABLE IF EXISTS processed_events;