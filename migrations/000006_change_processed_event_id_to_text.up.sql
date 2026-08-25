-- The application represents event IDs as strings.
-- Keep the database representation aligned with that contract.
ALTER TABLE processed_events
    ALTER COLUMN event_id TYPE TEXT;