-- Roll back to UUID if required.
ALTER TABLE processed_events
    ALTER COLUMN event_id TYPE UUID
    USING event_id::uuid;