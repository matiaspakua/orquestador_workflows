-- Idempotency tracking (AUDIT finding: no duplicate detection in consumer)
-- Prevents the same Kafka message from being processed more than once.

CREATE TABLE IF NOT EXISTS processed_events (
    event_id   VARCHAR(100) PRIMARY KEY,
    consumer_id VARCHAR(50)  NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events(processed_at);

-- Dead-letter log — records messages that were routed to orchestration-dlq
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id           SERIAL PRIMARY KEY,
    event_id     VARCHAR(100),
    worker       VARCHAR(50),
    step_name    VARCHAR(100),
    error        TEXT,
    payload      JSONB,
    received_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
