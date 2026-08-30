-- Schema for the Real-Time Customer Heart Beat Monitoring System.

CREATE TABLE IF NOT EXISTS heart_rate_readings (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID NOT NULL UNIQUE,
    customer_id  TEXT NOT NULL,
    reading_time TIMESTAMPTZ NOT NULL,
    heart_rate   SMALLINT NOT NULL CHECK (heart_rate BETWEEN 0 AND 300),
    status       TEXT NOT NULL CHECK (status IN ('NORMAL', 'LOW', 'HIGH')),
    is_anomaly   BOOLEAN NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE heart_rate_readings IS
    'One row per validated heart-rate event. Anomaly thresholds are simulation/business rules, not medical diagnoses.';
COMMENT ON COLUMN heart_rate_readings.event_id IS
    'UUID assigned at event-creation time; used to de-duplicate at-least-once Kafka delivery.';
COMMENT ON COLUMN heart_rate_readings.reading_time IS
    'When the reading occurred (as generated), not when it was processed. See created_at for processing time.';
