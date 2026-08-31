-- Schema for the Real-Time Customer Heart Beat Monitoring System.
--
-- Design notes:
--   * event_id is UNIQUE, which is what makes the consumer's INSERT ... ON
--     CONFLICT DO NOTHING idempotent under Kafka's at-least-once delivery.
--     Without it, every consumer restart would duplicate rows.
--   * reading_time (event time) and created_at (processing time) are kept
--     separate. Collapsing them would make it impossible to distinguish a
--     genuinely old reading from one that arrived late.
--   * status is constrained rather than free text so a typo in application
--     code cannot quietly pollute the dimension every dashboard groups by.

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
COMMENT ON COLUMN heart_rate_readings.status IS
    'Classification of heart_rate against the configured thresholds: NORMAL, LOW, or HIGH.';
COMMENT ON COLUMN heart_rate_readings.is_anomaly IS
    'Convenience flag, true whenever status is not NORMAL. Denormalised so anomaly dashboards can use a small partial index.';
