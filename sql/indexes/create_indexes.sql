-- Indexes for heart_rate_readings.
--
-- The composite index (customer_id, reading_time DESC) is the important
-- one: it directly serves the dominant query pattern in a per-customer
-- monitoring system -- "recent history for customer X". The single-column
-- indexes remain for queries that filter on only one dimension (e.g. all
-- readings across all customers ordered by time, or a customer's full
-- history regardless of order).

CREATE INDEX IF NOT EXISTS idx_heart_rate_customer_id
    ON heart_rate_readings (customer_id);

CREATE INDEX IF NOT EXISTS idx_heart_rate_reading_time
    ON heart_rate_readings (reading_time DESC);

CREATE INDEX IF NOT EXISTS idx_heart_rate_customer_time
    ON heart_rate_readings (customer_id, reading_time DESC);

-- Speeds up anomaly-focused dashboard queries (WHERE is_anomaly = true).
CREATE INDEX IF NOT EXISTS idx_heart_rate_is_anomaly
    ON heart_rate_readings (is_anomaly)
    WHERE is_anomaly = true;
