-- Heartbeat monitoring schema
-- Runs automatically on first Postgres container startup (see docker-compose.yml)

CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(20) PRIMARY KEY,
    customer_name    VARCHAR(100),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS heartbeats (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,
    timestamp             TIMESTAMPTZ NOT NULL,
    heart_rate      SMALLINT NOT NULL CHECK (heart_rate > 0 AND heart_rate < 300),
    is_anomaly      BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Time-series queries are almost always "latest N for a customer" or
-- "everything in a time window" -- index both patterns.
CREATE INDEX IF NOT EXISTS idx_heartbeats_timestamp ON heartbeats (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_heartbeats_customer_timestamp ON heartbeats (customer_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_heartbeats_anomaly ON heartbeats (is_anomaly) WHERE is_anomaly = TRUE;

INSERT INTO customers (customer_id, customer_name) VALUES
    ('cust_001', 'Mugabo Johnson'),
    ('cust_002', 'Joseph Forson'),
    ('cust_003', 'Latifah AKIMANA'),
    ('cust_004', 'Vicent Logan'),
    ('cust_005', 'Megan Winny')
ON CONFLICT (customer_id) DO NOTHING;

