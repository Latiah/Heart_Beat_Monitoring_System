-- Most recent readings across all customers. Good smoke test that the
-- pipeline is actually writing data.
SELECT
    customer_id,
    reading_time,
    heart_rate,
    status,
    is_anomaly
FROM heart_rate_readings
ORDER BY reading_time DESC
LIMIT 20;
