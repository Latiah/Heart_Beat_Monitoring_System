-- Full reading history for a single customer, most recent first.
-- Replace :customer_id (or the literal below) as needed.
SELECT
    reading_time,
    heart_rate,
    status,
    is_anomaly
FROM heart_rate_readings
WHERE customer_id = 'CUST_001'
ORDER BY reading_time DESC;
