-- Hourly aggregate statistics across all customers. date_trunc('hour', ...)
-- buckets reading_time into hour-wide windows -- simple and index-friendly
-- for a learning-project scale of data (no need for TimescaleDB-style
-- continuous aggregates here).
SELECT
    date_trunc('hour', reading_time) AS hour_bucket,
    COUNT(*) AS reading_count,
    ROUND(AVG(heart_rate)::numeric, 1) AS avg_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    MAX(heart_rate) AS max_heart_rate,
    COUNT(*) FILTER (WHERE is_anomaly) AS anomaly_count
FROM heart_rate_readings
GROUP BY hour_bucket
ORDER BY hour_bucket DESC;
