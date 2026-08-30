-- Overall breakdown of readings by status.
SELECT
    status,
    COUNT(*) AS reading_count
FROM heart_rate_readings
GROUP BY status
ORDER BY reading_count DESC;

-- Per-customer anomaly rate -- useful for spotting a customer whose feed
-- looks unusually unstable.
SELECT
    customer_id,
    COUNT(*) FILTER (WHERE is_anomaly) AS anomaly_count,
    COUNT(*) AS total_readings,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_anomaly) / NULLIF(COUNT(*), 0),
        2
    ) AS anomaly_rate_percent
FROM heart_rate_readings
GROUP BY customer_id
ORDER BY anomaly_rate_percent DESC NULLS LAST;
