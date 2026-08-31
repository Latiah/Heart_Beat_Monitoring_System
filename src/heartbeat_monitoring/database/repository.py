"""
Data access for heart_rate_readings.

Every SQL statement in the ingestion path lives in this file. Nothing else in
the codebase writes SQL, so the schema can change here without rippling
through the consumer, and the queries are all readable in one place.
"""

from __future__ import annotations

import logging

from psycopg.rows import dict_row

from heartbeat_monitoring.database.connection import Database
from heartbeat_monitoring.models import HeartRateReading

logger = logging.getLogger(__name__)

# ON CONFLICT DO NOTHING is what makes the consumer safe to restart.
#
# Kafka gives at-least-once delivery: after a crash, offsets already
# processed but not yet committed get redelivered. Without this clause every
# restart would duplicate rows. With it, the event_id UNIQUE constraint makes
# the insert idempotent -- replaying a message is a no-op rather than
# corruption.
INSERT_READING_SQL = """
    INSERT INTO heart_rate_readings
        (event_id, customer_id, reading_time, heart_rate, status, is_anomaly)
    VALUES
        (%(event_id)s, %(customer_id)s, %(reading_time)s,
         %(heart_rate)s, %(status)s, %(is_anomaly)s)
    ON CONFLICT (event_id) DO NOTHING
"""

RECENT_READINGS_SQL = """
    SELECT customer_id, reading_time, heart_rate, status, is_anomaly
    FROM heart_rate_readings
    ORDER BY reading_time DESC
    LIMIT %(limit)s
"""

COUNT_READINGS_SQL = "SELECT COUNT(*) AS total FROM heart_rate_readings"


class HeartRateRepository:
    """Persists and reads back heart-rate readings."""

    def __init__(self, database: Database) -> None:
        self._db = database

    def insert_reading(self, reading: HeartRateReading) -> bool:
        """
        Insert one reading. Returns True if a row was written, False if the
        event_id was already present (a duplicate delivery).
        """
        with self._db.connection() as conn, conn.cursor() as cur:
            cur.execute(INSERT_READING_SQL, self._as_params(reading))
            return cur.rowcount == 1

    def insert_readings(self, readings: list[HeartRateReading]) -> int:
        """
        Insert a batch in a single round trip, returning rows actually written.

        Batching matters at stream volumes: one INSERT per message spends most
        of its time in network round trips rather than in the database.
        """
        if not readings:
            return 0

        params = [self._as_params(r) for r in readings]
        with self._db.connection() as conn, conn.cursor() as cur:
            cur.executemany(INSERT_READING_SQL, params)
            # executemany reports the total across all statements; duplicates
            # contribute 0, so this is the count of genuinely new rows.
            rowcount = cur.rowcount
            written = rowcount if rowcount is not None and rowcount >= 0 else len(params)

        skipped = len(params) - written
        if skipped > 0:
            logger.debug("Skipped %d duplicate reading(s) on insert", skipped)
        return written

    def count_readings(self) -> int:
        """Total rows stored. Used by tests and the dashboard summary."""
        with self._db.connection() as conn, conn.cursor() as cur:
            cur.execute(COUNT_READINGS_SQL)
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def recent_readings(self, limit: int = 20) -> list[dict]:
        """Most recent readings across all customers, newest first."""
        with self._db.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(RECENT_READINGS_SQL, {"limit": limit})
            return cur.fetchall()

    @staticmethod
    def _as_params(reading: HeartRateReading) -> dict:
        """Map the model onto the INSERT's named placeholders."""
        return {
            "event_id": reading.event_id,
            "customer_id": reading.customer_id,
            "reading_time": reading.reading_time,
            "heart_rate": reading.heart_rate,
            "status": reading.status.value,
            "is_anomaly": reading.is_anomaly,
        }
