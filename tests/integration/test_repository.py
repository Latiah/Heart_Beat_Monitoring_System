"""
Integration tests for the repository against real PostgreSQL.

Focus: that the SQL matches the schema, and that the idempotency guarantee
the consumer relies on genuinely holds in the database rather than only in
our reasoning about it.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from heartbeat_monitoring.models import HeartRateReading, ReadingStatus

pytestmark = pytest.mark.integration


def make_reading(
    customer_id: str,
    heart_rate: int = 72,
    status: ReadingStatus = ReadingStatus.NORMAL,
) -> HeartRateReading:
    return HeartRateReading(
        event_id=str(uuid.uuid4()),
        customer_id=customer_id,
        reading_time=datetime.now(UTC),
        heart_rate=heart_rate,
        status=status,
    )


def fetch_rows(database, customer_id: str) -> list[tuple]:
    with database.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT heart_rate, status, is_anomaly FROM heart_rate_readings "
            "WHERE customer_id = %s ORDER BY reading_time",
            (customer_id,),
        )
        return cur.fetchall()


@pytest.mark.parametrize(
    ("heart_rate", "status"),
    [(72, ReadingStatus.NORMAL), (35, ReadingStatus.LOW), (185, ReadingStatus.HIGH)],
)
def test_every_status_round_trips_through_the_check_constraint(
    repository, database, test_customer_id, heart_rate, status
):
    assert repository.insert_reading(make_reading(test_customer_id, heart_rate, status))

    rows = fetch_rows(database, test_customer_id)
    assert rows == [(heart_rate, status.value, status is not ReadingStatus.NORMAL)]


def test_reinserting_the_same_event_id_writes_no_second_row(repository, database, test_customer_id):
    """
    This is the property that makes the consumer safe to restart. Kafka
    redelivers uncommitted messages after a crash; without it, every restart
    would duplicate rows.
    """
    reading = make_reading(test_customer_id)

    assert repository.insert_reading(reading) is True
    assert repository.insert_reading(reading) is False
    assert len(fetch_rows(database, test_customer_id)) == 1


def test_replaying_an_entire_batch_is_a_no_op(repository, database, test_customer_id):
    batch = [make_reading(test_customer_id, heart_rate=60 + i) for i in range(5)]

    assert repository.insert_readings(batch) == 5
    assert repository.insert_readings(batch) == 0
    assert len(fetch_rows(database, test_customer_id)) == 5


def test_empty_batch_is_a_no_op(repository, test_customer_id):
    assert repository.insert_readings([]) == 0
