"""
Unit tests for the event and reading models.

Covers only the model behaviour the pipeline actually depends on: the
timezone contract, the JSON round trip across the Kafka boundary, and the
derivation of is_anomaly from status.
"""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from heartbeat_monitoring.models import HeartRateEvent, HeartRateReading, ReadingStatus


def make_event(**overrides) -> HeartRateEvent:
    defaults = {
        "customer_id": "CUST_001",
        "timestamp": datetime.now(UTC),
        "heart_rate": 72,
    }
    return HeartRateEvent(**{**defaults, **overrides})


def test_naive_timestamp_is_rejected():
    """An ambiguous timestamp must fail loudly rather than be guessed at."""
    with pytest.raises(ValidationError, match="timezone-aware"):
        make_event(timestamp=datetime(2026, 1, 1, 12, 0, 0))


def test_non_utc_timestamp_is_normalised_to_utc():
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=3)))
    event = make_event(timestamp=ts)

    assert event.timestamp.utcoffset() == timedelta(0)
    assert event.timestamp.hour == 9


def test_json_round_trip_preserves_every_field():
    """Kafka hands the consumer bytes, so the round trip must survive both forms."""
    original = make_event()
    assert HeartRateEvent.from_json(original.to_json()) == original
    assert HeartRateEvent.from_json(original.to_json().encode("utf-8")) == original


def test_unknown_field_is_rejected():
    """extra='forbid' catches producer/consumer schema drift early."""
    with pytest.raises(ValidationError):
        make_event(unexpected_field="surprise")


@pytest.mark.parametrize(
    ("status", "expected_anomaly"),
    [
        (ReadingStatus.NORMAL, False),
        (ReadingStatus.LOW, True),
        (ReadingStatus.HIGH, True),
    ],
)
def test_is_anomaly_is_derived_from_status(status, expected_anomaly):
    reading = HeartRateReading.from_event(make_event(), status)
    assert reading.is_anomaly is expected_anomaly


def test_status_values_match_the_database_check_constraint():
    """These strings are duplicated in sql/schema/create_tables.sql."""
    assert {s.value for s in ReadingStatus} == {"NORMAL", "LOW", "HIGH"}
