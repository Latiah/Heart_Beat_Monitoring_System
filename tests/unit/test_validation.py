"""
Unit tests for structural validation.

The behaviour that matters here is the invalid vs. valid-but-abnormal split:
a malformed message is dropped, but an alarming-yet-well-formed reading must
survive validation so the monitoring system can actually report it.
"""

import json
from datetime import UTC, datetime

import pytest

from heartbeat_monitoring.validation import validate_message


def raw_message(**overrides) -> str:
    payload = {
        "event_id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
        "customer_id": "CUST_001",
        "timestamp": datetime.now(UTC).isoformat(),
        "heart_rate": 72,
        "source": "synthetic-generator",
    }
    payload.update(overrides)
    return json.dumps(payload)


def test_well_formed_message_is_valid():
    result = validate_message(raw_message())
    assert result.is_valid
    assert result.event.heart_rate == 72


@pytest.mark.parametrize("heart_rate", [30, 190])
def test_abnormal_but_plausible_readings_stay_valid(heart_rate):
    """
    The pipeline's most important non-obvious rule: an alarming reading is
    valid data to store and flag, not bad data to discard. Dropping these
    would throw away exactly the events the system exists to catch.
    """
    assert validate_message(raw_message(heart_rate=heart_rate)).is_valid


@pytest.mark.parametrize("heart_rate", [0, 400])
def test_implausible_heart_rate_is_rejected_as_sensor_corruption(heart_rate):
    """No living person registers these; treat as a broken sensor, not an alert."""
    result = validate_message(raw_message(heart_rate=heart_rate))
    assert not result.is_valid
    assert "plausible" in result.error


def test_missing_required_field_is_invalid():
    payload = json.loads(raw_message())
    del payload["heart_rate"]
    assert not validate_message(json.dumps(payload)).is_valid


def test_naive_timestamp_is_invalid():
    """An ambiguous timestamp must be rejected, not silently assumed to be UTC."""
    assert not validate_message(raw_message(timestamp="2026-01-01T12:00:00")).is_valid


@pytest.mark.parametrize(
    "payload",
    ["", "{ not json", "{}", b"\xff\xfe binary garbage"],
)
def test_hostile_payloads_return_a_result_instead_of_raising(payload):
    """
    A single bad message must not be able to kill the consumer loop, so
    validate_message reports failures rather than raising them.
    """
    result = validate_message(payload)
    assert result.is_valid is False
    assert result.error is not None
