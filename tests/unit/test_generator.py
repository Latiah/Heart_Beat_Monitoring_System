"""
Unit tests for synthetic data generation.

Randomness is driven through an injected, seeded random.Random, so these
assertions are deterministic rather than flaky.
"""

import random

from heartbeat_monitoring.generator import (
    generate_customer_ids,
    generate_event,
    generate_heart_rate,
)
from heartbeat_monitoring.validation import validate_message

MIN_HR = 60
MAX_HR = 100


def test_customer_ids_are_stable_and_zero_padded():
    """Restarting the producer must not fragment per-customer history."""
    assert generate_customer_ids(3) == ["CUST_001", "CUST_002", "CUST_003"]


def test_no_anomalies_means_every_reading_is_in_range():
    rng = random.Random(42)
    for _ in range(200):
        assert MIN_HR <= generate_heart_rate(MIN_HR, MAX_HR, 0.0, rng) <= MAX_HR


def test_guaranteed_anomalies_produce_both_low_and_high_outliers():
    """A one-sided anomaly generator would leave the LOW path untested."""
    rng = random.Random(11)
    rates = [generate_heart_rate(MIN_HR, MAX_HR, 1.0, rng) for _ in range(200)]

    assert all(not (MIN_HR <= hr <= MAX_HR) for hr in rates)
    assert any(hr < MIN_HR for hr in rates)
    assert any(hr > MAX_HR for hr in rates)


def test_generated_events_are_always_structurally_valid():
    """
    The generator must never emit something its own validator rejects --
    including its deliberate anomalies, which are valid readings to store.
    """
    rng = random.Random(99)
    for _ in range(200):
        event = generate_event("CUST_001", MIN_HR, MAX_HR, 0.5, rng)
        assert validate_message(event.to_json()).is_valid


def test_events_carry_the_customer_id_and_a_utc_timestamp():
    event = generate_event("CUST_042", MIN_HR, MAX_HR, 0.0, random.Random(1))
    assert event.customer_id == "CUST_042"
    assert event.timestamp.tzinfo is not None
