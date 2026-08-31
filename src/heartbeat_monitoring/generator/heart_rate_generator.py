"""
Synthetic heart-rate data generation.

Deliberately written as pure functions that take an injected `random.Random`
rather than using the module-level `random` singleton. That makes every
function here deterministic under a seeded RNG, so the generator's behaviour
is unit-testable without patching globals.

A share of readings (`anomaly_probability`) is generated deliberately outside
the plausible range. Without them the downstream anomaly-detection path would
never be exercised, and the interesting half of the pipeline would be dead
code in every demo run.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime

from heartbeat_monitoring.models import HeartRateEvent

# Bounds for deliberately-anomalous readings. Kept just outside any plausible
# normal range but still inside the DB CHECK (0..300), because an anomaly is a
# real reading worth storing and alerting on -- not corrupt data to reject.
ANOMALY_LOW_RANGE = (25, 45)
ANOMALY_HIGH_RANGE = (130, 200)


def generate_customer_ids(count: int) -> list[str]:
    """
    Build a stable list of synthetic customer identifiers: CUST_001, CUST_002...

    Stable (not random) IDs matter: restarting the producer must keep writing
    to the same customers, otherwise per-customer history in the database
    fragments across every restart.
    """
    if count < 1:
        raise ValueError(f"count must be at least 1, got {count}")
    return [f"CUST_{i:03d}" for i in range(1, count + 1)]


def generate_heart_rate(
    min_heart_rate: int,
    max_heart_rate: int,
    anomaly_probability: float,
    rng: random.Random | None = None,
) -> int:
    """
    Return a heart rate: usually within [min, max], occasionally an outlier.

    The anomaly branch picks low or high with equal probability so both
    classification paths get exercised over a long enough run.
    """
    rng = rng or random.Random()

    if rng.random() < anomaly_probability:
        low, high = rng.choice([ANOMALY_LOW_RANGE, ANOMALY_HIGH_RANGE])
        return rng.randint(low, high)

    return rng.randint(min_heart_rate, max_heart_rate)


def generate_event(
    customer_id: str,
    min_heart_rate: int,
    max_heart_rate: int,
    anomaly_probability: float,
    rng: random.Random | None = None,
) -> HeartRateEvent:
    """Generate one complete, valid heart-rate event for a customer."""
    return HeartRateEvent(
        customer_id=customer_id,
        timestamp=datetime.now(UTC),
        heart_rate=generate_heart_rate(
            min_heart_rate=min_heart_rate,
            max_heart_rate=max_heart_rate,
            anomaly_probability=anomaly_probability,
            rng=rng,
        ),
    )
