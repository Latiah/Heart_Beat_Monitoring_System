"""
Synthetic heart-rate data generator.

Produces one reading at a time as a plain dict:
    {"customer_id": "cust_001", "timestamp": "2026-08-28T12:00:00.123456+00:00", "heart_rate": 72}

Each customer gets its own slowly-drifting "baseline" heart rate so the
stream looks like real people (not pure noise) -- and occasionally emits a
deliberately out-of-range reading so the anomaly-detection logic downstream
has something to catch. Can be run standalone to preview output.
"""
import random
from datetime import datetime, timezone

import config

# Per-customer running baseline, so consecutive readings drift smoothly
# instead of jumping around randomly every tick.
_baselines = {
    cid: random.randint(config.NORMAL_HR_MIN, config.NORMAL_HR_MAX)
    for cid in config.CUSTOMER_IDS
}


def _next_baseline(customer_id: str) -> int:
    """Random walk the customer's baseline heart rate within normal bounds."""
    current = _baselines[customer_id]
    step = random.randint(-2, 2)
    new_value = max(config.NORMAL_HR_MIN, min(config.NORMAL_HR_MAX, current + step))
    _baselines[customer_id] = new_value
    return new_value


def generate_reading(customer_id: str | None = None) -> dict:
    """Generate a single synthetic heartbeat reading."""
    customer_id = customer_id or random.choice(config.CUSTOMER_IDS)

    if random.random() < config.ANOMALY_PROBABILITY:
        # Deliberately inject a spike or a drop.
        heart_rate = random.choice(
            [
                random.randint(20, config.ANOMALY_HR_LOW),
                random.randint(config.ANOMALY_HR_HIGH, 220),
            ]
        )
    else:
        baseline = _next_baseline(customer_id)
        # Small per-reading jitter on top of the drifting baseline.
        heart_rate = max(30, baseline + random.randint(-3, 3))

    return {
        "customer_id": customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": heart_rate,
    }


def generate_batch(n: int) -> list[dict]:
    """Generate n readings, one per customer round-robin."""
    return [generate_reading(config.CUSTOMER_IDS[i % len(config.CUSTOMER_IDS)]) for i in range(n)]


if __name__ == "__main__":
    # Quick manual preview: `python data_generator.py`
    for reading in generate_batch(10):
        print(reading)
