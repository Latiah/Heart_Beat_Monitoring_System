#!/usr/bin/env python
"""
Prints a stream of synthetic heart-rate events to stdout, without touching
Kafka. Useful for sanity-checking generator behaviour and configuration
before standing up any infrastructure.

Run with:
    python scripts/run_generator.py
    make run-generator
"""

from __future__ import annotations

import random
import sys
import time

from heartbeat_monitoring.config import get_settings
from heartbeat_monitoring.generator import generate_customer_ids, generate_event


def main() -> int:
    settings = get_settings()
    customer_ids = generate_customer_ids(settings.number_of_customers)
    rng = random.Random()

    print(f"Generating events for {len(customer_ids)} customers. Ctrl+C to stop.\n")

    try:
        while True:
            for customer_id in customer_ids:
                event = generate_event(
                    customer_id=customer_id,
                    min_heart_rate=settings.min_heart_rate,
                    max_heart_rate=settings.max_heart_rate,
                    anomaly_probability=settings.anomaly_probability,
                    rng=rng,
                )
                print(event.to_json())
            time.sleep(settings.generation_interval_seconds)
    except KeyboardInterrupt:
        print("\nStopped.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
