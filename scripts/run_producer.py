#!/usr/bin/env python
"""
Entrypoint: continuously generate synthetic heart-rate events and publish
them to Kafka, one round-robin pass across all configured customers per
GENERATION_INTERVAL_SECONDS.

Run with:
    python scripts/run_producer.py
    make run-producer

Stop with Ctrl+C -- SIGINT/SIGTERM trigger a graceful shutdown that flushes
any in-flight messages before exiting.
"""

from __future__ import annotations

import logging
import random
import signal
import sys
import time
from types import FrameType

from heartbeat_monitoring.config import get_settings
from heartbeat_monitoring.generator import generate_customer_ids, generate_event
from heartbeat_monitoring.producer import HeartRateProducer
from heartbeat_monitoring.utils import configure_logging

logger = logging.getLogger("heartbeat_monitoring.run_producer")

_shutdown_requested = False


def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    global _shutdown_requested
    logger.info("Received shutdown signal (%s); finishing current cycle...", signum)
    _shutdown_requested = True


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)

    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)

    customer_ids = generate_customer_ids(settings.number_of_customers)
    rng = random.Random()

    producer = HeartRateProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        client_id=settings.kafka_client_id,
    )

    logger.info(
        "Producer started (customers=%d, interval=%ss, topic=%s)",
        len(customer_ids),
        settings.generation_interval_seconds,
        settings.kafka_topic,
    )

    try:
        while not _shutdown_requested:
            for customer_id in customer_ids:
                event = generate_event(
                    customer_id=customer_id,
                    min_heart_rate=settings.min_heart_rate,
                    max_heart_rate=settings.max_heart_rate,
                    anomaly_probability=settings.anomaly_probability,
                    rng=rng,
                )
                producer.publish(event)
            time.sleep(settings.generation_interval_seconds)
    except Exception:
        logger.exception("Unexpected error in producer loop")
        return 1
    finally:
        producer.close()
        logger.info("Graceful shutdown complete.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
