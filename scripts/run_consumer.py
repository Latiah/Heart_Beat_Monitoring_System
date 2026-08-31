#!/usr/bin/env python
"""
Entrypoint: consume heart-rate events from Kafka, validate and classify them,
and persist them to PostgreSQL.

Run with:
    python scripts/run_consumer.py
    make run-consumer

Stop with Ctrl+C -- SIGINT/SIGTERM trigger a graceful shutdown that flushes
the buffered batch and commits offsets before exiting.

Requires the schema to exist: run `make db-init` first.
"""

from __future__ import annotations

import logging
import signal
import sys
from types import FrameType

from heartbeat_monitoring.config import get_settings
from heartbeat_monitoring.consumer import HeartRateConsumer
from heartbeat_monitoring.database import Database, HeartRateRepository
from heartbeat_monitoring.utils import configure_logging

logger = logging.getLogger("heartbeat_monitoring.run_consumer")


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)

    database = Database(dsn=settings.postgres_dsn)

    try:
        database.open()
    except Exception:
        logger.exception(
            "Could not connect to PostgreSQL at %s:%s -- is `make up` running?",
            settings.postgres_host,
            settings.postgres_port,
        )
        return 1

    consumer = HeartRateConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        consumer_group=settings.kafka_consumer_group,
        repository=HeartRateRepository(database),
        anomaly_low_threshold=settings.anomaly_low_threshold,
        anomaly_high_threshold=settings.anomaly_high_threshold,
    )

    def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
        # Only ask the loop to stop; the flush-and-commit happens on the main
        # thread inside run()'s finally block, where it is safe to do I/O.
        logger.info("Received shutdown signal (%s); stopping consumer...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)

    try:
        consumer.run()
    except Exception:
        logger.exception("Unexpected error in consumer loop")
        return 1
    finally:
        database.close()
        logger.info("Graceful shutdown complete.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
