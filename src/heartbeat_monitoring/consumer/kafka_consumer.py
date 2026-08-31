"""
Kafka consumer -- the orchestrator of the processing pipeline.

For each message: validate -> classify -> persist. The consumer itself holds
no validation or classification logic; it wires together the validation,
processing, and database layers and owns the delivery semantics.

Offset handling is the important design decision here. Auto-commit is
disabled and offsets are committed only *after* a batch has been written to
PostgreSQL. Committing before the write would mean a crash between the two
loses those readings permanently. Committing after means a crash instead
*replays* them -- and the repository's ON CONFLICT DO NOTHING makes that
replay harmless. At-least-once delivery plus an idempotent write is
effectively exactly-once storage, without distributed transactions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from types import TracebackType

from confluent_kafka import Consumer, KafkaError, KafkaException

from heartbeat_monitoring.database import HeartRateRepository
from heartbeat_monitoring.models import HeartRateReading
from heartbeat_monitoring.processing import classify_heart_rate
from heartbeat_monitoring.validation import validate_message

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """Running counters, logged periodically so a long run is observable."""

    consumed: int = 0
    stored: int = 0
    duplicates: int = 0
    invalid: int = 0
    anomalies: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)

    def record_status(self, status: str) -> None:
        self.status_counts[status] = self.status_counts.get(status, 0) + 1

    def summary(self) -> str:
        return (
            f"consumed={self.consumed} stored={self.stored} "
            f"duplicates={self.duplicates} invalid={self.invalid} "
            f"anomalies={self.anomalies} by_status={self.status_counts}"
        )


class HeartRateConsumer:
    """Consumes, validates, classifies, and persists heart-rate events."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        repository: HeartRateRepository,
        anomaly_low_threshold: int,
        anomaly_high_threshold: int,
        batch_size: int = 50,
        poll_timeout_seconds: float = 1.0,
    ) -> None:
        self._topic = topic
        self._repository = repository
        self._low = anomaly_low_threshold
        self._high = anomaly_high_threshold
        self._batch_size = batch_size
        self._poll_timeout_seconds = poll_timeout_seconds

        self._batch: list[HeartRateReading] = []
        self.stats = PipelineStats()
        self._running = False

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": consumer_group,
                # earliest: a fresh consumer group reads the backlog rather
                # than silently skipping everything produced before it started.
                "auto.offset.reset": "earliest",
                # See the module docstring -- offsets are committed by hand
                # after a successful database write.
                "enable.auto.commit": False,
            }
        )
        logger.info(
            "Consumer configured (servers=%s, topic=%s, group=%s)",
            bootstrap_servers,
            topic,
            consumer_group,
        )

    def _process_message(self, raw: bytes) -> None:
        """Validate, classify, and buffer one message."""
        self.stats.consumed += 1

        result = validate_message(raw)
        if not result.is_valid:
            # A malformed message is logged and skipped, never fatal. One bad
            # record must not be able to stall the whole partition.
            self.stats.invalid += 1
            logger.warning("Discarding invalid message: %s", result.error)
            return

        event = result.event
        status = classify_heart_rate(event.heart_rate, self._low, self._high)
        reading = HeartRateReading.from_event(event, status)

        self.stats.record_status(status.value)
        if reading.is_anomaly:
            self.stats.anomalies += 1
            logger.warning(
                "ANOMALY %s: customer=%s heart_rate=%d at %s",
                status.value,
                reading.customer_id,
                reading.heart_rate,
                reading.reading_time.isoformat(),
            )

        self._batch.append(reading)

    def _flush_batch(self) -> None:
        """Write the buffered batch, then commit offsets."""
        if not self._batch:
            return

        size = len(self._batch)
        written = self._repository.insert_readings(self._batch)
        duplicates = size - written

        self.stats.stored += written
        self.stats.duplicates += duplicates

        # Commit only after the write succeeded. If insert_readings raised, we
        # never reach this line, and the messages are redelivered on restart.
        self._consumer.commit(asynchronous=False)

        logger.info(
            "Persisted batch: %d new, %d duplicate(s) | %s",
            written,
            duplicates,
            self.stats.summary(),
        )
        self._batch.clear()

    def run(self) -> PipelineStats:
        """
        Consume until stopped. Returns final statistics.

        KeyboardInterrupt and SIGTERM both land here as a clean exit path: the
        buffered batch is flushed and offsets committed before returning, so
        Ctrl+C never silently discards in-flight readings.
        """
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("Consumer started; waiting for messages on '%s'", self._topic)

        try:
            while self._running:
                message = self._consumer.poll(timeout=self._poll_timeout_seconds)

                if message is None:
                    # Idle tick: flush partial batches so low-throughput data
                    # isn't stuck in the buffer waiting for batch_size.
                    self._flush_batch()
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                self._process_message(message.value())

                if len(self._batch) >= self._batch_size:
                    self._flush_batch()

        except KeyboardInterrupt:
            logger.info("Interrupt received; shutting down consumer")
        finally:
            self._shutdown()

        return self.stats

    def stop(self) -> None:
        """Request a graceful stop from a signal handler or another thread."""
        self._running = False

    def _shutdown(self) -> None:
        """Flush what's buffered, then release the group membership."""
        try:
            self._flush_batch()
        except Exception:
            logger.exception("Failed to flush final batch; those messages will be replayed")
        finally:
            # close() commits nothing but leaves the group cleanly, so Kafka
            # rebalances immediately instead of waiting for a session timeout.
            self._consumer.close()
            logger.info("Consumer closed | final stats: %s", self.stats.summary())

    def __enter__(self) -> HeartRateConsumer:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()
