"""
Kafka producer wrapper.

Thin layer over confluent_kafka.Producer that owns three decisions worth
stating explicitly: the partition key, the durability settings, and how
delivery failures are surfaced.
"""

from __future__ import annotations

import logging
from types import TracebackType

from confluent_kafka import KafkaException, Producer

from heartbeat_monitoring.models import HeartRateEvent

logger = logging.getLogger(__name__)


class HeartRateProducer:
    """Publishes HeartRateEvents to a Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "heartbeat-producer",
        flush_timeout_seconds: float = 10.0,
    ) -> None:
        self._topic = topic
        self._flush_timeout_seconds = flush_timeout_seconds
        self._delivery_failures = 0
        self._delivered = 0

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                # acks=all: the leader waits for all in-sync replicas before
                # acknowledging. Slower, but a broker failure cannot silently
                # lose readings that the producer already considers sent.
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "linger.ms": 10,
                "compression.type": "snappy",
            }
        )
        logger.info("Producer configured (servers=%s, topic=%s)", bootstrap_servers, topic)

    def _on_delivery(self, error: object | None, message: object) -> None:
        """
        Delivery callback.

        confluent-kafka's produce() is asynchronous, so a broker-side failure
        arrives here rather than as an exception at the call site. Without this
        callback a run could report thousands of messages "sent" while the
        broker rejected every one.
        """
        if error is not None:
            self._delivery_failures += 1
            logger.error("Message delivery failed: %s", error)
        else:
            self._delivered += 1
            logger.debug(
                "Delivered to %s [partition %s] @ offset %s",
                message.topic(),
                message.partition(),
                message.offset(),
            )

    def publish(self, event: HeartRateEvent) -> None:
        """
        Publish one event.

        Keyed by customer_id so all readings for a customer land on the same
        partition, which preserves per-customer chronological ordering. Keying
        by event_id instead would scatter a customer's history across
        partitions and lose that guarantee.
        """
        try:
            self._producer.produce(
                topic=self._topic,
                key=event.customer_id.encode("utf-8"),
                value=event.to_json().encode("utf-8"),
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # Local queue is full: the broker isn't keeping up. Block for
            # outstanding deliveries, then retry once rather than dropping.
            logger.warning("Producer queue full; flushing before retry")
            self._producer.flush(self._flush_timeout_seconds)
            self._producer.produce(
                topic=self._topic,
                key=event.customer_id.encode("utf-8"),
                value=event.to_json().encode("utf-8"),
                on_delivery=self._on_delivery,
            )
        except KafkaException:
            logger.exception("Failed to enqueue event %s", event.event_id)
            raise

        # Serve delivery callbacks for already-completed sends without blocking.
        self._producer.poll(0)

    def flush(self) -> int:
        """Block until queued messages are delivered. Returns messages still pending."""
        return self._producer.flush(self._flush_timeout_seconds)

    def close(self) -> None:
        """Flush outstanding messages and report the delivery tally."""
        pending = self.flush()
        if pending > 0:
            logger.warning("Producer closed with %d message(s) undelivered", pending)
        logger.info(
            "Producer closed (delivered=%d, failed=%d)",
            self._delivered,
            self._delivery_failures,
        )

    def __enter__(self) -> HeartRateProducer:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
