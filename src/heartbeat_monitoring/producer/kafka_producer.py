"""
Kafka producer wrapper.

Thin layer over confluent_kafka.Producer that owns four decisions worth
stating explicitly: the partition key, the durability settings, how delivery
failures are surfaced, and how a fatal producer error is recovered from.
"""

from __future__ import annotations

import logging
import time
from types import TracebackType

from confluent_kafka import KafkaException, Producer

from heartbeat_monitoring.models import HeartRateEvent

logger = logging.getLogger(__name__)

# A broker-wide failure fires one delivery callback per in-flight message.
# Logging every one floods the output with identical lines and buries the
# actual cause, so only the first few are logged and the rest are counted.
MAX_DELIVERY_ERRORS_LOGGED = 3


class HeartRateProducer:
    """Publishes HeartRateEvents to a Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "heartbeat-producer",
        flush_timeout_seconds: float = 10.0,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._client_id = client_id
        self._flush_timeout_seconds = flush_timeout_seconds

        self._delivery_failures = 0
        self._delivered = 0
        self._recreations = 0

        self._producer = self._build_producer()
        logger.info("Producer configured (servers=%s, topic=%s)", bootstrap_servers, topic)

    def _build_producer(self) -> Producer:
        return Producer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "client.id": self._client_id,
                # acks=all: the leader waits for all in-sync replicas before
                # acknowledging. Slower, but a broker failure cannot silently
                # lose readings that the producer already considers sent.
                "acks": "all",
                # Idempotence stops a librdkafka-internal retry from writing
                # the same record twice. It pairs with the consumer's
                # ON CONFLICT dedupe: no duplicates on either side of Kafka.
                "enable.idempotence": True,
                "retries": 5,
                "linger.ms": 10,
                "compression.type": "snappy",
            }
        )

    def wait_until_ready(
        self,
        timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        """
        Block until the broker answers a metadata request.

        Called before the first publish for two reasons. It turns "Kafka isn't
        up" into one clear error at startup instead of a confusing failure on
        the first send. More importantly, an idempotent producer that starts
        sending while the broker is still coming up can enqueue records before
        it has acquired a producer id, which desynchronises its sequence
        numbers and kills it with a *fatal* OUT_OF_ORDER_SEQUENCE_NUMBER.
        Fetching metadata first gives the broker time to finish starting.
        """
        deadline = time.monotonic() + timeout_seconds
        attempt = 0

        while True:
            attempt += 1
            try:
                metadata = self._producer.list_topics(timeout=5.0)
                if metadata.brokers:
                    logger.info(
                        "Broker reachable at %s (%d broker(s))",
                        self._bootstrap_servers,
                        len(metadata.brokers),
                    )
                    return
                last_problem = "broker list is empty"
            except KafkaException as exc:
                last_problem = str(exc)

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Kafka at {self._bootstrap_servers} not ready after "
                    f"{timeout_seconds:.0f}s ({last_problem}). Is `make up` running?"
                )

            logger.warning(
                "Kafka not ready yet (attempt %d): %s -- retrying", attempt, last_problem
            )
            time.sleep(poll_interval_seconds)

    def _on_delivery(self, error: object | None, message: object) -> None:
        """
        Delivery callback.

        produce() is asynchronous, so a broker-side rejection arrives here
        rather than as an exception at the call site. Without this callback a
        run could report thousands of messages "sent" that the broker refused.

        This must never raise. librdkafka invokes it from inside flush() and
        poll(), sometimes while a fatal error is already pending on the thread
        -- and any exception escaping here surfaces as an unrelated
        `SystemError: returned a result with an exception set`, which hides the
        real failure completely.
        """
        try:
            if error is not None:
                self._delivery_failures += 1
                if self._delivery_failures <= MAX_DELIVERY_ERRORS_LOGGED:
                    logger.error("Message delivery failed: %s", error)
            else:
                self._delivered += 1
        except Exception:  # noqa: BLE001 - a callback may not propagate anything
            pass

    def _enqueue(self, event: HeartRateEvent) -> None:
        self._producer.produce(
            topic=self._topic,
            key=event.customer_id.encode("utf-8"),
            value=event.to_json().encode("utf-8"),
            on_delivery=self._on_delivery,
        )

    @staticmethod
    def _is_fatal(exc: KafkaException) -> bool:
        """
        Whether a KafkaException left the producer permanently unusable.

        librdkafka marks some errors fatal: the instance can never produce
        again and must be replaced. Retrying on the same object loops forever.
        """
        error = exc.args[0] if exc.args else None
        return bool(error is not None and getattr(error, "fatal", lambda: False)())

    def _recreate_after_fatal(self) -> None:
        """
        Replace a fatally-failed producer with a fresh instance.

        Records still queued on the dead producer are unrecoverable -- it
        cannot flush them either -- so they are reported as lost rather than
        silently dropped.
        """
        self._recreations += 1
        try:
            abandoned = len(self._producer)
        except Exception:  # noqa: BLE001 - a dead producer may not report its queue
            abandoned = -1

        logger.warning(
            "Producer hit a fatal error; recreating (recreation #%d, %s message(s) abandoned)",
            self._recreations,
            abandoned if abandoned >= 0 else "unknown",
        )
        self._producer = self._build_producer()

    def publish(self, event: HeartRateEvent) -> None:
        """
        Publish one event.

        Keyed by customer_id so all readings for a customer land on the same
        partition, which preserves per-customer chronological ordering. Keying
        by event_id instead would scatter a customer's history across
        partitions and lose that guarantee.
        """
        try:
            self._enqueue(event)
        except BufferError:
            # Local queue is full: the broker isn't keeping up. Block for
            # outstanding deliveries, then retry once rather than dropping.
            logger.warning("Producer queue full; flushing before retry")
            self._producer.flush(self._flush_timeout_seconds)
            self._enqueue(event)
        except KafkaException as exc:
            if not self._is_fatal(exc):
                logger.exception("Failed to enqueue event %s", event.event_id)
                raise
            # Fatal: the producer object is dead. Replace it and retry once so
            # a transient broker-startup race doesn't take the whole run down.
            self._recreate_after_fatal()
            self._enqueue(event)

        # Serve delivery callbacks for already-completed sends without blocking.
        self._producer.poll(0)

    def flush(self) -> int:
        """Block until queued messages are delivered. Returns messages still pending."""
        return self._producer.flush(self._flush_timeout_seconds)

    def close(self) -> None:
        """
        Flush outstanding messages and report the delivery tally.

        Deliberately swallows errors: this runs in the `finally` of the
        entrypoint, and a raise here would replace whatever actually went
        wrong with a shutdown traceback.
        """
        try:
            pending = self.flush()
            if pending > 0:
                logger.warning("Producer closed with %d message(s) undelivered", pending)
        except KafkaException as exc:
            logger.error("Could not flush on shutdown: %s", exc)
        except Exception:  # noqa: BLE001 - shutdown must not mask the real error
            logger.exception("Unexpected error while flushing on shutdown")

        unlogged = max(0, self._delivery_failures - MAX_DELIVERY_ERRORS_LOGGED)
        logger.info(
            "Producer closed (delivered=%d, failed=%d%s)",
            self._delivered,
            self._delivery_failures,
            f", {unlogged} failure(s) not logged individually" if unlogged else "",
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
