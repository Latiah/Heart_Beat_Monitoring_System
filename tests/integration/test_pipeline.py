"""
End-to-end pipeline tests: Kafka -> consumer -> validation -> classification
-> PostgreSQL.

These publish real messages to a real broker and assert on real rows. Mocks
here would only prove the code calls the methods we told it to; the point of
these tests is to catch wiring mistakes (wrong column, wrong topic, wrong
serialisation) that unit tests structurally cannot see.

Each test gets its own throwaway topic and consumer group, so it starts from a
clean offset and cannot be disturbed by a producer left running elsewhere. The
consumer runs on a background thread because `run()` blocks.
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from heartbeat_monitoring.consumer import HeartRateConsumer
from heartbeat_monitoring.models import HeartRateEvent

pytestmark = pytest.mark.integration

LOW_THRESHOLD = 50
HIGH_THRESHOLD = 120
ROW_WAIT_TIMEOUT_SECONDS = 30.0


@pytest.fixture
def kafka_topic(settings):
    """Create a unique topic for this test and delete it afterwards."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})

    try:
        cluster = admin.list_topics(timeout=10)
    except Exception as exc:
        pytest.skip(f"Kafka unavailable ({exc}); run `make up`")
    if not cluster.brokers:
        pytest.skip("Kafka reports no brokers; run `make up`")

    topic = f"test-heart-rate-{uuid.uuid4().hex[:12]}"
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    for future in admin.create_topics([new_topic]).values():
        future.result(timeout=15)

    yield topic

    admin.delete_topics([topic])


@pytest.fixture
def publish(settings, kafka_topic):
    """Return a function that publishes raw values to the test topic."""
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})

    def _publish(value: str | bytes) -> None:
        payload = value.encode("utf-8") if isinstance(value, str) else value
        producer.produce(topic=kafka_topic, key=b"test-key", value=payload)

    yield _publish
    assert producer.flush(15) == 0, "producer failed to deliver test messages"


@pytest.fixture
def run_consumer(settings, repository, kafka_topic):
    """Start the real consumer on a background thread; stop it on teardown."""
    started: list[HeartRateConsumer] = []
    threads: list[threading.Thread] = []

    def _start() -> HeartRateConsumer:
        consumer = HeartRateConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=kafka_topic,
            consumer_group=f"test-group-{uuid.uuid4().hex[:12]}",
            repository=repository,
            anomaly_low_threshold=LOW_THRESHOLD,
            anomaly_high_threshold=HIGH_THRESHOLD,
            # batch_size=1 removes batching latency so tests aren't waiting on
            # a buffer to fill. Batching itself is covered in test_repository.
            batch_size=1,
            poll_timeout_seconds=0.5,
        )
        thread = threading.Thread(target=consumer.run, daemon=True)
        thread.start()

        started.append(consumer)
        threads.append(thread)
        return consumer

    yield _start

    for consumer in started:
        consumer.stop()
    for thread in threads:
        thread.join(timeout=15)


def wait_for_rows(database, customer_id: str, expected: int) -> list[tuple]:
    """Poll until `expected` rows exist for the customer, or time out."""
    deadline = time.monotonic() + ROW_WAIT_TIMEOUT_SECONDS
    rows: list[tuple] = []

    while time.monotonic() < deadline:
        with database.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT heart_rate, status, is_anomaly FROM heart_rate_readings "
                "WHERE customer_id = %s ORDER BY reading_time",
                (customer_id,),
            )
            rows = cur.fetchall()
        if len(rows) >= expected:
            return rows
        time.sleep(0.25)

    return rows


def event_json(customer_id: str, heart_rate: int, event_id: str | None = None) -> str:
    return HeartRateEvent(
        event_id=event_id or str(uuid.uuid4()),
        customer_id=customer_id,
        timestamp=datetime.now(UTC),
        heart_rate=heart_rate,
    ).to_json()


@pytest.mark.parametrize(
    ("heart_rate", "status", "is_anomaly"),
    [
        (75, "NORMAL", False),
        (35, "LOW", True),
        (190, "HIGH", True),
    ],
)
def test_published_event_reaches_postgres_correctly_classified(
    publish, run_consumer, database, test_customer_id, heart_rate, status, is_anomaly
):
    """The whole pipeline in one assertion: publish a reading, find the row."""
    run_consumer()
    publish(event_json(test_customer_id, heart_rate))

    rows = wait_for_rows(database, test_customer_id, expected=1)

    assert len(rows) == 1, "event never reached PostgreSQL"
    assert rows[0] == (heart_rate, status, is_anomaly)


def test_malformed_messages_do_not_block_subsequent_valid_ones(
    publish, run_consumer, database, test_customer_id
):
    """
    One poison record must not stall the partition. This is the failure mode
    that takes a real pipeline down at 3am.
    """
    run_consumer()

    publish("{ this is not valid json")
    publish(b"\xff\xfe binary garbage")
    publish(event_json(test_customer_id, 400))  # implausible: sensor corruption
    publish(event_json(test_customer_id, 68))  # valid

    rows = wait_for_rows(database, test_customer_id, expected=1)

    assert [r[0] for r in rows] == [68], "valid message after bad ones never arrived"


def test_duplicate_delivery_does_not_duplicate_the_row(
    publish, run_consumer, database, test_customer_id
):
    """Proves at-least-once delivery plus an idempotent write is safe."""
    payload = event_json(test_customer_id, 77, event_id=str(uuid.uuid4()))

    run_consumer()
    publish(payload)
    publish(payload)

    assert len(wait_for_rows(database, test_customer_id, expected=1)) == 1
    time.sleep(2)  # give the consumer a chance to (incorrectly) write a second row
    assert len(wait_for_rows(database, test_customer_id, expected=1)) == 1
