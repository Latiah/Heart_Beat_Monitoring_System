"""
Kafka producer: continuously generates synthetic heartbeat readings and
publishes them to the configured Kafka topic.

Run:
    python producer.py                 # runs forever, ~2 msgs/sec by default
    python producer.py --count 100     # send 100 messages then stop
"""
import argparse
import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import config
from data_generator import generate_reading


def build_producer(retries: int = 10, delay_seconds: float = 3.0) -> KafkaProducer:
    """Connect to Kafka, retrying while the broker finishes starting up."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable as err:
            last_err = err
            print(f"[producer] Kafka not ready yet (attempt {attempt}/{retries}), retrying...")
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not connect to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}") from last_err


def run(count: int | None, interval: float):
    producer = build_producer()
    print(f"[producer] Connected. Streaming to topic '{config.KAFKA_TOPIC}' "
          f"every {interval}s. Press Ctrl+C to stop.")

    sent = 0
    try:
        while count is None or sent < count:
            reading = generate_reading()
            # Key by customer_id so all of one customer's readings land on
            # the same partition and preserve per-customer ordering.
            producer.send(config.KAFKA_TOPIC, key=reading["customer_id"], value=reading)
            sent += 1
            if sent % 20 == 0:
                producer.flush()
                print(f"[producer] sent {sent} messages (latest: {reading})")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[producer] Stopping (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        print(f"[producer] Done. Total messages sent: {sent}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream synthetic heartbeat data to Kafka.")
    parser.add_argument("--count", type=int, default=None, help="number of messages to send (default: run forever)")
    parser.add_argument("--interval", type=float, default=config.MESSAGE_INTERVAL_SECONDS, help="seconds between messages")
    args = parser.parse_args()

    try:
        run(args.count, args.interval)
    except RuntimeError as e:
        print(f"[producer] FATAL: {e}", file=sys.stderr)
        sys.exit(1)
