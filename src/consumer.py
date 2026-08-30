"""
Kafka consumer: reads heartbeat messages from Kafka, validates/flags them,
and writes them into PostgreSQL.

Run:
    python consumer.py
"""
import json
import sys
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import config

INSERT_SQL = """
    INSERT INTO heartbeats (customer_id, ts, heart_rate, is_anomaly)
    VALUES %s
"""

BATCH_SIZE = 20
BATCH_TIMEOUT_SECONDS = 2.0


def build_consumer(retries: int = 10, delay_seconds: float = 3.0) -> KafkaConsumer:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="heartbeat-consumer-group",
            )
        except NoBrokersAvailable as err:
            last_err = err
            print(f"[consumer] Kafka not ready yet (attempt {attempt}/{retries}), retrying...")
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not connect to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}") from last_err


def build_pg_connection(retries: int = 10, delay_seconds: float = 3.0):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
            )
        except psycopg2.OperationalError as err:
            last_err = err
            print(f"[consumer] Postgres not ready yet (attempt {attempt}/{retries}), retrying...")
            time.sleep(delay_seconds)
    raise RuntimeError("Could not connect to Postgres") from last_err


def validate_and_flag(reading: dict) -> dict | None:
    """
    Basic validation + anomaly flagging.
    Returns None if the message is malformed and should be dropped;
    otherwise returns the reading annotated with is_anomaly.
    """
    try:
        customer_id = str(reading["customer_id"])
        ts = datetime.fromisoformat(reading["timestamp"])
        heart_rate = int(reading["heart_rate"])
    except (KeyError, ValueError, TypeError) as e:
        print(f"[consumer] Dropping malformed message {reading!r}: {e}")
        return None

    if heart_rate <= 0 or heart_rate >= 300:
        # Physiologically impossible / corrupt sensor reading -- reject outright.
        print(f"[consumer] Rejecting impossible heart_rate={heart_rate} for {customer_id}")
        return None

    is_anomaly = heart_rate < config.VALID_HR_LOW or heart_rate > config.VALID_HR_HIGH
    return {"customer_id": customer_id, "ts": ts, "heart_rate": heart_rate, "is_anomaly": is_anomaly}


def flush_batch(conn, batch: list[dict]):
    if not batch:
        return
    rows = [(r["customer_id"], r["ts"], r["heart_rate"], r["is_anomaly"]) for r in batch]
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()
    anomalies = sum(1 for r in batch if r["is_anomaly"])
    print(f"[consumer] Inserted {len(batch)} rows ({anomalies} flagged as anomalies)")


def run():
    consumer = build_consumer()
    conn = build_pg_connection()
    print(f"[consumer] Connected. Listening on topic '{config.KAFKA_TOPIC}'. Press Ctrl+C to stop.")

    batch: list[dict] = []
    last_flush = time.time()

    try:
        while True:
            records = consumer.poll(timeout_ms=500)
            for _, messages in records.items():
                for msg in messages:
                    validated = validate_and_flag(msg.value)
                    if validated:
                        batch.append(validated)
                        if validated["is_anomaly"]:
                            print(f"[consumer] ANOMALY: {validated['customer_id']} "
                                  f"heart_rate={validated['heart_rate']} at {validated['ts']}")

            now = time.time()
            if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= BATCH_TIMEOUT_SECONDS):
                flush_batch(conn, batch)
                batch = []
                last_flush = now
    except KeyboardInterrupt:
        print("\n[consumer] Stopping (Ctrl+C).")
    finally:
        flush_batch(conn, batch)  # flush anything left in the buffer
        consumer.close()
        conn.close()
        print("[consumer] Shut down cleanly.")


if __name__ == "__main__":
    try:
        run()
    except RuntimeError as e:
        print(f"[consumer] FATAL: {e}", file=sys.stderr)
        sys.exit(1)
