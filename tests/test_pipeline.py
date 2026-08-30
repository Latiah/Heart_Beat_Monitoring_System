"""
Simple end-to-end smoke test:
1. Sends a handful of known-good, known-bad, and known-anomalous readings
   directly onto the Kafka topic.
2. Waits a few seconds for the consumer to process them.
3. Queries Postgres to confirm the expected rows landed with the right
   is_anomaly flags.

Run this AFTER `producer.py`/`consumer.py` style setup, with the consumer
already running in another terminal (or run `docker compose up` +
`python consumer.py` first). Usage:

    python test_pipeline.py
"""
import json
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer

import config

TEST_CUSTOMER = "cust_TEST"


def send_test_messages():
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    test_cases = [
        {"heart_rate": 72, "expected_anomaly": False, "label": "normal"},
        {"heart_rate": 220, "expected_anomaly": True, "label": "too high"},
        {"heart_rate": 25, "expected_anomaly": True, "label": "too low"},
    ]

    for case in test_cases:
        reading = {
            "customer_id": TEST_CUSTOMER,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heart_rate": case["heart_rate"],
        }
        producer.send(config.KAFKA_TOPIC, key=TEST_CUSTOMER, value=reading)
        print(f"[test] sent {case['label']} reading: {reading}")

    producer.flush()
    producer.close()
    return test_cases


def check_results(test_cases, wait_seconds=8):
    print(f"[test] waiting {wait_seconds}s for the consumer to process messages...")
    time.sleep(wait_seconds)

    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT heart_rate, is_anomaly FROM heartbeats "
            "WHERE customer_id = %s ORDER BY ingested_at DESC LIMIT %s",
            (TEST_CUSTOMER, len(test_cases)),
        )
        rows = cur.fetchall()
    conn.close()

    print(f"[test] found {len(rows)} row(s) in Postgres for {TEST_CUSTOMER}: {rows}")
    if len(rows) < len(test_cases):
        print("[test] FAIL: fewer rows than expected. Is the consumer running?")
        return False

    by_rate = {hr: anomaly for hr, anomaly in rows}
    ok = True
    for case in test_cases:
        got = by_rate.get(case["heart_rate"])
        status = "OK" if got == case["expected_anomaly"] else "MISMATCH"
        if status != "OK":
            ok = False
        print(f"[test] heart_rate={case['heart_rate']} ({case['label']}): "
              f"expected is_anomaly={case['expected_anomaly']}, got={got} -> {status}")

    print("[test] RESULT:", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    cases = send_test_messages()
    check_results(cases)
