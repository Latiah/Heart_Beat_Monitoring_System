"""
Shared configuration, loaded from environment variables with sane local
defaults so everything "just works" against the docker-compose.yml stack.
"""
import os

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-stream")

# --- Postgres ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "hbuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "hbpassword")

# --- Simulation ---
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "5"))
CUSTOMER_IDS = [f"cust_{i:03d}" for i in range(1, NUM_CUSTOMERS + 1)]

# Roughly-realistic resting heart rate range, plus how often we deliberately
# inject an out-of-range reading so the anomaly-detection path gets exercised.
NORMAL_HR_MIN = int(os.getenv("NORMAL_HR_MIN", "55"))
NORMAL_HR_MAX = int(os.getenv("NORMAL_HR_MAX", "100"))
ANOMALY_HR_LOW = int(os.getenv("ANOMALY_HR_LOW", "30"))
ANOMALY_HR_HIGH = int(os.getenv("ANOMALY_HR_HIGH", "180"))
ANOMALY_PROBABILITY = float(os.getenv("ANOMALY_PROBABILITY", "0.03"))  # 3%

MESSAGE_INTERVAL_SECONDS = float(os.getenv("MESSAGE_INTERVAL_SECONDS", "0.5"))

# What the consumer considers "out of range" for a valid adult resting/active
# heart rate. Anything outside this is flagged is_anomaly = TRUE (not dropped).
VALID_HR_LOW = int(os.getenv("VALID_HR_LOW", "40"))
VALID_HR_HIGH = int(os.getenv("VALID_HR_HIGH", "200"))
