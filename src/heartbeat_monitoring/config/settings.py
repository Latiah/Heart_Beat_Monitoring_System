"""
Typed application settings.

This is the single place environment variables are read. Every other module
receives plain values as arguments instead of reaching for os.environ, which
keeps them trivially testable and makes the full configuration surface of the
system visible in one file.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for every component of the pipeline."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "heart-rate-readings"
    kafka_client_id: str = "heartbeat-producer"
    kafka_consumer_group: str = "heartbeat-consumer-group"

    # --- PostgreSQL ---
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "heartbeat_monitoring"
    postgres_user: str = "heartbeat_user"
    postgres_password: str = "heartbeat_password"

    # --- Simulation ---
    number_of_customers: int = Field(default=5, ge=1)
    generation_interval_seconds: float = Field(default=1.0, gt=0)
    min_heart_rate: int = Field(default=55, ge=0, le=300)
    max_heart_rate: int = Field(default=100, ge=0, le=300)
    anomaly_probability: float = Field(default=0.05, ge=0.0, le=1.0)

    # --- Anomaly classification (business rule, not a medical threshold) ---
    anomaly_low_threshold: int = Field(default=50, ge=0, le=300)
    anomaly_high_threshold: int = Field(default=120, ge=0, le=300)

    # --- Observability ---
    log_level: str = "INFO"

    @model_validator(mode="after")
    def check_ranges_are_ordered(self) -> Settings:
        """
        Catch inverted bounds at startup rather than letting them surface as
        confusing behaviour deep inside the generator or classifier.
        """
        if self.min_heart_rate > self.max_heart_rate:
            raise ValueError(
                f"MIN_HEART_RATE ({self.min_heart_rate}) must not exceed "
                f"MAX_HEART_RATE ({self.max_heart_rate})"
            )
        if self.anomaly_low_threshold >= self.anomaly_high_threshold:
            raise ValueError(
                f"ANOMALY_LOW_THRESHOLD ({self.anomaly_low_threshold}) must be "
                f"below ANOMALY_HIGH_THRESHOLD ({self.anomaly_high_threshold})"
            )
        return self

    @property
    def postgres_dsn(self) -> str:
        """libpq connection string used by psycopg."""
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the process-wide Settings instance.

    Cached so configuration is parsed and validated exactly once per process;
    tests can clear it with `get_settings.cache_clear()`.
    """
    return Settings()
