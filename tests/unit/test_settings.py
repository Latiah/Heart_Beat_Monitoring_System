"""
Unit tests for configuration.

Only the cross-field rules are worth testing -- those are our own logic, and
inverted bounds are a real footgun. Pydantic's own type coercion is not
retested here. `_env_file=None` stops these from depending on the
developer's local .env.
"""

import pytest
from pydantic import ValidationError

from heartbeat_monitoring.config import Settings


def build(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def test_defaults_load_without_any_environment():
    settings = build()
    assert settings.kafka_topic == "heart-rate-readings"
    assert settings.postgres_db == "heartbeat_monitoring"


def test_normal_band_defaults_sit_inside_the_anomaly_thresholds():
    """
    Otherwise a "normal" generated reading would be flagged as an anomaly and
    every demo run would look broken.
    """
    settings = build()
    assert settings.anomaly_low_threshold <= settings.min_heart_rate
    assert settings.max_heart_rate <= settings.anomaly_high_threshold


def test_inverted_heart_rate_range_is_rejected():
    with pytest.raises(ValidationError, match="MIN_HEART_RATE"):
        build(min_heart_rate=120, max_heart_rate=60)


def test_inverted_anomaly_thresholds_are_rejected():
    with pytest.raises(ValidationError, match="ANOMALY_LOW_THRESHOLD"):
        build(anomaly_low_threshold=150, anomaly_high_threshold=60)


def test_dsn_contains_every_connection_parameter():
    dsn = build(postgres_host="db.internal", postgres_port=6000).postgres_dsn
    assert "host=db.internal" in dsn
    assert "port=6000" in dsn
    assert "dbname=heartbeat_monitoring" in dsn
