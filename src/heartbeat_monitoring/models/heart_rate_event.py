"""
The HeartRateEvent model -- the wire format of the pipeline.

This is the single source of truth for what a heart-rate event looks like on
the Kafka topic. The generator constructs it, the producer serializes it, the
consumer deserializes and validates against it. Keeping the schema in one
place means a field can never silently drift between producer and consumer.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class HeartRateEvent(BaseModel):
    """A single heart-rate reading for one customer at one point in time."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str = Field(min_length=1)
    timestamp: datetime
    heart_rate: int
    source: str = "synthetic-generator"

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_timezone_aware(cls, value: datetime) -> datetime:
        """
        Reject naive datetimes outright rather than silently guessing a
        timezone. An event with an ambiguous timestamp is worse than one
        that fails loudly at creation time.
        """
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (use UTC)")
        return value.astimezone(UTC)

    def to_json(self) -> str:
        """Serialize to a JSON string suitable for a Kafka message value."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str | bytes) -> HeartRateEvent:
        """Deserialize a Kafka message value back into an event."""
        return cls.model_validate_json(raw)
