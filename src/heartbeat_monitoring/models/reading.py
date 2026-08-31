"""
The persistence-side model.

A HeartRateEvent is what arrives off Kafka; a HeartRateReading is that event
after it has been validated and classified, and is what the repository writes
to PostgreSQL. Keeping the two apart means the classification step has an
explicit, typed output rather than mutating the inbound event in place.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from heartbeat_monitoring.models.heart_rate_event import HeartRateEvent


class ReadingStatus(StrEnum):
    """
    Business classification of a reading.

    Mirrors the CHECK constraint on heart_rate_readings.status, so an invalid
    status is impossible to construct in Python rather than being caught only
    at INSERT time.
    """

    NORMAL = "NORMAL"
    LOW = "LOW"
    HIGH = "HIGH"


class HeartRateReading(BaseModel):
    """A validated, classified reading ready to be persisted."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str
    customer_id: str
    reading_time: datetime
    heart_rate: int
    status: ReadingStatus

    @property
    def is_anomaly(self) -> bool:
        """Anything not classified NORMAL is an anomaly. Derived, never stored twice."""
        return self.status is not ReadingStatus.NORMAL

    @classmethod
    def from_event(cls, event: HeartRateEvent, status: ReadingStatus) -> HeartRateReading:
        """Build a persistable reading from an inbound event plus its classification."""
        return cls(
            event_id=event.event_id,
            customer_id=event.customer_id,
            reading_time=event.timestamp,
            heart_rate=event.heart_rate,
            status=status,
        )
