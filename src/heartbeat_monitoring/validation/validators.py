"""
Structural validation of inbound Kafka messages.

The single most important distinction in this module -- and the reason it is
separate from `processing/` -- is:

  * **invalid**  = the message is not a usable heart-rate reading at all
                   (malformed JSON, missing field, wrong type, physiologically
                   impossible value). It is dropped and logged.

  * **valid but abnormal** = the message is a perfectly well-formed reading
                   that happens to record an alarming heart rate. It is
                   *stored*, flagged by `processing/`, and never dropped.

Conflating the two is the classic bug in this kind of pipeline: you filter out
"bad" heart rates and silently throw away exactly the events the monitoring
system exists to catch.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError

from heartbeat_monitoring.models import HeartRateEvent

# A reading outside these bounds is treated as sensor corruption, not as a
# medical emergency: no living person registers 0 or 400 bpm. Kept wider than
# the anomaly thresholds so genuine extremes still make it into the database.
MIN_PLAUSIBLE_HEART_RATE = 20
MAX_PLAUSIBLE_HEART_RATE = 250


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of validating one raw message."""

    event: HeartRateEvent | None
    error: str | None = None

    @property
    def is_valid(self) -> bool:
        return self.event is not None

    @classmethod
    def ok(cls, event: HeartRateEvent) -> ValidationResult:
        return cls(event=event)

    @classmethod
    def invalid(cls, error: str) -> ValidationResult:
        return cls(event=None, error=error)


def validate_message(raw: str | bytes) -> ValidationResult:
    """
    Parse and structurally validate one raw Kafka message value.

    Never raises: a bad message must not be able to kill the consumer loop, so
    every failure mode is returned as an invalid ValidationResult instead.
    """
    if raw is None or (isinstance(raw, (str, bytes)) and len(raw) == 0):
        return ValidationResult.invalid("empty message payload")

    try:
        event = HeartRateEvent.from_json(raw)
    except ValidationError as exc:
        return ValidationResult.invalid(f"schema validation failed: {exc.errors()!r}")
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        return ValidationResult.invalid(f"could not decode message: {exc}")

    if not (MIN_PLAUSIBLE_HEART_RATE <= event.heart_rate <= MAX_PLAUSIBLE_HEART_RATE):
        return ValidationResult.invalid(
            f"heart_rate {event.heart_rate} outside plausible sensor range "
            f"[{MIN_PLAUSIBLE_HEART_RATE}, {MAX_PLAUSIBLE_HEART_RATE}] "
            f"-- treating as corrupt reading"
        )

    return ValidationResult.ok(event)
