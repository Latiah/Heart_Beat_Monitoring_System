"""
Anomaly classification.

These thresholds are a *business rule for a simulation*, not a medical
diagnosis. A resting rate of 45 bpm is normal for a trained athlete and
alarming for someone else; this system has no per-customer clinical baseline,
so it applies one configurable global rule and says so plainly.

Separated from `validation/` on purpose: validation decides whether a message
is usable at all, classification decides what a usable message *means*. The
thresholds are parameters rather than module constants so they can be tuned
via configuration without touching code.
"""

from __future__ import annotations

from heartbeat_monitoring.models import ReadingStatus


def classify_heart_rate(
    heart_rate: int,
    low_threshold: int,
    high_threshold: int,
) -> ReadingStatus:
    """
    Classify a heart rate as LOW, HIGH, or NORMAL.

    Thresholds are inclusive bounds of the normal band: a reading equal to
    either threshold is NORMAL, and only strictly-beyond values are flagged.
    Boundary behaviour is stated here because "is 120 an anomaly?" is exactly
    the question an off-by-one in this function makes unanswerable.
    """
    if low_threshold >= high_threshold:
        raise ValueError(
            f"low_threshold ({low_threshold}) must be below high_threshold ({high_threshold})"
        )

    if heart_rate < low_threshold:
        return ReadingStatus.LOW
    if heart_rate > high_threshold:
        return ReadingStatus.HIGH
    return ReadingStatus.NORMAL
