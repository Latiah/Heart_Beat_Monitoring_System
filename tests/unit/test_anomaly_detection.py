"""
Unit tests for anomaly classification.

Off-by-one at the threshold is the realistic bug in this function, so the
exact inclusive/exclusive behaviour is pinned down explicitly. Everything else
here is one case per branch.
"""

import pytest

from heartbeat_monitoring.models import ReadingStatus
from heartbeat_monitoring.processing import classify_heart_rate

LOW = 50
HIGH = 120


def classify(heart_rate: int) -> ReadingStatus:
    return classify_heart_rate(heart_rate, LOW, HIGH)


@pytest.mark.parametrize(
    ("heart_rate", "expected"),
    [
        (72, ReadingStatus.NORMAL),
        (35, ReadingStatus.LOW),
        (190, ReadingStatus.HIGH),
    ],
)
def test_each_branch_classifies_as_expected(heart_rate, expected):
    assert classify(heart_rate) == expected


@pytest.mark.parametrize(
    ("heart_rate", "expected"),
    [
        (LOW - 1, ReadingStatus.LOW),
        (LOW, ReadingStatus.NORMAL),
        (HIGH, ReadingStatus.NORMAL),
        (HIGH + 1, ReadingStatus.HIGH),
    ],
)
def test_thresholds_are_inclusive_bounds_of_the_normal_band(heart_rate, expected):
    """A reading equal to a threshold is NORMAL; only strictly-beyond is flagged."""
    assert classify(heart_rate) == expected


def test_thresholds_are_parameters_not_hardcoded_constants():
    """Widening the band must reclassify a previously-flagged reading."""
    assert classify_heart_rate(130, 50, 120) == ReadingStatus.HIGH
    assert classify_heart_rate(130, 40, 180) == ReadingStatus.NORMAL


def test_inverted_thresholds_are_rejected():
    with pytest.raises(ValueError, match="must be below"):
        classify_heart_rate(72, low_threshold=120, high_threshold=50)
