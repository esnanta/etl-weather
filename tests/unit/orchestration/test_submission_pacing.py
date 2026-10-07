"""Protect the fixed provider pacing policy used by orchestration."""

from config import SUBMISSION_INTERVAL_MINUTES
from orchestration.repositories.submission_pacing import submission_interval_seconds


def test_submission_interval_is_fixed_five_minutes() -> None:
    assert SUBMISSION_INTERVAL_MINUTES == 5
    assert submission_interval_seconds() == 300
