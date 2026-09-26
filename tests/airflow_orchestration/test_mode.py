from enum import Enum

import pytest

from orchestration.mode import RunMode, resolve_run_mode


class RunType(str, Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"


def test_scheduled_mode_has_no_operator_dates() -> None:
    assert resolve_run_mode(RunType.SCHEDULED, None) == RunMode("scheduled")
    assert resolve_run_mode("scheduled", {}) == RunMode("scheduled")


def test_manual_mode_preserves_explicit_inclusive_dates() -> None:
    assert resolve_run_mode(
        RunType.MANUAL,
        {"start_date": "2024-02-01", "end_date": "2024-02-29"},
    ) == RunMode("manual", "2024-02-01", "2024-02-29")
    assert resolve_run_mode(
        "manual", {"start_date": "2024-01-01", "end_date": "2024-01-01"}
    ).start_date == "2024-01-01"


@pytest.mark.parametrize(
    ("run_type", "conf", "message"),
    [
        ("scheduled", {"start_date": "2024-01-01"}, "Scheduled"),
        ("scheduled", {"mode": "manual"}, "Scheduled"),
        ("manual", None, "require only"),
        ("manual", {"start_date": "2024-01-01"}, "require only"),
        ("manual", {"end_date": "2024-01-31"}, "require only"),
        ("manual", {"start_date": "2024-01-01", "end_date": "2024-01-31", "mode": "scheduled"}, "require only"),
        ("manual", {"start_date": None, "end_date": "2024-01-31"}, "start_date"),
        ("manual", {"start_date": "2024-02-30", "end_date": "2024-03-01"}, "start_date"),
        ("manual", {"start_date": "2024-1-1", "end_date": "2024-01-31"}, "start_date"),
        ("manual", {"start_date": "2024-02-01", "end_date": "2024-01-31"}, "end_date"),
        ("backfill", {}, "Unsupported"),
    ],
)
def test_mode_rejects_ambiguous_or_invalid_inputs(run_type, conf, message) -> None:
    with pytest.raises(ValueError, match=message):
        resolve_run_mode(run_type, conf)
