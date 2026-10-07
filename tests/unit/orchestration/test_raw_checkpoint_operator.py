"""Check that Airflow defers pacing waits without marking extraction failed.

The operator resumes the same monthly task while keeping the existing pool,
retry, and progress contracts for genuine provider errors.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from airflow.sdk.exceptions import TaskDeferred

from orchestration.operators import raw_checkpoint
from orchestration.scheduling.submission_gate import SubmissionDeferred




@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0s"),
        (29.1, "30s"),
        (1723, "28m 43s"),
        (3600, "1h 0m 0s"),
        (3723, "1h 2m 3s"),
    ],
)
def test_format_wait_duration(seconds: float, expected: str) -> None:
    assert raw_checkpoint.format_wait_duration(seconds) == expected


def test_operator_exposes_month_label_after_runtime_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.ensure_raw_checkpoint_with_progress",
        lambda *args, **kwargs: {"boundary": "ensure_raw_checkpoint"},
    )
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="label_test",
        request_spec={"start_date": "2016-02-01"},
        month_label="2016-02",
        progress_id=42,
        run_mode="scheduled",
    )

    assert operator.month_label == "2016-02"
    operator.execute({})


def test_operator_rejects_missing_month_label_source_at_runtime() -> None:
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="invalid_label",
        request_spec={},
        month_label="2016-02",
        progress_id=42,
        run_mode="scheduled",
    )

    with pytest.raises(ValueError, match="request_spec.start_date"):
        operator.execute({})


def test_operator_rejects_mismatched_explicit_month_label() -> None:
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="invalid_label_mismatch",
        request_spec={"start_date": "2016-02-01"},
        month_label="2016-03",
        progress_id=42,
        run_mode="scheduled",
    )

    with pytest.raises(ValueError, match="month_label"):
        operator.execute({})


def test_operator_defers_then_rechecks_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = []

    def extract(spec, progress_id, mode, *, paced):
        attempts.append((progress_id, mode, paced))
        if len(attempts) == 1:
            raise SubmissionDeferred(datetime(2026, 9, 26, tzinfo=timezone.utc))
        return {"boundary": "ensure_raw_checkpoint"}

    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.ensure_raw_checkpoint_with_progress",
        extract,
    )
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="paced_test", request_spec={"start_date": "2024-01-01"},
        month_label="2024-01", progress_id=42, run_mode="manual",
    )
    with pytest.raises(TaskDeferred) as deferred:
        operator.execute({})
    assert getattr(deferred.value, "method_name", None) == "execute_complete"
    assert operator.execute_complete({}) == {"boundary": "ensure_raw_checkpoint"}
    assert attempts == [(42, "manual", True), (42, "manual", True)]



def test_successful_first_result_passes_through_without_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_result = {
        "boundary": "ensure_raw_checkpoint",
        "request": {"start_date": "2016-01-01"},
        "progress_id": 42,
        "run_mode": "scheduled",
    }

    def unexpected_barrier(progress_id: int) -> bool:
        raise AssertionError("Successful first results must not inspect the barrier.")

    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.first_pass_barrier_complete",
        unexpected_barrier,
    )
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="resolve_success",
        request_spec={"start_date": "2016-01-01"},
        month_label="2016-01",
        progress_id=42,
        run_mode="scheduled",
        first_result=first_result,
    )

    assert operator.execute({}) is first_result


def test_failed_first_result_defers_until_first_pass_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.first_pass_barrier_complete",
        lambda progress_id: False,
    )
    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.ensure_raw_checkpoint_with_progress",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="resolve_failed_waiting",
        request_spec={"start_date": "2016-02-01"},
        month_label="2016-02",
        progress_id=42,
        run_mode="scheduled",
        first_result={"first_attempt_failed": True},
    )

    with pytest.raises(TaskDeferred) as deferred:
        operator.execute({})

    assert getattr(deferred.value, "method_name", None) == "execute_complete"
    assert calls == []


def test_failed_first_result_retries_after_first_pass_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.first_pass_barrier_complete",
        lambda progress_id: True,
    )

    def extract(spec, progress_id, mode, *, paced):
        calls.append((spec["start_date"], progress_id, mode, paced))
        return {
            "boundary": "ensure_raw_checkpoint",
            "request": spec,
            "progress_id": progress_id,
            "run_mode": mode,
        }

    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.ensure_raw_checkpoint_with_progress",
        extract,
    )
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="resolve_failed_ready",
        request_spec={"start_date": "2016-02-01"},
        month_label="2016-02",
        progress_id=42,
        run_mode="scheduled",
        first_result={"first_attempt_failed": True},
    )

    result = operator.execute({})

    assert result["boundary"] == "ensure_raw_checkpoint"
    assert calls == [("2016-02-01", 42, "scheduled", True)]


def test_final_retry_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(spec, progress_id, mode, *, paced):
        raise RuntimeError("terminal")

    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.ensure_raw_checkpoint_with_progress",
        fail,
    )
    monkeypatch.setattr(
        "orchestration.operators.raw_checkpoint.first_pass_barrier_complete",
        lambda progress_id: True,
    )
    operator = raw_checkpoint.RawCheckpointOperator(
        task_id="terminal", request_spec={"start_date": "2016-01-01"},
        month_label="2016-01", progress_id=42, run_mode="scheduled",
        first_result={"first_attempt_failed": True},
        retries=1,
    )
    assert operator.retries == 1
    with pytest.raises(RuntimeError, match="terminal"):
        operator.execute({})
