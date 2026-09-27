"""Check that Airflow defers pacing waits without marking extraction failed.

The operator resumes the same monthly task while keeping the existing pool,
retry, and progress contracts for genuine provider errors.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from airflow.sdk.exceptions import TaskDeferred

from orchestration.tasks import cds_extract_operator
from pipeline.extraction.cds import CdsSubmissionDeferred


def test_operator_defers_then_rechecks_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = []

    def extract(spec, progress_id, mode, *, paced):
        attempts.append((progress_id, mode, paced))
        if len(attempts) == 1:
            raise CdsSubmissionDeferred(datetime(2026, 9, 26, tzinfo=timezone.utc))
        return {"boundary": "extract_or_resume"}

    monkeypatch.setattr(cds_extract_operator, "extract_with_progress", extract)
    operator = cds_extract_operator.CdsPacedExtractOperator(
        task_id="paced_test", request_spec={"start_date": "2024-01-01"},
        progress_id=42, run_mode="manual",
    )
    with pytest.raises(TaskDeferred) as deferred:
        operator.execute({})
    assert deferred.value.method_name == "execute_complete"
    assert operator.execute_complete({}) == {"boundary": "extract_or_resume"}
    assert attempts == [(42, "manual", True), (42, "manual", True)]



def test_first_pass_failure_is_handed_to_bounded_retry_after_all_first_maps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def extract(spec, progress_id, mode, *, paced):
        month = spec["start_date"]
        calls.append(month)
        if month == "2016-01-01" and calls.count(month) < 3:
            raise RuntimeError("temporary failure")
        return {"boundary": "extract_or_resume", "request": spec,
                "progress_id": progress_id, "run_mode": mode}

    monkeypatch.setattr(cds_extract_operator, "extract_with_progress", extract)
    first = []
    for month in ("2016-01-01", "2016-02-01", "2016-03-01"):
        first.append(cds_extract_operator.CdsPacedExtractOperator(
            task_id=f"first_{month}", request_spec={"start_date": month},
            progress_id=42, run_mode="scheduled", first_pass=True,
        ).execute({}))
    assert calls == ["2016-01-01", "2016-02-01", "2016-03-01"]
    assert first[0]["first_attempt_failed"] is True
    for index, month in enumerate(("2016-01-01", "2016-02-01", "2016-03-01")):
        operator = cds_extract_operator.CdsPacedExtractOperator(
            task_id=f"retry_{month}", request_spec={"start_date": month},
            progress_id=42, run_mode="scheduled", first_result=first[index],
        )
        if index == 0:
            with pytest.raises(RuntimeError):
                operator.execute({})
            assert operator.execute({})["boundary"] == "extract_or_resume"
        else:
            assert operator.execute({}) is first[index]
    assert calls == ["2016-01-01", "2016-02-01", "2016-03-01",
                     "2016-01-01", "2016-01-01"]


def test_final_retry_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(spec, progress_id, mode, *, paced):
        raise RuntimeError("terminal")

    monkeypatch.setattr(cds_extract_operator, "extract_with_progress", fail)
    operator = cds_extract_operator.CdsPacedExtractOperator(
        task_id="terminal", request_spec={"start_date": "2016-01-01"},
        progress_id=42, run_mode="scheduled", first_result={"first_attempt_failed": True},
        retries=2,
    )
    assert operator.retries == 2
    with pytest.raises(RuntimeError, match="terminal"):
        operator.execute({})
