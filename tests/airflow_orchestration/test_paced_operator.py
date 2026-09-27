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
