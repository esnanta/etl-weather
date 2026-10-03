"""Pilot scheduled quarter selection without database writes or CDS requests.

This exercises the actual mode, selector, and monthly mapping boundary against
isolated progress state before a real scheduled DagRun can submit 2016 data.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

from orchestration import campaign
from orchestration.repositories import pipeline_progress as repository
from orchestration.tasks import progress


def test_scheduled_selection_recovery_and_terminal_noop(
    monkeypatch, caplog
) -> None:
    statuses: dict[int, str] = {}
    claims: dict[int, str] = {}
    reconciled: list[int] = []
    engine = object()

    @contextmanager
    def isolated_engine():
        yield engine

    monkeypatch.setattr(progress, "runtime_database_engine", isolated_engine)
    monkeypatch.setattr(progress, "initialize_progress_storage", lambda unused_engine: None)
    monkeypatch.setattr(
        campaign, "initialize_campaign_progress",
        lambda unused_engine, **kwargs: tuple(range(1, 41)),
    )
    monkeypatch.setattr(
        repository, "reconcile_pipeline_progress",
        lambda unused_engine, progress_id: reconciled.append(progress_id),
    )

    def get_progress(unused_engine, progress_id):
        status = statuses.get(progress_id, "pending")
        return SimpleNamespace(
            execution_status=status,
            validation_status="valid" if status == "complete" else "pending",
            failed_partition_count=int(status == "failed"),
        )

    def claim(unused_engine, progress_id, owner):
        if progress_id in claims and claims[progress_id] != owner:
            return False
        claims[progress_id] = owner
        return True

    monkeypatch.setattr(repository, "get_pipeline_progress_by_id", get_progress)
    monkeypatch.setattr(repository, "try_claim_pipeline_progress", claim)
    monkeypatch.setattr(
        repository, "get_failed_progress_partitions",
        lambda unused_engine, progress_id: (
            (date(2016, 4, 1), date(2016, 4, 30), "ValidationError"),
        ),
    )

    # Any unexpected provider call makes the pilot fail immediately.
    def unexpected_cds(*args, **kwargs):
        raise AssertionError("The offline selection pilot must not call CDS.")

    monkeypatch.setattr("pipeline.extraction.cds._download_cds", unexpected_cds)

    first = progress.prepare_monthly_jobs("scheduled", {}, "scheduled__first")
    assert [item["request_spec"]["start_date"] for item in first] == [
        "2016-01-01", "2016-02-01", "2016-03-01",
    ]
    assert all(item["run_mode"] == "scheduled" for item in first)
    assert {item["progress_id"] for item in first} == {1}
    assert claims == {1: "scheduled__first"}

    # Another tick cannot move to Q2 while Q1 belongs to the first run.
    assert progress.prepare_monthly_jobs("scheduled", {}, "scheduled__other") == []
    assert claims == {1: "scheduled__first"}

    statuses[1] = "complete"
    claims.pop(1)
    second = progress.prepare_monthly_jobs("scheduled", {}, "scheduled__second")
    assert [item["request_spec"]["start_date"] for item in second] == [
        "2016-04-01", "2016-05-01", "2016-06-01",
    ]
    assert {item["progress_id"] for item in second} == {2}

    # A failed month blocks automatic resubmission until manual repair.
    statuses[2] = "failed"
    assert progress.prepare_monthly_jobs("scheduled", {}, "scheduled__third") == []
    assert "ValidationError" in caplog.text
    assert claims[2] == "scheduled__second"

    statuses[2] = "complete"
    claims.pop(2)
    third = progress.prepare_monthly_jobs("scheduled", {}, "scheduled__recovered")
    assert [item["request_spec"]["start_date"] for item in third] == [
        "2016-07-01", "2016-08-01", "2016-09-01",
    ]
    assert {item["progress_id"] for item in third} == {3}

    statuses.update({number: "complete" for number in range(1, 41)})
    assert progress.prepare_monthly_jobs("scheduled", {}, "scheduled__terminal") == []
    assert reconciled[-40:] == list(range(1, 41))
