"""Pilot scheduled quarter selection without database writes or CDS requests.

This exercises the actual mode, selector, and monthly mapping boundary against
 the isolated progress state before a real scheduled DagRun can submit campaign data.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

from orchestration.campaign import policy as campaign
from orchestration.config import CAMPAIGN_END_DATE, CAMPAIGN_START_DATE

CAMPAIGN_START_YEAR = CAMPAIGN_START_DATE.year
CAMPAIGN_END_YEAR = CAMPAIGN_END_DATE.year
from orchestration.repositories import pipeline_progress as repository
from orchestration.tasks import build_monthly_requests as job_tasks


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

    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.runtime_database_engine",
        isolated_engine,
    )
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.initialize_progress_storage",
        lambda unused_engine: None,
    )
    monkeypatch.setattr(
        campaign, "initialize_campaign_progress",
        lambda unused_engine, **kwargs: tuple(
            range(1, len(campaign.iter_campaign_quarters()) + 1)
        ),
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
            (date(CAMPAIGN_START_YEAR, 4, 1), date(CAMPAIGN_START_YEAR, 4, 30), "ValidationError"),
        ),
    )

    # Any unexpected provider call makes the pilot fail immediately.
    def unexpected_cds(*args, **kwargs):
        raise AssertionError("The offline selection pilot must not call CDS.")

    monkeypatch.setattr("pipeline.extraction.cds_era5_land._download_cds", unexpected_cds)

    first = job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__first")
    first_start_dates: list[object] = []
    for item in first:
        request_spec = item["request_spec"]
        assert isinstance(request_spec, Mapping)
        first_start_dates.append(request_spec["start_date"])
    assert first_start_dates == [
        f"{CAMPAIGN_START_YEAR}-01-01", f"{CAMPAIGN_START_YEAR}-02-01", f"{CAMPAIGN_START_YEAR}-03-01",
    ]
    assert all(item["run_mode"] == "scheduled" for item in first)
    assert {item["progress_id"] for item in first} == {1}
    assert claims == {1: "scheduled__first"}

    # Another tick cannot move to Q2 while Q1 belongs to the first run.
    assert job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__other") == []
    assert claims == {1: "scheduled__first"}

    statuses[1] = "complete"
    claims.pop(1)
    second = job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__second")
    second_start_dates: list[object] = []
    for item in second:
        request_spec = item["request_spec"]
        assert isinstance(request_spec, Mapping)
        second_start_dates.append(request_spec["start_date"])
    assert second_start_dates == [
        f"{CAMPAIGN_START_YEAR}-04-01", f"{CAMPAIGN_START_YEAR}-05-01", f"{CAMPAIGN_START_YEAR}-06-01",
    ]
    assert {item["progress_id"] for item in second} == {2}

    # A failed month blocks automatic resubmission until manual repair.
    statuses[2] = "failed"
    assert job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__third") == []
    assert "ValidationError" in caplog.text
    assert claims[2] == "scheduled__second"

    statuses[2] = "complete"
    claims.pop(2)
    third = job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__recovered")
    third_start_dates: list[object] = []
    for item in third:
        request_spec = item["request_spec"]
        assert isinstance(request_spec, Mapping)
        third_start_dates.append(request_spec["start_date"])
    assert third_start_dates == [
        f"{CAMPAIGN_START_YEAR}-07-01", f"{CAMPAIGN_START_YEAR}-08-01", f"{CAMPAIGN_START_YEAR}-09-01",
    ]
    assert {item["progress_id"] for item in third} == {3}

    quarter_count = len(campaign.iter_campaign_quarters())
    statuses.update({number: "complete" for number in range(1, quarter_count + 1)})
    assert job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__terminal") == []
    assert reconciled[-quarter_count:] == list(range(1, quarter_count + 1))
