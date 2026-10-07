"""Verify progress wiring around mapped ETL boundaries and campaign repair.

The tests exercise production task, campaign, and load-evidence functions while
isolating database/runtime side effects. Production CampaignSelection and
DatasetArtifact models are used instead of look-alike objects. Real PostgreSQL
reconciliation and locking semantics are covered by integration tests.
"""

from contextlib import contextmanager
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from collections.abc import Generator, Mapping

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.campaign import repair as campaign_repair
from orchestration.campaign.policy import (
    CampaignBlocked,
    CampaignQuarter,
    CampaignSelection,
)
from orchestration.config import CAMPAIGN_END_DATE, CAMPAIGN_START_DATE
from orchestration.repositories.load_evidence import persisted_row_count
from orchestration.scheduling.requests import build_historical_request_specs
from orchestration.scheduling.submission_gate import SubmissionDeferred
from orchestration.tasks import build_monthly_requests as job_tasks
from orchestration.tasks import load as load_tasks
from orchestration.tasks import raw_checkpoint as extract_tasks
from pipeline.models.weather import DataLayer, DatasetArtifact

CAMPAIGN_START_YEAR = CAMPAIGN_START_DATE.year
CAMPAIGN_END_YEAR = CAMPAIGN_END_DATE.year


@contextmanager
def _engine() -> Generator[Engine, None, None]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def _campaign_spec() -> dict[str, object]:
    year = CAMPAIGN_START_YEAR
    return build_historical_request_specs(
        f"{year}-01-01", f"{year}-01-31", current_local_date=date.max
    )[0]


def _skip_schema_setup(monkeypatch) -> None:
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.initialize_progress_storage",
        lambda engine: None,
    )


def test_manual_run_initializes_exact_progress_and_maps_one_month(monkeypatch) -> None:
    definitions = []
    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.initialize_pipeline_progress",
        lambda engine, definition: definitions.append(definition) or 42,
    )

    jobs = job_tasks.prepare_monthly_jobs(
        "manual", {"start_date": "2024-01-01", "end_date": "2024-01-31"}
    )

    assert len(jobs) == 1
    first_job = jobs[0]
    assert first_job["progress_id"] == 42
    assert first_job["run_mode"] == "manual"
    first_request_spec = first_job["request_spec"]
    assert isinstance(first_request_spec, Mapping)
    assert first_request_spec["start_date"] == "2024-01-01"
    assert definitions[0].expected_partition_count == 1


def test_scheduled_terminal_run_maps_no_partitions(monkeypatch) -> None:
    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.select_earliest_incomplete_quarter",
        lambda engine, claim_owner: None,
    )
    assert job_tasks.prepare_monthly_jobs(
        "scheduled", None, "scheduled__one"
    ) == []


def test_scheduled_run_maps_only_selected_quarter(monkeypatch) -> None:
    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", _engine)
    _skip_schema_setup(monkeypatch)
    year = CAMPAIGN_START_YEAR
    specs = build_historical_request_specs(
        f"{year}-01-01", f"{year}-03-31", current_local_date=date.max
    )
    selected = CampaignSelection(
        quarter=CampaignQuarter(year, 1),
        progress_id=19,
        request_specs=tuple(specs),
    )
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.select_earliest_incomplete_quarter",
        lambda engine, claim_owner: selected,
    )

    jobs = job_tasks.prepare_monthly_jobs("scheduled", {}, "scheduled__one")
    assert len(jobs) == 3
    assert all(job["progress_id"] == 19 for job in jobs)
    assert all(job["run_mode"] == "scheduled" for job in jobs)
    start_dates: list[object] = []
    for job in jobs:
        request_spec = job["request_spec"]
        assert isinstance(request_spec, Mapping)
        start_dates.append(request_spec["start_date"])
    assert start_dates == [
        f"{year}-01-01", f"{year}-02-01", f"{year}-03-01"
    ]


def test_blocked_scheduled_run_maps_no_partitions(monkeypatch, caplog) -> None:
    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.select_earliest_incomplete_quarter",
        lambda engine, claim_owner: CampaignBlocked(
            CampaignQuarter(CAMPAIGN_START_YEAR, 1),
            19,
            "Manual repair required",
        ),
    )
    with caplog.at_level("WARNING"):
        assert job_tasks.prepare_monthly_jobs(
            "scheduled", None, "scheduled__next"
        ) == []
    assert "Manual repair required" in caplog.text


def test_invalid_manual_conf_fails_before_database_access(monkeypatch) -> None:
    def unexpected():
        raise AssertionError("Database access is not expected.")

    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", unexpected)
    with pytest.raises(ValueError, match="require only"):
        job_tasks.prepare_monthly_jobs("manual", {"start_date": "2024-01-01"})


def test_load_records_completion_only_after_persisted_evidence(monkeypatch) -> None:
    updates = []
    repairs = []
    monkeypatch.setattr("orchestration.tasks.load.runtime_database_engine", _engine)
    monkeypatch.setattr(
        "orchestration.tasks.load.record_progress",
        lambda progress_id, request_key, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.load_partition",
        lambda result: {
            "boundary": "load",
            "request_key": "monthly-key",
            "daily_climate_id": 7,
            "artifact_path": "partition.parquet",
            "rows_processed": 0,
        },
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.artifact_identity",
        lambda result: ("path", "sha", 100),
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.persisted_row_count",
        lambda engine, result, path, sha: 15,
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.reconcile_campaign_repair",
        lambda *args, **kwargs: repairs.append((args, kwargs)),
    )

    result = load_tasks.load_with_progress(
        {
            "progress_id": 42, "request_key": "monthly-key",
            "request": _campaign_spec(), "run_mode": "manual",
        }
    )
    assert result["progress_id"] == 42
    assert updates[0] == {"load_status": "running", "validation_status": "pending"}
    assert updates[-1]["load_status"] == "complete"
    assert updates[-1]["validation_status"] == "valid"
    assert updates[-1]["row_count"] == 15
    assert repairs[0][0][0] == 42
    assert repairs[0][0][2] == "monthly-key"
    assert repairs[0][1]["manual_repair"] is True


def test_load_proof_failure_preserves_failure_for_retry(monkeypatch) -> None:
    updates = []
    monkeypatch.setattr("orchestration.tasks.load.runtime_database_engine", _engine)
    monkeypatch.setattr(
        "orchestration.tasks.load.record_progress",
        lambda progress_id, request_key, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.load_partition",
        lambda result: {"request_key": "monthly-key", "artifact_path": "partition"},
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.artifact_identity",
        lambda result: ("path", "sha", 100),
    )
    monkeypatch.setattr(
        "orchestration.tasks.load.record_failure",
        lambda progress_id, request_key, boundary, error: updates.append(
            {
                f"{boundary}_status": "failed",
                "validation_status": "failed",
                "last_error": type(error).__name__,
            }
        ),
    )

    def invalid_proof(engine, result, path, checksum):
        raise ValueError("Mismatch")

    monkeypatch.setattr("orchestration.tasks.load.persisted_row_count", invalid_proof)
    with pytest.raises(ValueError, match="Mismatch"):
        load_tasks.load_with_progress({"progress_id": 42, "request_key": "monthly-key"})
    assert updates[-1]["load_status"] == "failed"
    assert updates[-1]["validation_status"] == "failed"


@pytest.mark.parametrize(
    "row",
    [
        None,
        {
            "request_key": "other", "transformed_artifact_path": "path",
            "transformed_sha256": "sha", "row_count": 2,
        },
        {
            "request_key": "key", "transformed_artifact_path": "path",
            "transformed_sha256": "different", "row_count": 2,
        },
        {
            "request_key": "key", "transformed_artifact_path": "path",
            "transformed_sha256": "sha", "row_count": 0,
        },
    ],
)
def test_persisted_load_proof_rejects_missing_or_mismatched_rows(
    row: Mapping[str, object] | None,
    monkeypatch,
) -> None:
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement, params):
            assert params == {"daily_climate_id": 7}
            return self

        def mappings(self):
            return self

        @staticmethod
        def one_or_none():
            return row

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    monkeypatch.setattr(engine, "connect", lambda: Connection())
    with pytest.raises(ValueError, match="does not match"):
        persisted_row_count(
            engine, {"daily_climate_id": 7, "request_key": "key"}, "path", "sha"
        )
    engine.dispose()


def test_persisted_load_proof_accepts_matching_rows(monkeypatch) -> None:
    class Result:
        def mappings(self):
            return self

        @staticmethod
        def one_or_none():
            return {
                "request_key": "key",
                "transformed_artifact_path": "path",
                "transformed_sha256": "sha",
                "row_count": 2,
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        @staticmethod
        def execute(statement, params):
            return Result()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    monkeypatch.setattr(engine, "connect", lambda: Connection())
    assert persisted_row_count(
        engine, {"daily_climate_id": 7, "request_key": "key"}, "path", "sha"
    ) == 2
    engine.dispose()


def test_manual_month_reconciles_its_campaign_detail(monkeypatch) -> None:
    from orchestration.campaign.policy import matching_campaign_progress_definition

    definition = matching_campaign_progress_definition(_campaign_spec())
    assert definition is not None
    updates = []
    reconciled = []
    released = []
    monkeypatch.setattr("orchestration.campaign.repair.runtime_database_engine", _engine)
    monkeypatch.setattr(
        "orchestration.campaign.repair.initialize_pipeline_progress",
        lambda engine, received: 91 if received == definition else None,
    )
    monkeypatch.setattr(
        "orchestration.campaign.repair.raw_artifact_for_request",
        lambda request: DatasetArtifact(
            layer=DataLayer.RAW,
            created_at=datetime(2026, 9, 27, tzinfo=timezone.utc),
            path=Path("raw.nc"),
        ),
    )
    monkeypatch.setattr(
        "orchestration.campaign.repair.artifact_identity",
        lambda result: ("raw.nc", "raw-sha", 80),
    )
    monkeypatch.setattr(
        "orchestration.campaign.repair.update_pipeline_progress_detail",
        lambda engine, progress_id, request_key, **fields:
            updates.append((progress_id, request_key, fields)),
    )
    monkeypatch.setattr(
        "orchestration.campaign.repair.reconcile_pipeline_progress",
        lambda engine, progress_id: reconciled.append(progress_id),
    )
    monkeypatch.setattr(
        "orchestration.campaign.repair.release_pipeline_progress_claim",
        lambda engine, progress_id, require_complete:
            released.append((progress_id, require_complete)),
    )
    campaign_repair.reconcile_campaign_repair(
        42, _campaign_spec(), definition.partitions[0].request_key,
        "transformed.parquet", "parquet-sha", 100, 15,
        manual_repair=True,
    )

    assert updates[0][0] == 91
    assert updates[0][2]["extract_status"] == "complete"
    assert updates[0][2]["transform_status"] == "complete"
    assert updates[0][2]["load_status"] == "complete"
    assert updates[0][2]["validation_status"] == "valid"
    assert updates[0][2]["row_count"] == 15
    assert reconciled == [91]
    assert released == [(91, False)]


def test_non_campaign_manual_range_does_not_touch_quarter(monkeypatch) -> None:
    outside_year = CAMPAIGN_END_YEAR + 1
    partial = build_historical_request_specs(
        f"{outside_year}-01-15",
        f"{outside_year}-01-31",
        current_local_date=date.max,
    )[0]

    def unexpected():
        raise AssertionError("Quarter progress must not be touched.")

    monkeypatch.setattr("orchestration.campaign.repair.runtime_database_engine", unexpected)
    campaign_repair.reconcile_campaign_repair(
        42, partial, "partial-key", "path", "sha", 100, 15,
        manual_repair=True,
    )


def test_manual_quarter_run_releases_claim_after_valid_load(monkeypatch) -> None:
    released = []
    monkeypatch.setattr("orchestration.campaign.repair.runtime_database_engine", _engine)
    monkeypatch.setattr(
        "orchestration.campaign.repair.initialize_pipeline_progress",
        lambda engine, definition: 42,
    )
    monkeypatch.setattr(
        "orchestration.campaign.repair.release_pipeline_progress_claim",
        lambda engine, progress_id, require_complete:
            released.append((progress_id, require_complete)),
    )
    campaign_repair.reconcile_campaign_repair(
        42, _campaign_spec(), "monthly-key", "path", "sha", 100, 15,
        manual_repair=True,
    )
    assert released == [(42, False)]


def test_pacing_deferral_does_not_mark_extract_failed(monkeypatch) -> None:
    updates = []
    monkeypatch.setattr(
        "orchestration.tasks.raw_checkpoint.record_progress",
        lambda progress_id, request_key, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        "orchestration.tasks.raw_checkpoint.ensure_raw_checkpoint_partition",
        lambda spec, *, paced: (_ for _ in ()).throw(
            SubmissionDeferred(datetime(2026, 9, 26, tzinfo=timezone.utc))
        ),
    )
    with pytest.raises(SubmissionDeferred):
        extract_tasks.ensure_raw_checkpoint_with_progress(_campaign_spec(), 42, "manual", paced=True)
    assert updates == [{
        "extract_status": "running", "transform_status": "pending",
        "load_status": "pending", "validation_status": "pending",
    }]



def test_scheduled_earliest_slots_share_one_anchor(monkeypatch) -> None:
    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", _engine)
    _skip_schema_setup(monkeypatch)
    year = CAMPAIGN_START_YEAR
    specs = build_historical_request_specs(
        f"{year}-01-01", f"{year}-03-31", current_local_date=date.max
    )
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.select_earliest_incomplete_quarter",
        lambda engine, claim_owner: CampaignSelection(
            quarter=CampaignQuarter(year, 1),
            progress_id=19,
            request_specs=tuple(specs),
        ),
    )
    anchor = datetime(2026, 9, 27, 10, tzinfo=timezone.utc)
    jobs = job_tasks.prepare_monthly_jobs(
        "scheduled", {}, "scheduled__one", slot_anchor=anchor
    )
    assert [job["earliest_start_at"] for job in jobs] == [
        (anchor + timedelta(minutes=15 * offset)).isoformat()
        for offset in range(3)
    ]
    assert len({job["progress_id"] for job in jobs}) == 1


def test_manual_recovery_has_no_scheduled_slot(monkeypatch) -> None:
    monkeypatch.setattr("orchestration.tasks.build_monthly_requests.runtime_database_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        "orchestration.tasks.build_monthly_requests.initialize_pipeline_progress",
        lambda engine, definition: 42,
    )
    jobs = job_tasks.prepare_monthly_jobs(
        "manual", {"start_date": "2024-01-01", "end_date": "2024-01-31"}
    )
    assert jobs[0]["earliest_start_at"] is None
