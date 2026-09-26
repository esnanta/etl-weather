from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import pytest

from orchestration.campaign import CampaignBlocked, CampaignQuarter
from orchestration.tasks import progress
from orchestration.tasks.era5_land import build_historical_request_specs


@contextmanager
def _engine():
    yield object()


def _spec():
    return build_historical_request_specs(
        "2024-01-01", "2024-01-31", current_local_date=date(2026, 9, 26)
    )[0]


def _skip_schema_setup(monkeypatch) -> None:
    for name in (
        "create_schema", "create_daily_climate_table",
        "create_daily_climate_detail_table", "create_pipeline_progress_tables",
    ):
        monkeypatch.setattr(progress, name, lambda engine: None)


def test_manual_run_initializes_exact_progress_and_maps_one_month(monkeypatch) -> None:
    definitions = []
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        progress, "initialize_pipeline_progress",
        lambda engine, definition: definitions.append(definition) or 42,
    )

    jobs = progress.prepare_monthly_jobs(
        "manual", {"start_date": "2024-01-01", "end_date": "2024-01-31"}
    )

    assert len(jobs) == 1
    assert jobs[0]["progress_id"] == 42
    assert jobs[0]["run_mode"] == "manual"
    assert jobs[0]["request_spec"]["start_date"] == "2024-01-01"
    assert definitions[0].expected_partition_count == 1


def test_scheduled_terminal_run_maps_no_partitions(monkeypatch) -> None:
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        progress, "select_earliest_incomplete_quarter",
        lambda engine, claim_owner: None,
    )
    assert progress.prepare_monthly_jobs(
        "scheduled", None, "scheduled__one"
    ) == []


def test_scheduled_run_maps_only_selected_quarter(monkeypatch) -> None:
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    _skip_schema_setup(monkeypatch)
    specs = build_historical_request_specs(
        "2024-01-01", "2024-03-31", current_local_date=date(2026, 9, 26)
    )
    selected = SimpleNamespace(
        quarter=CampaignQuarter(2024, 1),
        progress_id=19,
        request_specs=tuple(specs),
    )
    monkeypatch.setattr(
        progress, "select_earliest_incomplete_quarter",
        lambda engine, claim_owner: selected,
    )

    jobs = progress.prepare_monthly_jobs("scheduled", {}, "scheduled__one")
    assert len(jobs) == 3
    assert all(job["progress_id"] == 19 for job in jobs)
    assert all(job["run_mode"] == "scheduled" for job in jobs)
    assert [job["request_spec"]["start_date"] for job in jobs] == [
        "2024-01-01", "2024-02-01", "2024-03-01"
    ]


def test_blocked_scheduled_run_maps_no_partitions(monkeypatch, caplog) -> None:
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    _skip_schema_setup(monkeypatch)
    monkeypatch.setattr(
        progress, "select_earliest_incomplete_quarter",
        lambda engine, claim_owner: CampaignBlocked(
            CampaignQuarter(2024, 1), 19, "Manual repair required"
        ),
    )
    with caplog.at_level("WARNING"):
        assert progress.prepare_monthly_jobs(
            "scheduled", None, "scheduled__next"
        ) == []
    assert "Manual repair required" in caplog.text


def test_invalid_manual_conf_fails_before_database_access(monkeypatch) -> None:
    def unexpected():
        raise AssertionError("Database access is not expected.")

    monkeypatch.setattr(progress, "_progress_engine", unexpected)
    with pytest.raises(ValueError, match="require only"):
        progress.prepare_monthly_jobs("manual", {"start_date": "2024-01-01"})


def test_load_records_completion_only_after_persisted_evidence(monkeypatch) -> None:
    updates = []
    repairs = []
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    monkeypatch.setattr(
        progress, "_record",
        lambda progress_id, request_key, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        progress, "load_partition",
        lambda result: {
            "boundary": "load",
            "request_key": "monthly-key",
            "daily_climate_id": 7,
            "artifact_path": "partition.parquet",
            "rows_processed": 0,
        },
    )
    monkeypatch.setattr(progress, "_identity", lambda result: ("path", "sha", 100))
    monkeypatch.setattr(
        progress, "_persisted_row_count", lambda engine, result, path, sha: 15
    )
    monkeypatch.setattr(
        progress, "_reconcile_campaign_repair",
        lambda *args, **kwargs: repairs.append((args, kwargs)),
    )

    result = progress.load_with_progress(
        {
            "progress_id": 42, "request_key": "monthly-key",
            "request": _spec(), "run_mode": "manual",
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
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    monkeypatch.setattr(
        progress, "_record",
        lambda progress_id, request_key, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        progress, "load_partition",
        lambda result: {"request_key": "monthly-key", "artifact_path": "partition"},
    )
    monkeypatch.setattr(progress, "_identity", lambda result: ("path", "sha", 100))

    def invalid_proof(engine, result, path, checksum):
        raise ValueError("Mismatch")

    monkeypatch.setattr(progress, "_persisted_row_count", invalid_proof)
    with pytest.raises(ValueError, match="Mismatch"):
        progress.load_with_progress({"progress_id": 42, "request_key": "monthly-key"})
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
def test_persisted_load_proof_rejects_missing_or_mismatched_rows(row) -> None:
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

        def one_or_none(self):
            return row

    engine = SimpleNamespace(connect=lambda: Connection())
    with pytest.raises(ValueError, match="does not match"):
        progress._persisted_row_count(
            engine, {"daily_climate_id": 7, "request_key": "key"}, "path", "sha"
        )


def test_persisted_load_proof_accepts_matching_rows() -> None:
    class Result:
        def mappings(self):
            return self

        def one_or_none(self):
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

        def execute(self, statement, params):
            return Result()

    engine = SimpleNamespace(connect=lambda: Connection())
    assert progress._persisted_row_count(
        engine, {"daily_climate_id": 7, "request_key": "key"}, "path", "sha"
    ) == 2


def test_manual_month_reconciles_its_campaign_detail(monkeypatch) -> None:
    from orchestration.campaign import matching_campaign_progress_definition

    definition = matching_campaign_progress_definition(_spec())
    assert definition is not None
    updates = []
    reconciled = []
    released = []
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    monkeypatch.setattr(
        progress, "initialize_pipeline_progress",
        lambda engine, received: 91 if received == definition else None,
    )
    monkeypatch.setattr(
        progress, "raw_artifact_for_request",
        lambda request: SimpleNamespace(
            path=SimpleNamespace(as_posix=lambda: "raw.nc")
        ),
    )
    monkeypatch.setattr(progress, "_identity", lambda result: ("raw.nc", "raw-sha", 80))
    monkeypatch.setattr(
        progress, "update_pipeline_progress_detail",
        lambda engine, progress_id, request_key, **fields:
            updates.append((progress_id, request_key, fields)),
    )
    monkeypatch.setattr(
        progress, "reconcile_pipeline_progress",
        lambda engine, progress_id: reconciled.append(progress_id),
    )
    monkeypatch.setattr(
        progress, "release_pipeline_progress_claim",
        lambda engine, progress_id, require_complete:
            released.append((progress_id, require_complete)),
    )
    progress._reconcile_campaign_repair(
        42, _spec(), definition.partitions[0].request_key,
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
    partial = build_historical_request_specs(
        "2024-01-15", "2024-01-31", current_local_date=date(2026, 9, 26)
    )[0]

    def unexpected():
        raise AssertionError("Quarter progress must not be touched.")

    monkeypatch.setattr(progress, "_progress_engine", unexpected)
    progress._reconcile_campaign_repair(
        42, partial, "partial-key", "path", "sha", 100, 15,
        manual_repair=True,
    )


def test_manual_quarter_run_releases_claim_after_valid_load(monkeypatch) -> None:
    released = []
    monkeypatch.setattr(progress, "_progress_engine", _engine)
    monkeypatch.setattr(
        progress, "initialize_pipeline_progress",
        lambda engine, definition: 42,
    )
    monkeypatch.setattr(
        progress, "release_pipeline_progress_claim",
        lambda engine, progress_id, require_complete:
            released.append((progress_id, require_complete)),
    )
    progress._reconcile_campaign_repair(
        42, _spec(), "monthly-key", "path", "sha", 100, 15,
        manual_repair=True,
    )
    assert released == [(42, False)]
