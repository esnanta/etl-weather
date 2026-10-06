from typing import cast

import pytest
from sqlalchemy.engine import Engine

from orchestration.config import (
    PIPELINE_PROGRESS_DETAIL_TABLE,
    PIPELINE_PROGRESS_SCHEMA,
    PIPELINE_PROGRESS_TABLE,
)
from orchestration.progress.definition import (
    PipelineProgressDefinition,
    ProgressPartition,
)
from orchestration.repositories.pipeline_progress import (
    are_extract_attempts_terminal,
    create_pipeline_progress_tables,
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    release_pipeline_progress_claim,
    try_claim_pipeline_progress,
    update_pipeline_progress_detail,
)


class _ScalarResult:
    rowcount = 1

    @staticmethod
    def scalar_one() -> int:
        return 42

    @staticmethod
    def scalar_one_or_none() -> int:
        return 42


class _Result:
    rowcount = 1


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.params: list[dict[str, object] | None] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(params)
        if "RETURNING id" in sql:
            return _ScalarResult()
        return _Result()


class _Context:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> _Connection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()

    def begin(self) -> _Context:
        return _Context(self.connection)


def _definition() -> PipelineProgressDefinition:
    return PipelineProgressDefinition(
        pipeline_name="cds_era5_land_etl",
        source="cds",
        dataset_name="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-03-31",
        configuration_identity="config-identity",
        partitions=(
            ProgressPartition("jan-key", "2024-01-01", "2024-01-31"),
            ProgressPartition("feb-key", "2024-02-01", "2024-02-29"),
            ProgressPartition("mar-key", "2024-03-01", "2024-03-31"),
        ),
    )


def test_progress_ddl_is_cadence_neutral_parent_detail_model() -> None:
    engine = _Engine()

    create_pipeline_progress_tables(cast(Engine, cast(object, engine)))

    sql = "\n".join(engine.connection.statements)
    assert f"CREATE SCHEMA IF NOT EXISTS {PIPELINE_PROGRESS_SCHEMA}" in sql
    assert (
        f"CREATE TABLE IF NOT EXISTS "
        f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE}"
    ) in sql
    assert (
        f"CREATE TABLE IF NOT EXISTS "
        f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}"
    ) in sql
    assert "configuration_identity TEXT NOT NULL" in sql
    assert "expected_partition_count INTEGER NOT NULL" in sql
    assert "completed_partition_count INTEGER NOT NULL DEFAULT 0" in sql
    assert "failed_partition_count INTEGER NOT NULL DEFAULT 0" in sql
    assert "pipeline_progress_id BIGINT NOT NULL" in sql
    assert "request_key TEXT NOT NULL" in sql
    assert "raw_artifact_path TEXT" in sql
    assert "raw_sha256 TEXT" in sql
    assert "transformed_artifact_path TEXT" in sql
    assert "transformed_sha256 TEXT" in sql
    assert "temporal_count INTEGER" in sql
    assert "grid_count INTEGER" in sql
    assert "row_count INTEGER" in sql
    assert "extract_status TEXT NOT NULL DEFAULT 'pending'" in sql
    assert "validation_status TEXT NOT NULL DEFAULT 'pending'" in sql
    assert "metrics JSONB" in sql
    assert "quarter SMALLINT" not in sql
    assert "campaign_key" not in sql


def test_initialize_progress_seeds_parent_and_expected_details() -> None:
    engine = _Engine()

    progress_id = initialize_pipeline_progress(
        cast(Engine, cast(object, engine)),
        _definition(),
    )

    sql = "\n".join(engine.connection.statements)
    assert progress_id == 42
    assert (
        f"INSERT INTO {PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE}"
    ) in sql
    assert "configuration_identity" in sql
    assert "ON CONFLICT (" in sql
    assert (
        f"INSERT INTO {PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}"
    ) in sql
    detail_params: list[dict[str, object]] = []
    for params in engine.connection.params:
        if isinstance(params, dict) and "request_key" in params:
            detail_params.append(params)
    assert [params["request_key"] for params in detail_params] == [
        "jan-key",
        "feb-key",
        "mar-key",
    ]


def test_detail_update_keeps_status_and_validation_as_columns() -> None:
    engine = _Engine()

    update_pipeline_progress_detail(
        cast(Engine, cast(object, engine)),
        42,
        "feb-key",
        extract_status="complete",
        transform_status="complete",
        load_status="failed",
        validation_status="failed",
        validation_message="row count mismatch",
        transformed_sha256="abc123",
        row_count=123,
        metrics={"peak_rss_mib": 512.5},
    )

    sql = engine.connection.statements[-1]
    params = engine.connection.params[-1]
    assert params is not None
    assert "load_status = COALESCE(:load_status, load_status)" in sql
    assert "validation_status = COALESCE(:validation_status, validation_status)" in sql
    assert "transformed_sha256 = COALESCE" in sql
    assert "row_count = COALESCE(:row_count, row_count)" in sql
    assert "CAST(:metrics AS JSONB)" in sql
    assert params["load_status"] == "failed"
    assert params["validation_status"] == "failed"
    assert params["metrics"] == '{"peak_rss_mib": 512.5}'


def test_detail_update_rejects_invalid_status() -> None:
    engine = _Engine()

    with pytest.raises(ValueError, match="Unsupported load_status"):
        update_pipeline_progress_detail(
            cast(Engine, cast(object, engine)),
            42,
            "jan-key",
            load_status="successful",
        )


def test_parent_completion_is_derived_from_all_detail_evidence() -> None:
    engine = _Engine()

    reconcile_pipeline_progress(cast(Engine, cast(object, engine)), 42)

    sql = engine.connection.statements[-1]
    assert "COUNT(partition.id) FILTER" in sql
    assert "partition.extract_status = 'complete'" in sql
    assert "partition.transform_status = 'complete'" in sql
    assert "partition.load_status = 'complete'" in sql
    assert "partition.validation_status = 'valid'" in sql
    assert "persisted.request_key = partition.request_key" in sql
    assert "persisted.source::text = batch.source" in sql
    assert "persisted.dataset_name = batch.dataset_name" in sql
    assert "persisted.start_date = partition.start_date" in sql
    assert "persisted.end_date = partition.end_date" in sql
    assert "persisted.transformed_artifact_path =" in sql
    assert "persisted.transformed_sha256 =" in sql
    assert "observations.row_count = partition.row_count" in sql
    assert "detail_state.actual_count = progress.expected_partition_count" in sql
    assert "detail_state.completed_count = progress.expected_partition_count" in sql
    assert "detail_state.failed_count = 0" in sql
    assert "THEN 'complete'" in sql
    assert "THEN 'failed'" in sql
    assert "ELSE 'pending'" in sql
    assert "detail_state.attempted_count > 0" in sql
    assert "COALESCE(progress.completed_at, CURRENT_TIMESTAMP)" in sql
    assert "completed_partition_count = detail_state.completed_count" in sql
    assert "failed_partition_count = detail_state.failed_count" in sql


def test_claim_is_atomic_and_released_only_after_verified_completion() -> None:
    engine = _Engine()
    assert try_claim_pipeline_progress(
        cast(Engine, cast(object, engine)), 42, "scheduled__one"
    )
    claim_sql = engine.connection.statements[-1]
    assert "selection_owner IS NULL OR selection_owner = :owner" in claim_sql
    assert "failed_partition_count = 0" in claim_sql
    assert "RETURNING id" in claim_sql

    assert release_pipeline_progress_claim(
        cast(Engine, cast(object, engine)), 42, require_complete=True
    )
    release_sql = engine.connection.statements[-1]
    assert "execution_status = 'complete'" in release_sql
    assert "validation_status = 'valid'" in release_sql
    assert "selection_owner = NULL" in release_sql



class _BarrierResult:
    def __init__(self, row: dict[str, int] | None) -> None:
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


class _BarrierConnection:
    def __init__(self, row: dict[str, int] | None) -> None:
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, statement, params=None):
        assert params == {"pipeline_progress_id": 42}
        return _BarrierResult(self.row)


class _BarrierEngine:
    def __init__(self, row: dict[str, int] | None) -> None:
        self.row = row

    def connect(self):
        return _BarrierConnection(self.row)


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            {"expected_count": 3, "actual_count": 3, "terminal_count": 3},
            True,
        ),
        (
            {"expected_count": 3, "actual_count": 3, "terminal_count": 2},
            False,
        ),
        (
            {"expected_count": 3, "actual_count": 2, "terminal_count": 2},
            False,
        ),
    ],
)
def test_extract_attempt_barrier_requires_every_expected_terminal_partition(
    row: dict[str, int],
    expected: bool,
) -> None:
    engine = _BarrierEngine(row)

    assert are_extract_attempts_terminal(
        cast(Engine, cast(object, engine)),
        42,
    ) is expected


def test_extract_attempt_barrier_rejects_unknown_progress() -> None:
    engine = _BarrierEngine(None)

    with pytest.raises(ValueError, match="unknown pipeline progress"):
        are_extract_attempts_terminal(
            cast(Engine, cast(object, engine)),
            42,
        )
