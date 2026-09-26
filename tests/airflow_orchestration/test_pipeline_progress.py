from typing import cast

import pytest
from sqlalchemy.engine import Engine

from orchestration.progress import (
    PipelineProgressDefinition,
    ProgressPartition,
)
from orchestration.repositories.pipeline_progress import (
    create_pipeline_progress_tables,
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    update_pipeline_progress_detail,
)


class _ScalarResult:
    rowcount = 1

    def scalar_one(self) -> int:
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
    assert "CREATE SCHEMA IF NOT EXISTS climate" in sql
    assert "CREATE TABLE IF NOT EXISTS climate.pipeline_progress" in sql
    assert "CREATE TABLE IF NOT EXISTS climate.pipeline_progress_detail" in sql
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
    assert "INSERT INTO climate.pipeline_progress" in sql
    assert "configuration_identity" in sql
    assert "ON CONFLICT (" in sql
    assert "INSERT INTO climate.pipeline_progress_detail" in sql
    detail_params = [
        params
        for params in engine.connection.params
        if params and "request_key" in params
    ]
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
    assert "COUNT(*) FILTER" in sql
    assert "extract_status = 'complete'" in sql
    assert "transform_status = 'complete'" in sql
    assert "load_status = 'complete'" in sql
    assert "validation_status = 'valid'" in sql
    assert "detail_state.actual_count = progress.expected_partition_count" in sql
    assert "detail_state.completed_count = progress.expected_partition_count" in sql
    assert "detail_state.failed_count = 0" in sql
    assert "THEN 'complete'" in sql
    assert "THEN 'failed'" in sql
    assert "completed_partition_count = detail_state.completed_count" in sql
    assert "failed_partition_count = detail_state.failed_count" in sql
