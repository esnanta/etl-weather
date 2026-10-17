"""Verify pipeline-progress SQL construction and input validation.

These unit tests exercise the production progress repository with real
PipelineProgressDefinition/ProgressPartition models and a minimal recording
engine. They assert DDL, seeded identities, update parameters, and reconciliation
SQL construction only. PostgreSQL state transitions, atomic claims, release
semantics, persisted-evidence reconciliation, and extraction-attempt barriers
are authoritative in orchestration integration tests.
"""

from typing import cast

import pytest
from sqlalchemy.engine import Engine

from database.config import (
    PIPELINE_PROGRESS_DETAIL_TABLE,
    PIPELINE_PROGRESS_SCHEMA,
    PIPELINE_PROGRESS_TABLE,
)
from orchestration.progress.definition import (
    PipelineProgressDefinition,
    ProgressPartition,
)
from database.schema import create_pipeline_progress_tables
from orchestration.repositories.pipeline_progress import (
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    update_pipeline_progress_detail,
)


class _ScalarResult:
    rowcount = 1

    @staticmethod
    def scalar_one() -> int:
        return 42


class _Result:
    rowcount = 1


class _Connection:
    """Capture generated SQL and parameters without emulating PostgreSQL state."""

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


def test_initialize_progress_builds_parent_and_expected_detail_inserts() -> None:
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

    detail_params = [
        params
        for params in engine.connection.params
        if isinstance(params, dict) and "request_key" in params
    ]
    assert [params["request_key"] for params in detail_params] == [
        "jan-key",
        "feb-key",
        "mar-key",
    ]


def test_detail_update_builds_status_evidence_and_metrics_parameters() -> None:
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


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"load_status": "successful"}, "Unsupported load_status"),
        ({"validation_status": "complete"}, "Unsupported validation_status"),
    ],
)
def test_detail_update_rejects_invalid_status(
    kwargs: dict[str, str],
    message: str,
) -> None:
    engine = _Engine()

    with pytest.raises(ValueError, match=message):
        update_pipeline_progress_detail(
            cast(Engine, cast(object, engine)),
            42,
            "jan-key",
            **kwargs,
        )


def test_reconciliation_sql_requires_complete_matching_persisted_evidence() -> None:
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
    assert "COALESCE(progress.completed_at, CURRENT_TIMESTAMP)" in sql
