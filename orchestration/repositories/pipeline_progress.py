from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from orchestration.config import (
    PIPELINE_PROGRESS_DETAIL_TABLE,
    PIPELINE_PROGRESS_SCHEMA,
    PIPELINE_PROGRESS_TABLE,
)
from orchestration.progress import PipelineProgressDefinition
from pipeline.database import DAILY_CLIMATE_DETAIL_TABLE, DAILY_CLIMATE_TABLE, SCHEMA_NAME
_ALLOWED_EXECUTION_STATUSES = ("pending", "running", "failed", "complete")
_ALLOWED_BOUNDARY_STATUSES = ("pending", "running", "failed", "complete")
_ALLOWED_VALIDATION_STATUSES = ("pending", "failed", "valid")


@dataclass(frozen=True)
class PipelineProgress:
    id: int
    pipeline_name: str
    source: str
    dataset_name: str
    start_date: date
    end_date: date
    configuration_identity: str
    expected_partition_count: int
    completed_partition_count: int
    failed_partition_count: int
    execution_status: str
    validation_status: str
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.execution_status not in _ALLOWED_EXECUTION_STATUSES:
            raise ValueError(f"Unsupported execution status: {self.execution_status}")
        if self.validation_status not in _ALLOWED_VALIDATION_STATUSES:
            raise ValueError(f"Unsupported validation status: {self.validation_status}")


def _parent_table() -> str:
    return f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE}"


def _detail_table() -> str:
    return f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}"


def create_pipeline_progress_tables(engine: Engine) -> None:
    """Create cadence-neutral operational progress tables in PostgreSQL."""
    parent = _parent_table()
    detail = _detail_table()
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {PIPELINE_PROGRESS_SCHEMA}"))
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {parent}
                (
                    id BIGSERIAL PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    configuration_identity TEXT NOT NULL,
                    expected_partition_count INTEGER NOT NULL,
                    completed_partition_count INTEGER NOT NULL DEFAULT 0,
                    failed_partition_count INTEGER NOT NULL DEFAULT 0,
                    execution_status TEXT NOT NULL DEFAULT 'pending',
                    validation_status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    selection_owner TEXT,
                    selection_claimed_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_{PIPELINE_PROGRESS_TABLE}_identity
                        UNIQUE (
                            pipeline_name,
                            source,
                            dataset_name,
                            start_date,
                            end_date,
                            configuration_identity
                        ),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_TABLE}_period
                        CHECK (start_date <= end_date),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_TABLE}_execution_status
                        CHECK (execution_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_TABLE}_validation_status
                        CHECK (validation_status IN ('pending', 'failed', 'valid'))
                )
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {detail}
                (
                    id BIGSERIAL PRIMARY KEY,
                    pipeline_progress_id BIGINT NOT NULL,
                    request_key TEXT NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    raw_artifact_path TEXT,
                    raw_sha256 TEXT,
                    raw_file_size_bytes BIGINT,
                    transformed_artifact_path TEXT,
                    transformed_sha256 TEXT,
                    transformed_file_size_bytes BIGINT,
                    temporal_start TIMESTAMP,
                    temporal_end TIMESTAMP,
                    temporal_count INTEGER,
                    latitude_min DOUBLE PRECISION,
                    latitude_max DOUBLE PRECISION,
                    latitude_count INTEGER,
                    longitude_min DOUBLE PRECISION,
                    longitude_max DOUBLE PRECISION,
                    longitude_count INTEGER,
                    grid_count INTEGER,
                    row_count INTEGER,
                    extract_status TEXT NOT NULL DEFAULT 'pending',
                    transform_status TEXT NOT NULL DEFAULT 'pending',
                    load_status TEXT NOT NULL DEFAULT 'pending',
                    validation_status TEXT NOT NULL DEFAULT 'pending',
                    validation_message TEXT,
                    last_error TEXT,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    metrics JSONB,
                    CONSTRAINT fk_{PIPELINE_PROGRESS_DETAIL_TABLE}_parent
                        FOREIGN KEY (pipeline_progress_id)
                        REFERENCES {parent}(id)
                        ON DELETE CASCADE,
                    CONSTRAINT uq_{PIPELINE_PROGRESS_DETAIL_TABLE}_request
                        UNIQUE (pipeline_progress_id, request_key),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_period
                        CHECK (start_date <= end_date),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_extract_status
                        CHECK (extract_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_transform_status
                        CHECK (transform_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_load_status
                        CHECK (load_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_validation_status
                        CHECK (validation_status IN ('pending', 'failed', 'valid'))
                )
                """
            )
        )
        connection.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS ix_{PIPELINE_PROGRESS_TABLE}_selection "
                f"ON {parent} (pipeline_name, source, dataset_name, start_date, execution_status)"
            )
        )


def initialize_pipeline_progress(
    engine: Engine,
    definition: PipelineProgressDefinition,
) -> int:
    """Create/reuse one logical batch and seed its expected partition rows."""
    parent = _parent_table()
    detail = _detail_table()
    parent_statement = text(
        f"""
        INSERT INTO {parent}
            (pipeline_name, source, dataset_name, start_date, end_date,
             configuration_identity, expected_partition_count)
        VALUES
            (:pipeline_name, :source, :dataset_name, :start_date, :end_date,
             :configuration_identity, :expected_partition_count)
        ON CONFLICT (
            pipeline_name, source, dataset_name, start_date, end_date,
            configuration_identity
        )
        DO UPDATE SET
            expected_partition_count = EXCLUDED.expected_partition_count,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
        """
    )
    detail_statement = text(
        f"""
        INSERT INTO {detail}
            (pipeline_progress_id, request_key, start_date, end_date)
        VALUES
            (:pipeline_progress_id, :request_key, :start_date, :end_date)
        ON CONFLICT (pipeline_progress_id, request_key)
        DO UPDATE SET
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as connection:
        progress_id = int(
            connection.execute(
                parent_statement,
                {
                    "pipeline_name": definition.pipeline_name,
                    "source": definition.source,
                    "dataset_name": definition.dataset_name,
                    "start_date": definition.start_date,
                    "end_date": definition.end_date,
                    "configuration_identity": definition.configuration_identity,
                    "expected_partition_count": definition.expected_partition_count,
                },
            ).scalar_one()
        )
        for partition in definition.partitions:
            connection.execute(
                detail_statement,
                {
                    "pipeline_progress_id": progress_id,
                    "request_key": partition.request_key,
                    "start_date": partition.start_date,
                    "end_date": partition.end_date,
                },
            )
    return progress_id


def update_pipeline_progress_detail(
    engine: Engine,
    progress_id: int,
    request_key: str,
    *,
    extract_status: str | None = None,
    transform_status: str | None = None,
    load_status: str | None = None,
    validation_status: str | None = None,
    validation_message: str | None = None,
    last_error: str | None = None,
    raw_artifact_path: str | None = None,
    raw_sha256: str | None = None,
    raw_file_size_bytes: int | None = None,
    transformed_artifact_path: str | None = None,
    transformed_sha256: str | None = None,
    transformed_file_size_bytes: int | None = None,
    temporal_start: object | None = None,
    temporal_end: object | None = None,
    temporal_count: int | None = None,
    latitude_min: float | None = None,
    latitude_max: float | None = None,
    latitude_count: int | None = None,
    longitude_min: float | None = None,
    longitude_max: float | None = None,
    longitude_count: int | None = None,
    grid_count: int | None = None,
    row_count: int | None = None,
    metrics: Mapping[str, Any] | None = None,
) -> None:
    """Update one partition without letting observability JSON replace core truth."""
    for status, allowed, name in (
        (extract_status, _ALLOWED_BOUNDARY_STATUSES, "extract_status"),
        (transform_status, _ALLOWED_BOUNDARY_STATUSES, "transform_status"),
        (load_status, _ALLOWED_BOUNDARY_STATUSES, "load_status"),
        (validation_status, _ALLOWED_VALIDATION_STATUSES, "validation_status"),
    ):
        if status is not None and status not in allowed:
            raise ValueError(f"Unsupported {name}: {status}")
    if progress_id <= 0:
        raise ValueError("progress_id must be positive.")
    if not request_key.strip():
        raise ValueError("request_key must not be empty.")

    detail = _detail_table()
    statement = text(
        f"""
        UPDATE {detail}
        SET extract_status = COALESCE(:extract_status, extract_status),
            transform_status = COALESCE(:transform_status, transform_status),
            load_status = COALESCE(:load_status, load_status),
            validation_status = COALESCE(:validation_status, validation_status),
            validation_message = COALESCE(:validation_message, validation_message),
            last_error = COALESCE(:last_error, last_error),
            raw_artifact_path = COALESCE(:raw_artifact_path, raw_artifact_path),
            raw_sha256 = COALESCE(:raw_sha256, raw_sha256),
            raw_file_size_bytes = COALESCE(:raw_file_size_bytes, raw_file_size_bytes),
            transformed_artifact_path = COALESCE(
                :transformed_artifact_path, transformed_artifact_path
            ),
            transformed_sha256 = COALESCE(:transformed_sha256, transformed_sha256),
            transformed_file_size_bytes = COALESCE(
                :transformed_file_size_bytes, transformed_file_size_bytes
            ),
            temporal_start = COALESCE(:temporal_start, temporal_start),
            temporal_end = COALESCE(:temporal_end, temporal_end),
            temporal_count = COALESCE(:temporal_count, temporal_count),
            latitude_min = COALESCE(:latitude_min, latitude_min),
            latitude_max = COALESCE(:latitude_max, latitude_max),
            latitude_count = COALESCE(:latitude_count, latitude_count),
            longitude_min = COALESCE(:longitude_min, longitude_min),
            longitude_max = COALESCE(:longitude_max, longitude_max),
            longitude_count = COALESCE(:longitude_count, longitude_count),
            grid_count = COALESCE(:grid_count, grid_count),
            row_count = COALESCE(:row_count, row_count),
            metrics = COALESCE(CAST(:metrics AS JSONB), metrics),
            started_at = CASE
                WHEN started_at IS NULL AND (
                    :extract_status = 'running' OR :transform_status = 'running'
                    OR :load_status = 'running'
                ) THEN CURRENT_TIMESTAMP
                ELSE started_at
            END,
            completed_at = CASE
                WHEN COALESCE(:extract_status, extract_status) = 'complete'
                 AND COALESCE(:transform_status, transform_status) = 'complete'
                 AND COALESCE(:load_status, load_status) = 'complete'
                 AND COALESCE(:validation_status, validation_status) = 'valid'
                    THEN CURRENT_TIMESTAMP
                WHEN COALESCE(:extract_status, extract_status) = 'failed'
                  OR COALESCE(:transform_status, transform_status) = 'failed'
                  OR COALESCE(:load_status, load_status) = 'failed'
                  OR COALESCE(:validation_status, validation_status) = 'failed'
                    THEN NULL
                ELSE completed_at
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_progress_id = :pipeline_progress_id
          AND request_key = :request_key
        """
    )
    params = {
        "pipeline_progress_id": progress_id,
        "request_key": request_key.strip(),
        "extract_status": extract_status,
        "transform_status": transform_status,
        "load_status": load_status,
        "validation_status": validation_status,
        "validation_message": validation_message,
        "last_error": last_error[:2000] if last_error is not None else None,
        "raw_artifact_path": raw_artifact_path,
        "raw_sha256": raw_sha256,
        "raw_file_size_bytes": raw_file_size_bytes,
        "transformed_artifact_path": transformed_artifact_path,
        "transformed_sha256": transformed_sha256,
        "transformed_file_size_bytes": transformed_file_size_bytes,
        "temporal_start": temporal_start,
        "temporal_end": temporal_end,
        "temporal_count": temporal_count,
        "latitude_min": latitude_min,
        "latitude_max": latitude_max,
        "latitude_count": latitude_count,
        "longitude_min": longitude_min,
        "longitude_max": longitude_max,
        "longitude_count": longitude_count,
        "grid_count": grid_count,
        "row_count": row_count,
        "metrics": json.dumps(dict(metrics), sort_keys=True) if metrics is not None else None,
    }
    with engine.begin() as connection:
        result = connection.execute(statement, params)
        if result.rowcount != 1:
            raise ValueError(
                "Cannot update pipeline progress detail: parent/request identity does not exist."
            )


def reconcile_pipeline_progress(engine: Engine, progress_id: int) -> None:
    """Derive parent status from detail state and matching persisted observations."""
    if progress_id <= 0:
        raise ValueError("progress_id must be positive.")
    parent = _parent_table()
    detail = _detail_table()
    statement = text(
        f"""
        WITH detail_state AS (
            SELECT
                COUNT(partition.id) AS actual_count,
                COUNT(partition.id) FILTER (
                    WHERE partition.extract_status = 'complete'
                      AND partition.transform_status = 'complete'
                      AND partition.load_status = 'complete'
                      AND partition.validation_status = 'valid'
                      AND partition.row_count > 0
                      AND persisted.id IS NOT NULL
                      AND persisted.transformed_artifact_path =
                          partition.transformed_artifact_path
                      AND persisted.transformed_sha256 =
                          partition.transformed_sha256
                      AND observations.row_count = partition.row_count
                ) AS completed_count,
                COUNT(partition.id) FILTER (
                    WHERE partition.extract_status = 'failed'
                       OR partition.transform_status = 'failed'
                       OR partition.load_status = 'failed'
                       OR partition.validation_status = 'failed'
                ) AS failed_count,
                COUNT(partition.id) FILTER (
                    WHERE partition.extract_status = 'running'
                       OR partition.transform_status = 'running'
                       OR partition.load_status = 'running'
                ) AS running_count,
                COUNT(partition.id) FILTER (
                    WHERE partition.extract_status <> 'pending'
                       OR partition.transform_status <> 'pending'
                       OR partition.load_status <> 'pending'
                ) AS attempted_count
            FROM {parent} AS batch
            LEFT JOIN {detail} AS partition
              ON partition.pipeline_progress_id = batch.id
            LEFT JOIN {SCHEMA_NAME}.{DAILY_CLIMATE_TABLE} AS persisted
              ON persisted.request_key = partition.request_key
             AND persisted.source::text = batch.source
             AND persisted.dataset_name = batch.dataset_name
             AND persisted.start_date = partition.start_date
             AND persisted.end_date = partition.end_date
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS row_count
                FROM {SCHEMA_NAME}.{DAILY_CLIMATE_DETAIL_TABLE} AS observation
                WHERE observation.daily_climate_id = persisted.id
            ) AS observations ON TRUE
            WHERE batch.id = :pipeline_progress_id
        )
        UPDATE {parent} AS progress
        SET completed_partition_count = detail_state.completed_count,
            failed_partition_count = detail_state.failed_count,
            execution_status = CASE
                WHEN detail_state.actual_count = progress.expected_partition_count
                 AND detail_state.completed_count = progress.expected_partition_count
                 AND detail_state.failed_count = 0
                    THEN 'complete'
                WHEN detail_state.failed_count > 0
                    THEN 'failed'
                WHEN detail_state.completed_count > 0
                  OR detail_state.running_count > 0
                    THEN 'running'
                ELSE 'pending'
            END,
            validation_status = CASE
                WHEN detail_state.actual_count = progress.expected_partition_count
                 AND detail_state.completed_count = progress.expected_partition_count
                 AND detail_state.failed_count = 0
                    THEN 'valid'
                WHEN detail_state.failed_count > 0
                    THEN 'failed'
                ELSE 'pending'
            END,
            last_error = CASE
                WHEN detail_state.failed_count = 0 THEN NULL
                ELSE (
                    SELECT failed_detail.last_error
                    FROM {detail} AS failed_detail
                    WHERE failed_detail.pipeline_progress_id = :pipeline_progress_id
                      AND failed_detail.last_error IS NOT NULL
                    ORDER BY failed_detail.updated_at DESC
                    LIMIT 1
                )
            END,
            started_at = CASE
                WHEN detail_state.attempted_count > 0
                    THEN COALESCE(progress.started_at, CURRENT_TIMESTAMP)
                ELSE progress.started_at
            END,
            completed_at = CASE
                WHEN detail_state.actual_count = progress.expected_partition_count
                 AND detail_state.completed_count = progress.expected_partition_count
                 AND detail_state.failed_count = 0
                    THEN COALESCE(progress.completed_at, CURRENT_TIMESTAMP)
                ELSE NULL
            END,
            updated_at = CURRENT_TIMESTAMP
        FROM detail_state
        WHERE progress.id = :pipeline_progress_id
        """
    )
    with engine.begin() as connection:
        result = connection.execute(statement, {"pipeline_progress_id": progress_id})
        if result.rowcount != 1:
            raise ValueError("Cannot reconcile unknown pipeline progress row.")


def get_earliest_incomplete_progress(
    engine: Engine,
    *,
    pipeline_name: str,
    source: str,
    dataset_name: str,
    campaign_start_date: str,
    campaign_end_date: str,
) -> PipelineProgress | None:
    """Query generic batches; campaign selection uses exact seeded IDs instead."""
    parent = _parent_table()
    statement = text(
        f"""
        SELECT id, pipeline_name, source, dataset_name, start_date, end_date,
               configuration_identity, expected_partition_count,
               completed_partition_count, failed_partition_count,
               execution_status, validation_status, completed_at
        FROM {parent}
        WHERE pipeline_name = :pipeline_name
          AND source = :source
          AND dataset_name = :dataset_name
          AND start_date >= :campaign_start_date
          AND end_date <= :campaign_end_date
          AND execution_status <> 'complete'
        ORDER BY start_date, end_date
        LIMIT 1
        """
    )
    with engine.connect() as connection:
        row = connection.execute(
            statement,
            {
                "pipeline_name": pipeline_name,
                "source": source,
                "dataset_name": dataset_name,
                "campaign_start_date": campaign_start_date,
                "campaign_end_date": campaign_end_date,
            },
        ).mappings().first()
    return _progress_from_row(row) if row is not None else None


def get_pipeline_progress_by_id(
    engine: Engine,
    progress_id: int,
) -> PipelineProgress:
    """Read the exact initialized batch after reconciling its evidence."""
    if progress_id <= 0:
        raise ValueError("progress_id must be positive.")
    with engine.connect() as connection:
        row = connection.execute(
            text(
                f"""
                SELECT id, pipeline_name, source, dataset_name, start_date, end_date,
                       configuration_identity, expected_partition_count,
                       completed_partition_count, failed_partition_count,
                       execution_status, validation_status, completed_at
                FROM {_parent_table()}
                WHERE id = :pipeline_progress_id
                """
            ),
            {"pipeline_progress_id": progress_id},
        ).mappings().one_or_none()
    if row is None:
        raise ValueError("Cannot read unknown pipeline progress row.")
    return _progress_from_row(row)


def try_claim_pipeline_progress(
    engine: Engine, progress_id: int, owner: str
) -> bool:
    """Atomically claim one unfinished batch for a single scheduled DagRun."""
    if progress_id <= 0 or not owner.strip():
        raise ValueError("A positive progress_id and non-empty owner are required.")
    with engine.begin() as connection:
        claimed = connection.execute(
            text(
                f"""
                UPDATE {_parent_table()}
                SET selection_owner = :owner,
                    selection_claimed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :pipeline_progress_id
                  AND execution_status <> 'complete'
                  AND failed_partition_count = 0
                  AND (selection_owner IS NULL OR selection_owner = :owner)
                RETURNING id
                """
            ),
            {"pipeline_progress_id": progress_id, "owner": owner.strip()},
        ).scalar_one_or_none()
    return claimed is not None


def release_pipeline_progress_claim(
    engine: Engine, progress_id: int, *, require_complete: bool
) -> bool:
    """Release a claim after completion or validated manual intervention."""
    if progress_id <= 0:
        raise ValueError("progress_id must be positive.")
    completion_condition = (
        "AND execution_status = 'complete' AND validation_status = 'valid'"
        if require_complete else "AND failed_partition_count = 0"
    )
    with engine.begin() as connection:
        result = connection.execute(
            text(
                f"""
                UPDATE {_parent_table()}
                SET selection_owner = NULL,
                    selection_claimed_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :pipeline_progress_id
                  AND selection_owner IS NOT NULL
                  {completion_condition}
                """
            ),
            {"pipeline_progress_id": progress_id},
        )
    return result.rowcount == 1


def are_extract_attempts_terminal(
    engine: Engine,
    progress_id: int,
) -> bool:
    """Return whether every expected partition finished its first extraction attempt."""
    if progress_id <= 0:
        raise ValueError("progress_id must be positive.")

    statement = text(
        f"""
        SELECT
            progress.expected_partition_count AS expected_count,
            COUNT(detail.id) AS actual_count,
            COUNT(detail.id) FILTER (
                WHERE detail.extract_status IN ('complete', 'failed')
            ) AS terminal_count
        FROM {_parent_table()} AS progress
        LEFT JOIN {_detail_table()} AS detail
          ON detail.pipeline_progress_id = progress.id
        WHERE progress.id = :pipeline_progress_id
        GROUP BY progress.expected_partition_count
        """
    )
    with engine.connect() as connection:
        row = connection.execute(
            statement,
            {"pipeline_progress_id": progress_id},
        ).mappings().one_or_none()

    if row is None:
        raise ValueError("Cannot inspect unknown pipeline progress row.")

    expected_count = int(row["expected_count"])
    actual_count = int(row["actual_count"])
    terminal_count = int(row["terminal_count"])
    return (
        expected_count > 0
        and actual_count == expected_count
        and terminal_count == expected_count
    )


def get_failed_progress_partitions(
    engine: Engine, progress_id: int
) -> tuple[tuple[date, date, str | None], ...]:
    """Return bounded diagnostics for failed monthly details."""
    if progress_id <= 0:
        raise ValueError("progress_id must be positive.")
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                f"""
                SELECT start_date, end_date, last_error
                FROM {_detail_table()}
                WHERE pipeline_progress_id = :pipeline_progress_id
                  AND (
                      extract_status = 'failed'
                      OR transform_status = 'failed'
                      OR load_status = 'failed'
                      OR validation_status = 'failed'
                  )
                ORDER BY start_date
                """
            ),
            {"pipeline_progress_id": progress_id},
        ).mappings().all()
    return tuple(
        (row["start_date"], row["end_date"], row["last_error"])
        for row in rows
    )


def _progress_from_row(row: Mapping[str, Any]) -> PipelineProgress:
    return PipelineProgress(
        id=int(row["id"]),
        pipeline_name=str(row["pipeline_name"]),
        source=str(row["source"]),
        dataset_name=str(row["dataset_name"]),
        start_date=row["start_date"],
        end_date=row["end_date"],
        configuration_identity=str(row["configuration_identity"]),
        expected_partition_count=int(row["expected_partition_count"]),
        completed_partition_count=int(row["completed_partition_count"]),
        failed_partition_count=int(row["failed_partition_count"]),
        execution_status=str(row["execution_status"]),
        validation_status=str(row["validation_status"]),
        completed_at=row["completed_at"],
    )
