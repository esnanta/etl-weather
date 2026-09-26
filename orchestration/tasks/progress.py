from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from orchestration.campaign import select_earliest_incomplete_quarter
from orchestration.mode import resolve_run_mode
from orchestration.progress import build_pipeline_progress_definition
from orchestration.repositories.pipeline_progress import (
    create_pipeline_progress_tables,
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    update_pipeline_progress_detail,
)
from orchestration.tasks.era5_land import (
    build_historical_request_specs,
    extract_partition,
    load_partition,
    transform_partition,
)
from pipeline.database import (
    DatabaseConfig,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)
from pipeline.storage import artifact_path_for_metadata, calculate_file_sha256

logger = logging.getLogger(__name__)


@contextmanager
def _progress_engine() -> Iterator[Engine]:
    engine = create_engine(DatabaseConfig.from_env().url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def prepare_monthly_jobs(
    run_type: str,
    conf: Mapping[str, Any] | None,
) -> list[dict[str, object]]:
    """Resolve one run and initialize its exact progress identity."""
    mode = resolve_run_mode(run_type, conf)
    with _progress_engine() as engine:
        create_schema(engine)
        create_daily_climate_table(engine)
        create_daily_climate_detail_table(engine)
        create_pipeline_progress_tables(engine)
        if mode.mode == "scheduled":
            selected = select_earliest_incomplete_quarter(engine)
            if selected is None:
                logger.info("No eligible incomplete campaign quarter remains.")
                return []
            progress_id = selected.progress_id
            specs = list(selected.request_specs)
            logger.info(
                "Selected scheduled campaign quarter %s; progress_id=%d",
                selected.quarter.label,
                progress_id,
            )
        else:
            specs = build_historical_request_specs(mode.start_date, mode.end_date)
            progress_id = initialize_pipeline_progress(
                engine, build_pipeline_progress_definition(specs)
            )
            logger.info(
                "Selected manual range %s through %s; progress_id=%d",
                mode.start_date,
                mode.end_date,
                progress_id,
            )
        return [
            {"request_spec": spec, "progress_id": progress_id}
            for spec in specs
        ]


def _record(progress_id: int, request_key: str, **fields: Any) -> None:
    with _progress_engine() as engine:
        update_pipeline_progress_detail(engine, progress_id, request_key, **fields)
        reconcile_pipeline_progress(engine, progress_id)


def _record_failure(progress_id: int, request_key: str, boundary: str, error: Exception) -> None:
    try:
        _record(
            progress_id,
            request_key,
            **{f"{boundary}_status": "failed"},
            validation_status="failed",
            last_error=type(error).__name__,
        )
    except Exception:
        logger.exception("Unable to record failed %s boundary.", boundary)


def _identity(result: Mapping[str, Any]) -> tuple[str, str, int]:
    path = Path(str(result["artifact_path"]))
    return (
        artifact_path_for_metadata(path),
        calculate_file_sha256(path),
        path.stat().st_size,
    )


def _require_request_key(result: Mapping[str, Any], expected: str) -> None:
    if result.get("request_key") != expected:
        raise ValueError("Boundary result request key does not match progress detail.")


def extract_with_progress(
    request_spec: dict[str, object],
    progress_id: int,
) -> dict[str, object]:
    from pipeline.request import build_request_key, weather_request_from_spec

    request_key = build_request_key(weather_request_from_spec(request_spec))
    _record(
        progress_id, request_key, extract_status="running",
        transform_status="pending", load_status="pending", validation_status="pending",
    )
    try:
        result = extract_partition(request_spec)
        _require_request_key(result, request_key)
        path, checksum, size = _identity(result)
        _record(
            progress_id, request_key, extract_status="complete",
            raw_artifact_path=path, raw_sha256=checksum, raw_file_size_bytes=size,
        )
    except Exception as error:
        _record_failure(progress_id, request_key, "extract", error)
        raise
    return {**result, "progress_id": progress_id}


def transform_with_progress(extract_result: dict[str, object]) -> dict[str, object]:
    progress_id = int(extract_result["progress_id"])
    request_key = str(extract_result["request_key"])
    _record(
        progress_id, request_key, transform_status="running",
        load_status="pending", validation_status="pending",
    )
    try:
        result = transform_partition(extract_result)
        _require_request_key(result, request_key)
        path, checksum, size = _identity(result)
        _record(
            progress_id, request_key, transform_status="complete",
            transformed_artifact_path=path, transformed_sha256=checksum,
            transformed_file_size_bytes=size,
        )
    except Exception as error:
        _record_failure(progress_id, request_key, "transform", error)
        raise
    return {**result, "progress_id": progress_id}


def _persisted_row_count(
    engine: Engine,
    result: Mapping[str, Any],
    path: str,
    checksum: str,
) -> int:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT metadata.request_key, metadata.transformed_artifact_path,
                       metadata.transformed_sha256, COUNT(observation.id) AS row_count
                FROM climate_data.daily_climate AS metadata
                LEFT JOIN climate_data.daily_climate_detail AS observation
                  ON observation.daily_climate_id = metadata.id
                WHERE metadata.id = :daily_climate_id
                GROUP BY metadata.id
                """
            ),
            {"daily_climate_id": result["daily_climate_id"]},
        ).mappings().one_or_none()
    if (
        row is None
        or row["request_key"] != result["request_key"]
        or row["transformed_artifact_path"] != path
        or row["transformed_sha256"] != checksum
        or int(row["row_count"]) <= 0
    ):
        raise ValueError("Persisted partition evidence does not match the artifact.")
    return int(row["row_count"])


def load_with_progress(transform_result: dict[str, object]) -> dict[str, object]:
    progress_id = int(transform_result["progress_id"])
    request_key = str(transform_result["request_key"])
    _record(
        progress_id, request_key, load_status="running", validation_status="pending"
    )
    try:
        result = load_partition(transform_result)
        _require_request_key(result, request_key)
        path, checksum, size = _identity(result)
        with _progress_engine() as engine:
            row_count = _persisted_row_count(engine, result, path, checksum)
        _record(
            progress_id, request_key, load_status="complete", validation_status="valid",
            transformed_artifact_path=path, transformed_sha256=checksum,
            transformed_file_size_bytes=size, row_count=row_count,
        )
    except Exception as error:
        _record_failure(progress_id, request_key, "load", error)
        raise
    return {**result, "progress_id": progress_id}
