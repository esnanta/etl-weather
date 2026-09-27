"""Persist mapped ETL evidence and protect the bounded campaign on retries.

Pacing deferrals leave a partition runnable while genuine extraction failures
remain recorded for deliberate manual repair.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from orchestration.campaign import (
    CampaignBlocked,
    matching_campaign_progress_definition,
    select_earliest_incomplete_quarter,
)
from orchestration.mode import resolve_run_mode
from orchestration.progress import build_pipeline_progress_definition
from orchestration.repositories.pipeline_progress import (
    create_pipeline_progress_tables,
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    release_pipeline_progress_claim,
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
from pipeline.storage import (
    artifact_path_for_metadata,
    calculate_file_sha256,
    raw_artifact_for_request,
)

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
    run_id: str | None = None,
    *,
    slot_anchor: datetime | None = None,
) -> list[dict[str, object]]:
    """Resolve one run and initialize its exact progress identity."""
    mode = resolve_run_mode(run_type, conf)
    with _progress_engine() as engine:
        create_schema(engine)
        create_daily_climate_table(engine)
        create_daily_climate_detail_table(engine)
        create_pipeline_progress_tables(engine)
        if mode.mode == "scheduled":
            if not run_id:
                raise ValueError("Scheduled DagRuns require a non-empty run_id.")
            selected = select_earliest_incomplete_quarter(
                engine, claim_owner=run_id
            )
            if selected is None:
                logger.info("No eligible incomplete campaign quarter remains.")
                return []
            if isinstance(selected, CampaignBlocked):
                logger.warning(
                    "Scheduled campaign selection blocked; quarter=%s; reason=%s",
                    selected.quarter.label,
                    selected.reason,
                )
                return []
            progress_id = selected.progress_id
            specs = list(selected.request_specs)
            logger.info(
                "Selected scheduled campaign quarter %s; mode=scheduled; progress_id=%d; months=%s",
                selected.quarter.label,
                progress_id,
                ", ".join(str(spec["start_date"])[:7] for spec in specs),
            )
        else:
            specs = build_historical_request_specs(mode.start_date, mode.end_date)
            progress_id = initialize_pipeline_progress(
                engine, build_pipeline_progress_definition(specs)
            )
            logger.info(
                "Selected manual range %s through %s; mode=manual; progress_id=%d; months=%s",
                mode.start_date,
                mode.end_date,
                progress_id,
                ", ".join(str(spec["start_date"])[:7] for spec in specs),
            )
        anchor = slot_anchor or datetime.now(timezone.utc)
        if anchor.tzinfo is None:
            raise ValueError("Scheduled slot anchor must be timezone-aware.")
        return [
            {
                "request_spec": spec, "progress_id": progress_id,
                "run_mode": mode.mode,
                "earliest_start_at": (
                    (anchor + timedelta(hours=index)).isoformat()
                    if mode.mode == "scheduled" else None
                ),
            }
            for index, spec in enumerate(specs)
        ]


def _record(progress_id: int, request_key: str, **fields: Any) -> None:
    with _progress_engine() as engine:
        update_pipeline_progress_detail(engine, progress_id, request_key, **fields)
        reconcile_pipeline_progress(engine, progress_id)
        release_pipeline_progress_claim(
            engine, progress_id, require_complete=True
        )


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
    run_mode: str,
    *,
    paced: bool = False,
) -> dict[str, object]:
    from pipeline.extraction.cds import CdsSubmissionDeferred
    from pipeline.request import build_request_key, weather_request_from_spec

    request_key = build_request_key(weather_request_from_spec(request_spec))
    _record(
        progress_id, request_key, extract_status="running",
        transform_status="pending", load_status="pending", validation_status="pending",
    )
    try:
        result = extract_partition(request_spec, paced=True) if paced else extract_partition(request_spec)
        _require_request_key(result, request_key)
        path, checksum, size = _identity(result)
        _record(
            progress_id, request_key, extract_status="complete",
            raw_artifact_path=path, raw_sha256=checksum, raw_file_size_bytes=size,
        )
    except CdsSubmissionDeferred:
        logger.info("Extraction waiting for the next CDS submission slot; request_key=%s", request_key)
        raise
    except Exception as error:
        _record_failure(progress_id, request_key, "extract", error)
        raise
    return {**result, "progress_id": progress_id, "run_mode": run_mode}


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
    return {
        **result,
        "progress_id": progress_id,
        "run_mode": extract_result["run_mode"],
    }


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


def _reconcile_campaign_repair(
    progress_id: int,
    request_spec: Mapping[str, Any],
    request_key: str,
    transformed_path: str,
    transformed_sha256: str,
    transformed_size: int,
    row_count: int,
    *,
    manual_repair: bool,
) -> None:
    """Apply a validated whole-month load to its exact campaign quarter."""
    from pipeline.request import weather_request_from_spec

    definition = matching_campaign_progress_definition(request_spec)
    if definition is None:
        logger.info(
            "Loaded partition does not match a whole campaign month; request_key=%s",
            request_key,
        )
        return
    with _progress_engine() as engine:
        campaign_id = initialize_pipeline_progress(engine, definition)
        if campaign_id == progress_id:
            if manual_repair:
                release_pipeline_progress_claim(
                    engine, campaign_id, require_complete=False
                )
            return
        raw_artifact = raw_artifact_for_request(
            weather_request_from_spec(request_spec)
        )
        raw_path, raw_sha256, raw_size = _identity(
            {"artifact_path": raw_artifact.path.as_posix()}
        )
        update_pipeline_progress_detail(
            engine,
            campaign_id,
            request_key,
            extract_status="complete",
            transform_status="complete",
            load_status="complete",
            validation_status="valid",
            raw_artifact_path=raw_path,
            raw_sha256=raw_sha256,
            raw_file_size_bytes=raw_size,
            transformed_artifact_path=transformed_path,
            transformed_sha256=transformed_sha256,
            transformed_file_size_bytes=transformed_size,
            row_count=row_count,
        )
        reconcile_pipeline_progress(engine, campaign_id)
        if manual_repair:
            release_pipeline_progress_claim(
                engine, campaign_id, require_complete=False
            )
    logger.info(
        "Reconciled campaign repair; progress_id=%d; request_key=%s",
        campaign_id,
        request_key,
    )


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
        request_spec = transform_result["request"]
        if not isinstance(request_spec, Mapping):
            raise ValueError("Load control metadata is missing a request specification.")
        _reconcile_campaign_repair(
            progress_id, request_spec, request_key, path, checksum, size, row_count,
            manual_repair=transform_result["run_mode"] == "manual",
        )
    except Exception as error:
        _record_failure(progress_id, request_key, "load", error)
        raise
    return {
        **result,
        "progress_id": progress_id,
        "run_mode": transform_result["run_mode"],
    }

