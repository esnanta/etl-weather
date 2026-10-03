"""Run one load boundary, verify persistence, and reconcile campaign repair."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from orchestration.campaign_repair import reconcile_campaign_repair
from orchestration.database import runtime_database_engine
from orchestration.monitoring import measure_peak_memory, warn_low_disk_space
from orchestration.progress_tracking import (
    artifact_identity,
    record_failure,
    record_progress,
    require_request_key,
)
from orchestration.repositories.load_evidence import persisted_row_count
from orchestration.tasks.transform import _request_spec_from_result

# Preserve the historical ERA5-Land task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.era5_land")


def load_partition(
    transform_result: Mapping[str, Any],
) -> dict[str, object]:
    """Delegate one mapped load task using transform control metadata."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import load_request_partition

    request_spec = _request_spec_from_result(
        transform_result, "transform_or_resume"
    )
    request = weather_request_from_spec(request_spec)
    logger.info(
        "Starting database load for request_key=%s",
        transform_result["request_key"],
    )
    with measure_peak_memory("load"):
        warn_low_disk_space("load")
        return load_request_partition(request)


def load_with_progress(
    transform_result: dict[str, object],
) -> dict[str, object]:
    """Run load, verify persisted evidence, and reconcile manual repair."""
    progress_id = int(transform_result["progress_id"])
    request_key = str(transform_result["request_key"])
    record_progress(
        progress_id,
        request_key,
        load_status="running",
        validation_status="pending",
    )
    try:
        result = load_partition(transform_result)
        require_request_key(result, request_key)
        path, checksum, size = artifact_identity(result)
        with runtime_database_engine() as engine:
            row_count = persisted_row_count(
                engine, result, path, checksum
            )
        record_progress(
            progress_id,
            request_key,
            load_status="complete",
            validation_status="valid",
            transformed_artifact_path=path,
            transformed_sha256=checksum,
            transformed_file_size_bytes=size,
            row_count=row_count,
        )
        request_spec = transform_result["request"]
        if not isinstance(request_spec, Mapping):
            raise ValueError(
                "Load control metadata is missing a request specification."
            )
        reconcile_campaign_repair(
            progress_id,
            request_spec,
            request_key,
            path,
            checksum,
            size,
            row_count,
            manual_repair=transform_result["run_mode"] == "manual",
        )
    except Exception as error:
        record_failure(progress_id, request_key, "load", error)
        raise
    return {
        **result,
        "progress_id": progress_id,
        "run_mode": transform_result["run_mode"],
    }
