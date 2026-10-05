"""Ensure RAW checkpoints and coordinate their durable progress state."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from orchestration.monitoring.resources import measure_peak_memory, warn_low_disk_space
from orchestration.progress.tracking import (
    artifact_identity,
    record_failure,
    record_progress,
    require_request_key,
)

# Preserve the historical ERA5-Land task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.era5_land")


def ensure_raw_checkpoint_partition(
    request_spec: Mapping[str, Any],
    *,
    paced: bool = False,
) -> dict[str, object]:
    """Ensure one mapped RAW checkpoint through the pipeline runtime."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import ensure_raw_checkpoint_partition as ensure_raw_checkpoint_runtime

    request = weather_request_from_spec(request_spec)
    logger.info(
        "Ensuring RAW checkpoint for %s through %s",
        request.start_date,
        request.end_date,
    )
    with measure_peak_memory("ensure_raw_checkpoint"):
        warn_low_disk_space("ensure_raw_checkpoint")
        if paced:
            from orchestration.scheduling.submission_gate import reserve_or_defer

            return ensure_raw_checkpoint_runtime(
                request, before_submission=reserve_or_defer
            )
        return ensure_raw_checkpoint_runtime(request)


def ensure_raw_checkpoint_with_progress(
    request_spec: dict[str, object],
    progress_id: int,
    run_mode: str,
    *,
    paced: bool = False,
) -> dict[str, object]:
    """Ensure a RAW checkpoint while preserving pacing deferral and failure semantics."""
    from orchestration.scheduling.submission_gate import SubmissionDeferred
    from pipeline.request import build_request_key, weather_request_from_spec

    request_key = build_request_key(weather_request_from_spec(request_spec))
    record_progress(
        progress_id,
        request_key,
        extract_status="running",
        transform_status="pending",
        load_status="pending",
        validation_status="pending",
    )
    try:
        result = (
            ensure_raw_checkpoint_partition(request_spec, paced=True)
            if paced
            else ensure_raw_checkpoint_partition(request_spec)
        )
        require_request_key(result, request_key)
        path, checksum, size = artifact_identity(result)
        record_progress(
            progress_id,
            request_key,
            extract_status="complete",
            raw_artifact_path=path,
            raw_sha256=checksum,
            raw_file_size_bytes=size,
        )
    except SubmissionDeferred:
        logger.info(
            "RAW checkpoint waiting for the next CDS submission slot; request_key=%s",
            request_key,
        )
        raise
    except Exception as error:
        record_failure(progress_id, request_key, "extract", error)
        raise
    return {**result, "progress_id": progress_id, "run_mode": run_mode}
