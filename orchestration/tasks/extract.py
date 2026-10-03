"""Run one extraction boundary and coordinate its durable progress state."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from orchestration.monitoring import measure_peak_memory, warn_low_disk_space
from orchestration.progress_tracking import (
    artifact_identity,
    record_failure,
    record_progress,
    require_request_key,
)

# Preserve the historical ERA5-Land task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.era5_land")


def extract_partition(
    request_spec: Mapping[str, Any],
    *,
    paced: bool = False,
) -> dict[str, object]:
    """Delegate one mapped extraction task to the pipeline runtime."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import extract_request_partition

    request = weather_request_from_spec(request_spec)
    logger.info(
        "Starting extraction for %s through %s",
        request.start_date,
        request.end_date,
    )
    with measure_peak_memory("extract_or_resume"):
        warn_low_disk_space("extract_or_resume")
        if paced:
            from orchestration.pacing import reserve_or_defer

            return extract_request_partition(
                request, before_submission=reserve_or_defer
            )
        return extract_request_partition(request)


def extract_with_progress(
    request_spec: dict[str, object],
    progress_id: int,
    run_mode: str,
    *,
    paced: bool = False,
) -> dict[str, object]:
    """Run extraction while preserving pacing deferral and failure semantics."""
    from pipeline.extraction.cds import CdsSubmissionDeferred
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
            extract_partition(request_spec, paced=True)
            if paced
            else extract_partition(request_spec)
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
    except CdsSubmissionDeferred:
        logger.info(
            "Extraction waiting for the next CDS submission slot; request_key=%s",
            request_key,
        )
        raise
    except Exception as error:
        record_failure(progress_id, request_key, "extract", error)
        raise
    return {**result, "progress_id": progress_id, "run_mode": run_mode}
