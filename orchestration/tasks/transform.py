"""Ensure transformed checkpoints and coordinate durable progress state."""

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


def _request_spec_from_result(
    result: Mapping[str, Any],
    expected_boundary: str,
) -> Mapping[str, Any]:
    if result.get("boundary") != expected_boundary:
        raise ValueError(
            f"Expected {expected_boundary} control metadata before downstream task."
        )
    request_spec = result.get("request")
    if not isinstance(request_spec, Mapping):
        raise ValueError(
            "Upstream control metadata is missing a request specification."
        )
    return request_spec


def ensure_transformed_checkpoint_partition(
    extract_result: Mapping[str, Any],
) -> dict[str, object]:
    """Ensure one mapped transformed checkpoint from RAW control metadata."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import ensure_transformed_checkpoint_partition as ensure_transformed_checkpoint_runtime

    request_spec = _request_spec_from_result(
        extract_result, "ensure_raw_checkpoint"
    )
    request = weather_request_from_spec(request_spec)
    logger.info(
        "Ensuring transformed checkpoint for request_key=%s",
        extract_result["request_key"],
    )
    with measure_peak_memory("ensure_transformed_checkpoint"):
        warn_low_disk_space("ensure_transformed_checkpoint")
        return ensure_transformed_checkpoint_runtime(request)


def ensure_transformed_checkpoint_with_progress(
    extract_result: dict[str, object],
) -> dict[str, object]:
    """Ensure a transformed checkpoint while recording durable progress evidence."""
    progress_id = int(extract_result["progress_id"])
    request_key = str(extract_result["request_key"])
    record_progress(
        progress_id,
        request_key,
        transform_status="running",
        load_status="pending",
        validation_status="pending",
    )
    try:
        result = ensure_transformed_checkpoint_partition(extract_result)
        require_request_key(result, request_key)
        path, checksum, size = artifact_identity(result)
        record_progress(
            progress_id,
            request_key,
            transform_status="complete",
            transformed_artifact_path=path,
            transformed_sha256=checksum,
            transformed_file_size_bytes=size,
        )
    except Exception as error:
        record_failure(progress_id, request_key, "transform", error)
        raise
    return {
        **result,
        "progress_id": progress_id,
        "run_mode": extract_result["run_mode"],
    }
