"""Record mapped ETL progress and artifact identity in durable storage."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from orchestration.database import runtime_database_engine
from orchestration.repositories.pipeline_progress import (
    reconcile_pipeline_progress,
    release_pipeline_progress_claim,
    update_pipeline_progress_detail,
)
from pipeline.storage import artifact_path_for_metadata, calculate_file_sha256

# Preserve the historical progress-task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.progress")


def record_progress(progress_id: int, request_key: str, **fields: Any) -> None:
    """Persist one boundary update, reconcile its parent, and release if complete."""
    with runtime_database_engine() as engine:
        update_pipeline_progress_detail(engine, progress_id, request_key, **fields)
        reconcile_pipeline_progress(engine, progress_id)
        release_pipeline_progress_claim(
            engine, progress_id, require_complete=True
        )


def record_failure(
    progress_id: int,
    request_key: str,
    boundary: str,
    error: Exception,
) -> None:
    """Best-effort persistence of a failed ETL boundary."""
    try:
        record_progress(
            progress_id,
            request_key,
            **{f"{boundary}_status": "failed"},
            validation_status="failed",
            last_error=type(error).__name__,
        )
    except Exception:
        logger.exception("Unable to record failed %s boundary.", boundary)


def artifact_identity(result: Mapping[str, Any]) -> tuple[str, str, int]:
    """Return metadata path, checksum, and file size for one artifact result."""
    path = Path(str(result["artifact_path"]))
    return (
        artifact_path_for_metadata(path),
        calculate_file_sha256(path),
        path.stat().st_size,
    )


def require_request_key(result: Mapping[str, Any], expected: str) -> None:
    """Reject boundary metadata that does not belong to the expected request."""
    if result.get("request_key") != expected:
        raise ValueError("Boundary result request key does not match progress detail.")
