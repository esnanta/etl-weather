"""Reconcile validated manual whole-month loads into campaign progress."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from orchestration.campaign.policy import matching_campaign_progress_definition
from orchestration.repositories.database import runtime_database_engine
from orchestration.progress.tracking import artifact_identity
from orchestration.repositories.pipeline_progress import (
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    release_pipeline_progress_claim,
    update_pipeline_progress_detail,
)
from pipeline.storage import raw_artifact_for_request

# Preserve the historical progress-task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.progress")


def reconcile_campaign_repair(
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
    from pipeline.requests.weather import weather_request_from_spec

    definition = matching_campaign_progress_definition(request_spec)
    if definition is None:
        logger.info(
            "Loaded partition does not match a whole campaign month; request_key=%s",
            request_key,
        )
        return
    with runtime_database_engine() as engine:
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
        raw_path, raw_sha256, raw_size = artifact_identity(
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
