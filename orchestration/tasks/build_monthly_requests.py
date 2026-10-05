"""Prepare manual or scheduled monthly jobs without defining Airflow tasks."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from orchestration.campaign import CampaignBlocked, select_earliest_incomplete_quarter
from orchestration.config import MONTHLY_SLOT_SPACING_HOURS
from orchestration.database import initialize_progress_storage, runtime_database_engine
from orchestration.mode import resolve_run_mode
from orchestration.progress import build_pipeline_progress_definition
from orchestration.repositories.pipeline_progress import initialize_pipeline_progress
from orchestration.requests import build_historical_request_specs

# Preserve the historical progress-task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.progress")


def prepare_monthly_jobs(
    run_type: str,
    conf: Mapping[str, Any] | None,
    run_id: str | None = None,
    *,
    slot_anchor: datetime | None = None,
) -> list[dict[str, object]]:
    """Resolve one run and initialize its exact progress identity."""
    mode = resolve_run_mode(run_type, conf)
    with runtime_database_engine() as engine:
        initialize_progress_storage(engine)
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
                "Selected scheduled campaign quarter %s; mode=scheduled; "
                "progress_id=%d; months=%s",
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
                "Selected manual range %s through %s; mode=manual; "
                "progress_id=%d; months=%s",
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
                "request_spec": spec,
                "progress_id": progress_id,
                "run_mode": mode.mode,
                "earliest_start_at": (
                    (
                        anchor
                        + timedelta(hours=index * MONTHLY_SLOT_SPACING_HOURS)
                    ).isoformat()
                    if mode.mode == "scheduled"
                    else None
                ),
            }
            for index, spec in enumerate(specs)
        ]
