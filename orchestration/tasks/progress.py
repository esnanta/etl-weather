"""Compatibility facade for responsibility-specific progress-aware task modules.

Runtime DAG and operator code import the named task modules directly. These
re-exports preserve older imports without retaining mixed task responsibilities.
"""

from orchestration.campaign_repair import reconcile_campaign_repair
from orchestration.progress_tracking import (
    artifact_identity,
    record_failure,
    record_progress,
    require_request_key,
)
from orchestration.tasks.build_monthly_requests import prepare_monthly_jobs
from orchestration.tasks.extract import ensure_raw_checkpoint_with_progress
from orchestration.tasks.load import load_with_progress
from orchestration.tasks.transform import ensure_transformed_checkpoint_with_progress

__all__ = [
    "artifact_identity",
    "ensure_raw_checkpoint_with_progress",
    "load_with_progress",
    "prepare_monthly_jobs",
    "reconcile_campaign_repair",
    "record_failure",
    "record_progress",
    "require_request_key",
    "ensure_transformed_checkpoint_with_progress",
]
