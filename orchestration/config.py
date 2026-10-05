"""Operational configuration owned by the Airflow orchestration layer."""

from __future__ import annotations

import os
from datetime import date
from zoneinfo import ZoneInfo

from config import (
    APPLICATION_SCHEMA,
    APPLICATION_TIMEZONE_NAME,
    resolve_campaign_period,
)


def _required_environment(name: str) -> str:
    """Return one required non-blank orchestration environment value."""
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"{name} is required. Set it in the project .env before starting Airflow."
        )
    return value.strip()


def _validate_campaign_bounds(start_date: date, end_date: date) -> None:
    """Validate a scheduled campaign spanning one or more full calendar years."""
    if start_date > end_date:
        raise RuntimeError(
            "CAMPAIGN_START_DATE must be less than or equal to CAMPAIGN_END_DATE."
        )
    if (start_date.month, start_date.day) != (1, 1):
        raise RuntimeError(
            "CAMPAIGN_START_DATE must be January 1 for a scheduled campaign."
        )
    if (end_date.month, end_date.day) != (12, 31):
        raise RuntimeError(
            "CAMPAIGN_END_DATE must be December 31 for a scheduled campaign."
        )
    if end_date.year < start_date.year:
        raise RuntimeError(
            "Scheduled campaign bounds must include at least one calendar year."
        )


def _campaign_period() -> tuple[date, date]:
    """Resolve campaign date bounds from the already-exported environment."""
    try:
        start_value, end_value = resolve_campaign_period(os.environ)
    except ValueError as error:
        raise RuntimeError(str(error)) from error
    start_date = date.fromisoformat(start_value)
    end_date = date.fromisoformat(end_value)
    _validate_campaign_bounds(start_date, end_date)
    return start_date, end_date


# Stable campaign identity used by progress selection and reporting.
CAMPAIGN_KEY = _required_environment("CAMPAIGN_KEY")

# Inclusive quarter-aligned campaign bounds resolved from the environment.
CAMPAIGN_START_DATE, CAMPAIGN_END_DATE = _campaign_period()

# Local timezone used for campaign eligibility and interval interpretation.
CAMPAIGN_TIMEZONE = ZoneInfo(APPLICATION_TIMEZONE_NAME)

# Local timezone used by ETL task adapters.
PIPELINE_TIMEZONE = CAMPAIGN_TIMEZONE

# Environment variable that explicitly enables scheduled campaign ticks.
ENABLED_ENV = "ERA5_CAMPAIGN_SCHEDULE_ENABLED"

# Environment variable selecting the enabled scheduler cadence.
CADENCE_ENV = "ERA5_CAMPAIGN_SCHEDULE_CADENCE"

# Default scheduler cadence when scheduling is enabled.
DEFAULT_CADENCE = "2h"

# Supported scheduler cadences and Jakarta-local cron expressions.
CADENCE_CRON = {
    "30m": "*/30 * * * *",
    "1h": "0 * * * *",
    "2h": "0 */2 * * *",
}

# Provider availability lag retained for historical request eligibility.
AVAILABILITY_LAG_DAYS = 6

# Warning threshold for local artifact and temporary filesystems, in GiB.
DISK_WARNING_GIB = 5

# Number of bytes in one GiB for disk-space calculations.
GIB = 1024**3

# Hour spacing between mapped scheduled monthly lower bounds.
MONTHLY_SLOT_SPACING_HOURS = 1

# Shared application schema used by operational progress tables.
PIPELINE_PROGRESS_SCHEMA = APPLICATION_SCHEMA

# Parent progress table name.
PIPELINE_PROGRESS_TABLE = "pipeline_progress"

# Monthly partition progress table name.
PIPELINE_PROGRESS_DETAIL_TABLE = "pipeline_progress_detail"

# Shared application schema used by the submission pacing table.
PACING_SCHEMA = APPLICATION_SCHEMA

# Submission pacing table name.
PACING_TABLE = "submission_pacing"

# Environment variable controlling minimum seconds between submissions.
INTERVAL_ENV = "SUBMISSION_INTERVAL_SECONDS"

# Default minimum interval between provider submissions, in seconds.
DEFAULT_INTERVAL_SECONDS = 1800
