"""Operational configuration owned by the Airflow orchestration layer."""

from __future__ import annotations

import os
from zoneinfo import ZoneInfo

from config import APPLICATION_SCHEMA, APPLICATION_TIMEZONE_NAME


def _required_environment(name: str) -> str:
    """Return one required non-blank orchestration environment value."""
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"{name} is required. Set it in the project .env before starting Airflow."
        )
    return value.strip()


def _campaign_year(name: str) -> int:
    """Parse one required campaign year from the environment."""
    value = _required_environment(name)
    try:
        return int(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer year; received {value!r}.") from error


# Stable campaign identity used by progress selection and reporting.
CAMPAIGN_KEY = _required_environment("CAMPAIGN_KEY")

# First calendar year included in the bounded historical campaign.
CAMPAIGN_START_YEAR = _campaign_year("CAMPAIGN_START_YEAR")

# Last calendar year included in the bounded historical campaign.
CAMPAIGN_END_YEAR = _campaign_year("CAMPAIGN_END_YEAR")


def _validate_campaign_bounds(start_year: int, end_year: int) -> None:
    """Reject an inverted bounded historical campaign range."""
    if start_year > end_year:
        raise RuntimeError(
            "CAMPAIGN_START_YEAR must be less than or equal to CAMPAIGN_END_YEAR."
        )


_validate_campaign_bounds(CAMPAIGN_START_YEAR, CAMPAIGN_END_YEAR)

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

# Supported scheduler cadences and their Jakarta-local cron expressions.
CADENCE_CRON = {
    "30m": "*/30 * * * *",
    "1h": "0 * * * *",
    "2h": "0 */2 * * *",
}

# Provider availability lag retained for historical request eligibility.
AVAILABILITY_LAG_DAYS = 6

# Warning threshold for local artifact and temporary filesystems, in GiB.
DISK_WARNING_GIB = 5

# Number of bytes in one GiB for disk-space logging calculations.
GIB = 1024**3

# Hour spacing between mapped scheduled monthly lower bounds.
MONTHLY_SLOT_SPACING_HOURS = 1

# Shared application schema used by operational progress tables.
PIPELINE_PROGRESS_SCHEMA = APPLICATION_SCHEMA

# Parent progress table name.
PIPELINE_PROGRESS_TABLE = "pipeline_progress"

# Monthly partition progress table name.
PIPELINE_PROGRESS_DETAIL_TABLE = "pipeline_progress_detail"

# Shared application schema used by the CDS pacing table.
PACING_SCHEMA = APPLICATION_SCHEMA

# CDS pacing table name.
PACING_TABLE = "cds_submission_pacing"

# Stable row key for the ERA5-Land CDS submission clock.
PACING_KEY = "cds_era5_land"

# Environment variable controlling minimum seconds between CDS submissions.
INTERVAL_ENV = "CDS_MIN_SUBMISSION_INTERVAL_SECONDS"

# Default minimum interval between CDS submissions, in seconds.
DEFAULT_INTERVAL_SECONDS = 1800
