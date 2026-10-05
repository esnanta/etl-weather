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
    """Validate ordered quarter-aligned campaign date boundaries."""
    if start_date > end_date:
        raise RuntimeError(
            "CAMPAIGN_START_DATE must be less than or equal to CAMPAIGN_END_DATE."
        )
    if (start_date.month, start_date.day) not in {
        (1, 1), (4, 1), (7, 1), (10, 1)
    }:
        raise RuntimeError(
            "CAMPAIGN_START_DATE must be the first day of a calendar quarter."
        )
    if (end_date.month, end_date.day) not in {
        (3, 31), (6, 30), (9, 30), (12, 31)
    }:
        raise RuntimeError(
            "CAMPAIGN_END_DATE must be the last day of a calendar quarter."
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


# Stable campaign identity used by progress selection and reporting.\nCAMPAIGN_KEY = _required_environment("CAMPAIGN_KEY")
# Inclusive quarter-aligned campaign bounds resolved from the environment.\nCAMPAIGN_START_DATE, CAMPAIGN_END_DATE = _campaign_period()

# Local timezone used for campaign eligibility and interval interpretation.\nCAMPAIGN_TIMEZONE = ZoneInfo(APPLICATION_TIMEZONE_NAME)
# Local timezone used by ETL task adapters.\nPIPELINE_TIMEZONE = CAMPAIGN_TIMEZONE

# Environment variable that explicitly enables scheduled campaign ticks.\nENABLED_ENV = "ERA5_CAMPAIGN_SCHEDULE_ENABLED"
# Environment variable selecting the enabled scheduler cadence.\nCADENCE_ENV = "ERA5_CAMPAIGN_SCHEDULE_CADENCE"
# Default scheduler cadence when scheduling is enabled.\nDEFAULT_CADENCE = "2h"
# Supported scheduler cadences and Jakarta-local cron expressions.\nCADENCE_CRON = {
    "30m": "*/30 * * * *",
    "1h": "0 * * * *",
    "2h": "0 */2 * * *",
}

# Provider availability lag retained for historical request eligibility.\nAVAILABILITY_LAG_DAYS = 6
# Warning threshold for local artifact and temporary filesystems, in GiB.\nDISK_WARNING_# Number of bytes in one GiB for disk-space calculations.\nGIB = 5
GIB = 1024**3
# Hour spacing between mapped scheduled monthly lower bounds.\nMONTHLY_SLOT_SPACING_HOURS = 1

# Shared application schema used by operational progress tables.\nPIPELINE_PROGRESS_SCHEMA = APPLICATION_SCHEMA
# Parent progress table name.\nPIPELINE_PROGRESS_TABLE = "pipeline_progress"
# Monthly partition progress table name.\nPIPELINE_PROGRESS_DETAIL_TABLE = "pipeline_progress_detail"
# Shared application schema used by the submission pacing table.\nPACING_SCHEMA = APPLICATION_SCHEMA
# Submission pacing table name.\nPACING_TABLE = "submission_pacing"
# Environment variable controlling minimum seconds between submissions.\nINTERVAL_ENV = "SUBMISSION_INTERVAL_SECONDS"
# Default minimum interval between provider submissions, in seconds.\nDEFAULT_INTERVAL_SECONDS = 1800
