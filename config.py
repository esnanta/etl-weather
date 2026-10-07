"""Shared lightweight configuration for the ERA5-Land application.

This module intentionally contains only simple values and pure configuration
resolvers so pipeline, scripts, and orchestration can depend on it without
importing Airflow or heavy data libraries.

Application-wide constants are defined eagerly. Campaign settings are resolved
lazily on first access so standalone pipeline users are not forced to configure
Airflow campaign variables unless orchestration actually needs them.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import date
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Environment variable selecting destructive-maintenance policy.
APP_ENV_KEY = "APP_ENV"

# Development is the only environment where destructive maintenance may run.
APP_ENV_DEV = "dev"

# Production disables destructive maintenance completely.
APP_ENV_PROD = "prod"

# PostgreSQL application schema shared by weather and orchestration tables.
APPLICATION_SCHEMA = "climate_data"

# IANA timezone name used for the project's local daily semantics.
APPLICATION_TIMEZONE_NAME = "Asia/Jakarta"

# Aceh retrieval bounds used by the typed request factory.
ACEH_BBOX_COORDINATES = {
    "north_lat": 6.1,
    "south_lat": 1.9,
    "east_lon": 98.3,
    "west_lon": 94.9,
}

# Active Copernicus Climate Data Store dataset identifier.
CDS_DATASET = "reanalysis-era5-land"

# Environment key for the standalone pipeline default start date.
CDS_START_DATE_ENV = "CDS_START_DATE"

# Environment key for the standalone pipeline default end date.
CDS_END_DATE_ENV = "CDS_END_DATE"

# Active ERA5-Land variables requested from CDS, in stable request order.
CDS_VARIABLES = ("tp", "t2m", "swvl1", "ssrd", "d2m")

# Local timezone used by campaign eligibility and ETL task adapters.
CAMPAIGN_TIMEZONE = ZoneInfo(APPLICATION_TIMEZONE_NAME)
PIPELINE_TIMEZONE = CAMPAIGN_TIMEZONE

# Environment variable that explicitly enables scheduled campaign ticks.
ENABLED_ENV = "ERA5_CAMPAIGN_SCHEDULE_ENABLED"

# Fixed interval between scheduled campaign eligibility checks.
CAMPAIGN_SCHEDULE_INTERVAL_MINUTES = 10

# Minimum delay after a complete, valid quarter before the next quarter is eligible.
QUARTER_COOLDOWN_MINUTES = 10

# Provider availability lag retained for historical request eligibility.
AVAILABILITY_LAG_DAYS = 6

# Warning threshold for local artifact and temporary filesystems, in GiB.
DISK_WARNING_GIB = 5

# Number of bytes in one GiB for disk-space calculations.
GIB = 1024**3

# Minute spacing between mapped scheduled monthly lower bounds.
MONTHLY_SLOT_SPACING_MINUTES = 15

# Shared application schema aliases retained for operational table callers.
PIPELINE_PROGRESS_SCHEMA = APPLICATION_SCHEMA
PACING_SCHEMA = APPLICATION_SCHEMA

# Operational progress and pacing table names.
PIPELINE_PROGRESS_TABLE = "pipeline_progress"
PIPELINE_PROGRESS_DETAIL_TABLE = "pipeline_progress_detail"
PACING_TABLE = "submission_pacing"

# Fixed minimum interval between provider submission POST requests.
SUBMISSION_INTERVAL_MINUTES = 5

_CAMPAIGN_ATTRIBUTE_NAMES = frozenset(
    {
        "CAMPAIGN_KEY",
        "CAMPAIGN_START_YEAR",
        "CAMPAIGN_END_YEAR",
        "CAMPAIGN_START_DATE",
        "CAMPAIGN_END_DATE",
    }
)


def _environment(env: Mapping[str, str] | None) -> Mapping[str, str]:
    """Return the supplied environment or load the project's local .env."""
    if env is None:
        load_dotenv()
        return os.environ
    return env


def _required_environment(
    name: str,
    env: Mapping[str, str] | None = None,
) -> str:
    """Return one required non-blank project environment value."""
    env_vars = _environment(env)
    raw_value = env_vars.get(name)
    if raw_value is None or not raw_value.strip():
        raise RuntimeError(
            f"{name} is required. Set it in the project .env before starting Airflow."
        )
    return raw_value.strip()


def _campaign_year(
    name: str,
    env: Mapping[str, str] | None = None,
) -> int:
    """Return one required four-digit campaign year from the environment."""
    value = _required_environment(name, env)
    if len(value) != 4 or not value.isdigit():
        raise RuntimeError(f"{name} must be a four-digit year such as 2016.")
    return int(value)


def _validate_campaign_years(start_year: int, end_year: int) -> None:
    """Validate a scheduled campaign spanning one or more complete years."""
    if start_year > end_year:
        raise RuntimeError(
            "CAMPAIGN_START_YEAR must be less than or equal to CAMPAIGN_END_YEAR."
        )


def _load_campaign_attributes() -> None:
    """Resolve campaign settings once when orchestration first requests them."""
    if "CAMPAIGN_KEY" in globals():
        return

    campaign_key = _required_environment("CAMPAIGN_KEY")
    start_year = _campaign_year("CAMPAIGN_START_YEAR")
    end_year = _campaign_year("CAMPAIGN_END_YEAR")
    _validate_campaign_years(start_year, end_year)

    globals().update(
        {
            "CAMPAIGN_KEY": campaign_key,
            "CAMPAIGN_START_YEAR": start_year,
            "CAMPAIGN_END_YEAR": end_year,
            "CAMPAIGN_START_DATE": date(start_year, 1, 1),
            "CAMPAIGN_END_DATE": date(end_year, 12, 31),
        }
    )


def __getattr__(name: str) -> object:
    """Lazily expose required campaign constants without coupling pipeline imports."""
    if name in _CAMPAIGN_ATTRIBUTE_NAMES:
        _load_campaign_attributes()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _required_iso_date(
    name: str,
    env: Mapping[str, str] | None = None,
) -> str:
    """Return one required canonical YYYY-MM-DD environment value."""
    env_vars = _environment(env)
    raw_value = env_vars.get(name)
    if raw_value is None or not raw_value.strip():
        raise ValueError(f"{name} is required in the project .env.")
    value = raw_value.strip()
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(
            f"{name} must be a valid YYYY-MM-DD date; received {value!r}."
        ) from error
    if parsed.isoformat() != value:
        raise ValueError(
            f"{name} must use canonical YYYY-MM-DD format; received {value!r}."
        )
    return value


def resolve_date_range(
    start_name: str,
    end_name: str,
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Resolve one required inclusive date range from environment values."""
    start_date = _required_iso_date(start_name, env)
    end_date = _required_iso_date(end_name, env)
    if start_date > end_date:
        raise ValueError(f"{start_name} must be less than or equal to {end_name}.")
    return start_date, end_date


def resolve_cds_period(
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Resolve the standalone pipeline's configured default CDS period."""
    return resolve_date_range(CDS_START_DATE_ENV, CDS_END_DATE_ENV, env)


def resolve_app_environment(
    env: Mapping[str, str] | None = None,
) -> str:
    """Return the required validated application environment."""
    env_vars = _environment(env)
    raw_value = env_vars.get(APP_ENV_KEY)
    if raw_value is None or not raw_value.strip():
        raise ValueError(
            f"{APP_ENV_KEY} is required. Set {APP_ENV_KEY}={APP_ENV_DEV} "
            f"or {APP_ENV_KEY}={APP_ENV_PROD} before starting the application."
        )
    value = raw_value.strip().lower()
    if value not in {APP_ENV_DEV, APP_ENV_PROD}:
        raise ValueError(
            f"Invalid {APP_ENV_KEY}={raw_value!r}. Set {APP_ENV_KEY}={APP_ENV_DEV} "
            f"or {APP_ENV_KEY}={APP_ENV_PROD} before starting the application."
        )
    return value


def database_recreation_allowed(
    env: Mapping[str, str] | None = None,
) -> bool:
    """Return whether destructive database recreation may be exposed."""
    return resolve_app_environment(env) == APP_ENV_DEV


def validate_and_log_app_environment(
    logger: logging.Logger,
    env: Mapping[str, str] | None = None,
) -> str:
    """Validate APP_ENV, log the active mode, and surface actionable failures."""
    try:
        value = resolve_app_environment(env)
    except ValueError as error:
        logger.warning("Application startup aborted | %s", error)
        raise
    logger.info("Application environment | APP_ENV=%s", value)
    return value
