"""Shared lightweight configuration for the ERA5-Land application.

This module intentionally contains only simple values and pure configuration
resolvers so pipeline, scripts, and orchestration can depend on it without
importing Airflow or heavy data libraries.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import date

from dotenv import load_dotenv

# Environment variable selecting destructive-maintenance policy.
APP_ENV_KEY = "APP_ENV"

# Development is the only environment where database recreation may be exposed.
APP_ENV_DEV = "dev"

# Production disables database recreation completely.
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

# Environment key for the bounded campaign start date.
CAMPAIGN_START_DATE_ENV = "CAMPAIGN_START_DATE"

# Environment key for the bounded campaign end date.
CAMPAIGN_END_DATE_ENV = "CAMPAIGN_END_DATE"

# Active ERA5-Land variables requested from CDS, in stable request order.
CDS_VARIABLES = ("tp", "t2m", "swvl1", "ssrd", "d2m")


def _environment(env: Mapping[str, str] | None) -> Mapping[str, str]:
    """Return the supplied environment or load the project's local .env."""
    if env is None:
        load_dotenv()
        return os.environ
    return env


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


def resolve_campaign_period(
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Resolve the scheduled campaign's configured inclusive date bounds."""
    return resolve_date_range(
        CAMPAIGN_START_DATE_ENV,
        CAMPAIGN_END_DATE_ENV,
        env,
    )


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
