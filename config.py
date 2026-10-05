"""Shared lightweight configuration for the ERA5-Land application.

This module intentionally contains only simple values so pipeline, scripts, and
orchestration can depend on it without importing Airflow or heavy data libraries.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping

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

# Standalone script default start date; this is not the campaign start year.
CDS_START_DATE = "2022-01-01"

# Standalone script default end date; this is not the campaign end year.
CDS_END_DATE = "2022-01-03"

# Active ERA5-Land variables requested from CDS, in stable request order.
CDS_VARIABLES = ("tp", "t2m", "swvl1", "ssrd", "d2m")


def resolve_app_environment(
    env: Mapping[str, str] | None = None,
) -> str:
    """Return the required validated application environment."""
    if env is None:
        load_dotenv()
        env_vars = os.environ
    else:
        env_vars = env
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
