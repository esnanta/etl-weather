"""Shared lightweight configuration for the ERA5-Land application.

This module intentionally contains only simple values so pipeline, scripts, and
orchestration can depend on it without importing Airflow or heavy data libraries.
"""

from __future__ import annotations

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
