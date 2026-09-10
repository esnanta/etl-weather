from __future__ import annotations

import logging
from calendar import monthrange
from dataclasses import replace
from datetime import datetime, timedelta

from pipeline.model import WeatherRequest, DataSource

logger = logging.getLogger(__name__)

# =============================================================================
# DOMAIN CONFIGURATIONS
# =============================================================================

ARCO_ERA5_SINGLE_LEVELS_VARIABLES = {
    "total_precipitation",
    "surface_solar_radiation_downwards",
    "t2m",
    "u10",
    "v10",
    "swh",
    "msl",
}

CDS_ERA5_SINGLE_LEVELS_VARIABLES = {
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "mean_sea_level_pressure",
    "surface_solar_radiation_downwards",
    "total_precipitation",
    "sea_surface_temperature",
    "significant_height_of_combined_wind_waves_and_swell",
}

ARCO_DATASET_PROFILES = {
    "era5-single-levels": {
        "dataset_name": "arco-era5",
        "zarr_url": "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
        "data_reference": "https://github.com/google-research/arco-era5",
        "grid": "equiangular lat-lon",
        "horizontal_resolution": "0.25 deg",
        "time_steps_utc": "00..23 (hourly)",
        "levels": "1..1000 hPa (37 levels)",
        "total_size": "2.05 PB",
        "chunking": "{time:1, latitude:721, longitude:1440, level:37}",
        "chunk_size_per_variable": "154 MB",
        "supported_variables": ARCO_ERA5_SINGLE_LEVELS_VARIABLES,
    },
}

CDS_DATASET_PROFILES = {
    "reanalysis-era5-single-levels": {
        "dataset_name": "copernicus-cds-era5-single-levels",
        "api_dataset": "reanalysis-era5-single-levels",
        "data_reference": "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels",
        "grid": "regular lat-lon",
        "horizontal_resolution": "0.25 deg",
        "time_steps_utc": "00..23 (hourly)",
        "levels": "single level",
        "supported_variables": CDS_ERA5_SINGLE_LEVELS_VARIABLES,
    },
}

DATASET_PROFILES_BY_SOURCE = {
    DataSource.ARCO: ARCO_DATASET_PROFILES,
    DataSource.CDS: CDS_DATASET_PROFILES,
}

# =============================================================================
# REQUEST UTILITIES
# =============================================================================

def validate_request(request: WeatherRequest) -> None:
    """Validate a WeatherRequest."""
    if not isinstance(request.source, DataSource):
        raise ValueError("Invalid data source.")
    if not request.dataset.strip():
        raise ValueError("Dataset must not be empty.")
    if not request.variables:
        raise ValueError("At least one variable must be provided.")
    if not request.start_date.strip():
        raise ValueError("Start date must not be empty.")
    if not request.end_date.strip():
        raise ValueError("End date must not be empty.")

    try:
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Date format must be YYYY-MM-DD.")

    if start_date > end_date:
        raise ValueError("Start date must be before end date.")

    bbox = request.bbox
    if not (-90 <= bbox.south_lat <= 90):
        raise ValueError("Invalid south latitude.")
    if not (-90 <= bbox.north_lat <= 90):
        raise ValueError("Invalid north latitude.")
    if not (-180 <= bbox.west_lon <= 180):
        raise ValueError("Invalid west longitude.")
    if not (-180 <= bbox.east_lon <= 180):
        raise ValueError("Invalid east longitude.")
    if bbox.south_lat > bbox.north_lat:
        raise ValueError("South latitude must be less than or equal to north latitude.")
    if bbox.west_lon > bbox.east_lon:
        raise ValueError("West longitude must be less than or equal to east longitude.")

def build_request(request: WeatherRequest) -> WeatherRequest:
    """Build a normalized WeatherRequest."""
    dataset = request.dataset.strip().lower()

    source_profiles = DATASET_PROFILES_BY_SOURCE.get(request.source)
    if source_profiles is None:
        raise ValueError(f"Unsupported source: {request.source.value}")

    if dataset not in source_profiles:
        raise ValueError(
            f"Unsupported dataset '{dataset}' for source '{request.source.value}'."
        )

    variables = [variable.strip().lower() for variable in request.variables if variable.strip()]

    if not variables:
        raise ValueError("At least one variable must be provided.")

    supported_variables = source_profiles[dataset]["supported_variables"]
    for variable in variables:
        if variable not in supported_variables:
            raise ValueError(
                f"Variable '{variable}' is not supported by dataset '{dataset}' "
                f"for source '{request.source.value}'."
            )

    return WeatherRequest(
        source=request.source,
        dataset=dataset,
        start_date=request.start_date,
        end_date=request.end_date,
        variables=variables,
        bbox=request.bbox,
    )

def split_request_by_month(request: WeatherRequest) -> list[WeatherRequest]:
    """Split a WeatherRequest into monthly WeatherRequests."""
    start_date = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    requests: list[WeatherRequest] = []

    year = start_date.year
    month = start_date.month

    while True:
        month_start = datetime(year, month, 1).date()
        last_day = monthrange(year, month)[1]
        month_end = datetime(year, month, last_day).date()

        period_start = max(start_date, month_start)
        period_end = min(end_date, month_end)

        requests.append(
            replace(
                request,
                start_date=period_start.isoformat(),
                end_date=period_end.isoformat(),
            )
        )

        if period_end >= end_date:
            break

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    return requests

def split_request_by_week(request: WeatherRequest) -> list[WeatherRequest]:
    """
    Split a WeatherRequest into weekly WeatherRequests.
    Ensures intervals do not cross month boundaries or exceed request end_date.
    """
    start_date = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    requests: list[WeatherRequest] = []

    current_start = start_date

    while current_start <= end_date:
        week_end = current_start + timedelta(days=6)

        last_day_of_month = monthrange(current_start.year, current_start.month)[1]
        month_end = datetime(current_start.year, current_start.month, last_day_of_month).date()

        period_end = min(week_end, month_end, end_date)

        requests.append(
            replace(
                request,
                start_date=current_start.isoformat(),
                end_date=period_end.isoformat(),
            )
        )

        current_start = period_end + timedelta(days=1)

    return requests

# =============================================================================
# STORAGE UTILITIES
# =============================================================================

def build_variable_signature(variables: list[str]) -> str:
    """Build a deterministic variable signature."""
    return "_".join(sorted(variables))

def format_request_period(start_date: str, end_date: str) -> str:
    """Format a request period for filenames."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    return f"{start:%Y}_{start:%m%d}_{end:%m%d}"


def get_source_dataset_profile(source: DataSource, dataset: str) -> dict:
    """Return source-specific dataset profile."""
    source_profiles = DATASET_PROFILES_BY_SOURCE.get(source)
    if source_profiles is None:
        raise ValueError(f"Unsupported source: {source.value}")

    dataset_key = dataset.strip().lower()
    profile = source_profiles.get(dataset_key)
    if profile is None:
        raise ValueError(
            f"Unsupported dataset '{dataset_key}' for source '{source.value}'."
        )

    return profile
