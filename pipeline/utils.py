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

# Canonical meteorological names used by the active CDS pipeline. Provider
# aliases are normalized to these names at extraction boundaries and must not
# leak into RAW, transformed Parquet, validation contracts, or new DB writes.
CANONICAL_VARIABLE_NAMES = frozenset(
    {
        "total_precipitation",
        "surface_solar_radiation_downwards",
        "2m_temperature",
        "2m_dewpoint_temperature",
        "volumetric_soil_water_layer_1",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "mean_sea_level_pressure",
        "sea_surface_temperature",
        "significant_height_of_combined_wind_waves_and_swell",
    }
)

SOURCE_VARIABLE_ALIASES = {
    "tp": "total_precipitation",
    "ssrd": "surface_solar_radiation_downwards",
    "t2m": "2m_temperature",
    "d2m": "2m_dewpoint_temperature",
    "swvl1": "volumetric_soil_water_layer_1",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
    "msl": "mean_sea_level_pressure",
    "sst": "sea_surface_temperature",
    "swh": "significant_height_of_combined_wind_waves_and_swell",
}

# Focused compatibility name used by CDS NetCDF normalization.
CDS_SHORT_TO_CANONICAL_VARIABLES = {
    alias: canonical
    for alias, canonical in SOURCE_VARIABLE_ALIASES.items()
    if alias in {"tp", "ssrd", "t2m", "d2m", "swvl1"}
}

# Short names are allowed only for compact artifact signatures.
CANONICAL_TO_SHORT_VARIABLE_NAMES = {
    canonical: alias
    for alias, canonical in SOURCE_VARIABLE_ALIASES.items()
}

# ARCO remains source-native until its normalization path is harmonized in a
# separate change; P9 here closes the active CDS ERA5-Land naming contract.
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

CDS_ERA5_LAND_VARIABLES = {
    "2m_temperature",
    "2m_dewpoint_temperature",
    "surface_solar_radiation_downwards",
    "total_precipitation",
    "volumetric_soil_water_layer_1",
}

ARCO_DATASET_PROFILES = {
    "era5-single-levels": {
        "dataset_name": "arco-era5",
        "zarr_url": "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
        "data_reference": "https://github.com/google-research/arco-era5",
        "grid": "equiangular lat-lon",
        "horizontal_resolution": "0.25 deg",
        "horizontal_resolution_degrees": 0.25,
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
        "horizontal_resolution_degrees": 0.25,
        "time_steps_utc": "00..23 (hourly)",
        "levels": "single level",
        "requires_product_type": True,
        "supported_variables": CDS_ERA5_SINGLE_LEVELS_VARIABLES,
    },
    "reanalysis-era5-land": {
        "dataset_name": "copernicus-cds-era5-land",
        "api_dataset": "reanalysis-era5-land",
        "data_reference": "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land",
        "grid": "regular lat-lon",
        "horizontal_resolution": "0.1 deg",
        "horizontal_resolution_degrees": 0.1,
        "time_steps_utc": "00..23 (hourly)",
        "levels": "single level / land surface",
        "requires_product_type": False,
        "supported_variables": CDS_ERA5_LAND_VARIABLES,
    },
}

DATASET_PROFILES_BY_SOURCE = {
    DataSource.ARCO: ARCO_DATASET_PROFILES,
    DataSource.CDS: CDS_DATASET_PROFILES,
}


# =============================================================================
# VARIABLE NAMING
# =============================================================================


def canonicalize_variable_name(variable: str) -> str:
    """Return the CDS pipeline canonical name for a provider alias/name."""
    normalized = variable.strip().lower()
    return SOURCE_VARIABLE_ALIASES.get(normalized, normalized)


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
    """Build a normalized WeatherRequest with source-aware variable naming."""
    dataset = request.dataset.strip().lower()

    source_profiles = DATASET_PROFILES_BY_SOURCE.get(request.source)
    if source_profiles is None:
        raise ValueError(f"Unsupported source: {request.source.value}")

    if dataset not in source_profiles:
        raise ValueError(
            f"Unsupported dataset '{dataset}' for source '{request.source.value}'."
        )

    if request.source == DataSource.CDS:
        variables = [
            canonicalize_variable_name(variable)
            for variable in request.variables
            if variable.strip()
        ]
    else:
        variables = [
            variable.strip().lower()
            for variable in request.variables
            if variable.strip()
        ]

    if not variables:
        raise ValueError("At least one variable must be provided.")

    if len(set(variables)) != len(variables):
        raise ValueError(
            "Variables resolve to duplicate names after normalization."
        )

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
    """Split a WeatherRequest into calendar-month requests."""
    start_date = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    requests: list[WeatherRequest] = []

    current_start = start_date

    while current_start <= end_date:
        last_day = monthrange(current_start.year, current_start.month)[1]
        month_end = current_start.replace(day=last_day)
        period_end = min(month_end, end_date)

        requests.append(
            replace(
                request,
                start_date=current_start.isoformat(),
                end_date=period_end.isoformat(),
            )
        )

        current_start = period_end + timedelta(days=1)

    return requests


def split_request_by_week(request: WeatherRequest) -> list[WeatherRequest]:
    """
    Split a WeatherRequest into weekly WeatherRequests.
    Retained as a fallback strategy for unusually large or failed requests.
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
    """Build a deterministic compact variable signature for artifact names."""
    canonical_sorted = sorted(canonicalize_variable_name(v) for v in variables)
    short_names = [
        CANONICAL_TO_SHORT_VARIABLE_NAMES.get(variable, variable)
        for variable in canonical_sorted
    ]
    return "_".join(short_names)


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
