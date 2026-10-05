"""Define ERA5-Land metadata, CDS payload mapping, and schema normalization."""

from __future__ import annotations

from datetime import datetime, timedelta

import xarray as xr

from pipeline.models.weather import WeatherRequest


VARIABLE_DESCRIPTIONS = {
    "tp": "total precipitation",
    "t2m": "2 metre temperature",
    "d2m": "2 metre dewpoint temperature",
    "swvl1": "volumetric soil water layer 1",
    "ssrd": "surface solar radiation downwards",
}

CDS_API_VARIABLE_NAMES = {
    "tp": "total_precipitation",
    "t2m": "2m_temperature",
    "d2m": "2m_dewpoint_temperature",
    "swvl1": "volumetric_soil_water_layer_1",
    "ssrd": "surface_solar_radiation_downwards",
}

CDS_API_TO_PROVIDER_VARIABLES = {
    api_name: provider_name
    for provider_name, api_name in CDS_API_VARIABLE_NAMES.items()
}

CDS_ERA5_LAND_VARIABLES = {
    "tp",
    "t2m",
    "d2m",
    "swvl1",
    "ssrd",
}

CDS_DATASET_PROFILES = {
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


def build_cds_request_payload(request: WeatherRequest) -> dict[str, object]:
    """Build the ERA5-Land hourly CDS payload for one calendar-month chunk."""
    start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    if start.month != end.month or start.year != end.year:
        raise ValueError(
            "CDS request interval must stay in one month. "
            "Use split_request_by_month before extraction."
        )

    request_days: list[str] = []
    current_day = start
    while current_day <= end:
        request_days.append(f"{current_day.day:02d}")
        current_day += timedelta(days=1)

    from pipeline.providers.catalog import get_source_dataset_profile

    profile = get_source_dataset_profile(request.source, request.dataset)
    api_variables = [
        CDS_API_VARIABLE_NAMES.get(variable, variable)
        for variable in request.variables
    ]

    payload: dict[str, object] = {
        "variable": api_variables,
        "year": f"{start.year}",
        "month": f"{start.month:02d}",
        "day": request_days,
        "time": [f"{hour:02d}:00" for hour in range(24)],
        "area": [
            request.bbox.north_lat,
            request.bbox.west_lon,
            request.bbox.south_lat,
            request.bbox.east_lon,
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
    }
    if profile.get("requires_product_type", False):
        payload["product_type"] = "reanalysis"
    return payload


def normalize_cds_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """Normalize CDS schema mechanics while preserving provider variable codes."""
    if "time" in dataset.coords and "valid_time" not in dataset.coords:
        dataset = dataset.rename({"time": "valid_time"})

    rename_map = {
        api_name: provider_name
        for api_name, provider_name in CDS_API_TO_PROVIDER_VARIABLES.items()
        if api_name in dataset.data_vars and provider_name not in dataset.data_vars
    }
    if rename_map:
        dataset = dataset.rename(rename_map)
    return dataset
