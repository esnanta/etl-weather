from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import xarray as xr
from dotenv import load_dotenv

from pipeline.interfaces import WeatherExtractor
from pipeline.logging import log_metric
from pipeline.model import DataSource, RawWeatherDataset, WeatherRequest
from pipeline.utils import (
    CDS_API_TO_PROVIDER_VARIABLES,
    CDS_API_VARIABLE_NAMES,
    VARIABLE_DESCRIPTIONS,
    build_request,
    get_source_dataset_profile,
    split_request_by_month,
    validate_request,
)

logger = logging.getLogger(__name__)

CDSAPI_URL_ENV_KEY = "CDSAPI_URL"
CDSAPI_KEY_ENV_KEY = "CDSAPI_KEY"
DEFAULT_CDSAPI_URL = "https://cds.climate.copernicus.eu/api"
ERA5_LAND_DATASET = "reanalysis-era5-land"
ERA5_LAND_START_DATE = "1950-01-01"
LOCAL_DAY_CONTEXT_DAYS = 1


def _normalize_cds_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """Normalize schema mechanics while preserving provider variable names."""
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


def _resolve_cds_credentials() -> tuple[str, str]:
    load_dotenv()
    cdsapi_url = os.getenv(CDSAPI_URL_ENV_KEY, DEFAULT_CDSAPI_URL).strip()
    cdsapi_key = os.getenv(CDSAPI_KEY_ENV_KEY, "").strip()
    if not cdsapi_key:
        raise ValueError(
            f"{CDSAPI_KEY_ENV_KEY} is missing. Set it in .env as "
            "CDSAPI_KEY=YOUR_UID:YOUR_API_KEY"
        )
    return cdsapi_url, cdsapi_key


def _build_cds_request_payload(request: WeatherRequest) -> dict[str, object]:
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


def _build_era5_land_retrieval_request(request: WeatherRequest) -> WeatherRequest:
    start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    dataset_start = datetime.strptime(ERA5_LAND_START_DATE, "%Y-%m-%d").date()
    buffered_start = max(
        start - timedelta(days=LOCAL_DAY_CONTEXT_DAYS),
        dataset_start,
    )
    return replace(request, start_date=buffered_start.isoformat())


def _download_cds(request: WeatherRequest) -> xr.Dataset:
    try:
        import cdsapi
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'cdsapi'. Install it to use source='cds'."
        ) from error

    cdsapi_url, cdsapi_key = _resolve_cds_credentials()
    profile = get_source_dataset_profile(request.source, request.dataset)
    payload = _build_cds_request_payload(request)
    client = cdsapi.Client(url=cdsapi_url, key=cdsapi_key, quiet=True)

    temp_path = Path(tempfile.gettempdir()) / (
        f"cds_{request.start_date}_{request.end_date}_"
        f"{'_'.join(request.variables)}.nc"
    )
    if temp_path.exists():
        temp_path.unlink()

    logger.info(
        "Downloading CDS dataset from Copernicus API | period=%s -> %s.",
        request.start_date,
        request.end_date,
    )
    try:
        client.retrieve(profile["api_dataset"], payload, str(temp_path))
        with xr.open_dataset(temp_path) as opened_dataset:
            dataset = opened_dataset.load()
    finally:
        if temp_path.exists():
            temp_path.unlink()

    dataset = _normalize_cds_dataset(dataset)
    log_metric(
        "CDS source profile",
        {
            "dataset_name": profile["dataset_name"],
            "data_reference": profile["data_reference"],
            "grid": profile["grid"],
            "horizontal_resolution": profile["horizontal_resolution"],
            "time_steps_utc": profile["time_steps_utc"],
            "levels": profile["levels"],
            "retrieved_period": f"{request.start_date} -> {request.end_date}",
            "requested_variables": ", ".join(request.variables),
            "variable_descriptions": {
                variable: VARIABLE_DESCRIPTIONS.get(variable, variable)
                for variable in request.variables
            },
            "available_variables": len(dataset.data_vars),
        },
    )
    return dataset


def _download_cds_period(request: WeatherRequest) -> xr.Dataset:
    chunks = split_request_by_month(request)
    datasets = [_download_cds(chunk) for chunk in chunks]
    if len(datasets) == 1:
        return datasets[0]

    combined = xr.concat(
        datasets,
        dim="valid_time",
        data_vars="minimal",
        coords="minimal",
        compat="override",
    ).sortby("valid_time")
    valid_time_index = combined.get_index("valid_time")
    if valid_time_index.has_duplicates:
        combined = combined.isel(
            valid_time=~valid_time_index.duplicated(keep="first")
        )
    return combined


class CdsExtractor(WeatherExtractor):
    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        validate_request(request)
        normalized_request = build_request(request)
        retrieval_request = _build_era5_land_retrieval_request(
            normalized_request
        )

        dataset = _download_cds_period(retrieval_request)

        missing_variables = [
            variable
            for variable in normalized_request.variables
            if variable not in dataset.data_vars
        ]
        if missing_variables:
            available_variables = ", ".join(sorted(str(v) for v in dataset.data_vars))
            raise ValueError(
                "Requested CDS variables are missing from the downloaded dataset. "
                f"Missing: {', '.join(missing_variables)}. "
                f"Available: {available_variables}"
            )

        dataset = dataset[normalized_request.variables]
        return RawWeatherDataset(
            request=normalized_request,
            downloaded_at=datetime.now(timezone.utc),
            raw_data=dataset,
        )
