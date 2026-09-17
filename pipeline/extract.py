from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import xarray as xr
from dotenv import load_dotenv

from pipeline.interfaces import WeatherExtractor
from pipeline.logging import log_metric
from pipeline.model import DataSource, RawWeatherDataset, WeatherRequest
from pipeline.utils import (
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

# CDS NetCDF files may expose provider-native short variable names. This map is
# private to the extraction boundary so short names never become part of the
# pipeline's public request, artifact, validation, or database schema.
_CDS_PROVIDER_VARIABLE_NAMES = {
    "tp": "total_precipitation",
    "ssrd": "surface_solar_radiation_downwards",
    "t2m": "2m_temperature",
    "d2m": "2m_dewpoint_temperature",
    "swvl1": "volumetric_soil_water_layer_1",
}


# =============================================================================
# SOURCE NORMALIZATION
# =============================================================================


def _normalize_arco_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """Normalize ARCO temporal coordinate to the pipeline canonical name."""
    if "time" in dataset.coords or "time" in dataset.dims:
        dataset = dataset.rename({"time": "valid_time"})
    return dataset


def _normalize_cds_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """
    Normalize CDS provider schema to the pipeline canonical representation.

    valid_time is intentionally retained as the canonical temporal coordinate.
    Provider-native short names are converted immediately and do not leave the
    extraction boundary.
    """
    if "time" in dataset.coords and "valid_time" not in dataset.coords:
        dataset = dataset.rename({"time": "valid_time"})

    rename_map = {
        provider_name: canonical_name
        for provider_name, canonical_name in _CDS_PROVIDER_VARIABLE_NAMES.items()
        if provider_name in dataset.data_vars and canonical_name not in dataset.data_vars
    }
    if rename_map:
        dataset = dataset.rename(rename_map)

    return dataset


# =============================================================================
# ARCO DOWNLOAD
# =============================================================================


def _download_arco(request: WeatherRequest) -> xr.Dataset:
    """Open the ARCO ERA5 analysis-ready dataset."""
    logger.info("Opening %s.", request.dataset)

    profile = get_source_dataset_profile(
        request.source,
        request.dataset,
    )

    custom_storage_options = {
        "token": "anon",
        "connections": 10,
        "client_kwargs": {
            "timeout": aiohttp.ClientTimeout(total=1200),
        },
    }

    dataset = xr.open_zarr(
        profile["zarr_url"],
        chunks=None,
        storage_options=custom_storage_options,
    )

    logger.info("Successfully opened ARCO dataset.")

    min_time = str(dataset.time.min().values)[:10]
    max_time = str(dataset.time.max().values)[:10]

    log_metric(
        "ARCO source profile",
        {
            "dataset_name": profile["dataset_name"],
            "data_reference": profile["data_reference"],
            "grid": profile["grid"],
            "horizontal_resolution": profile["horizontal_resolution"],
            "time_steps_utc": profile["time_steps_utc"],
            "levels": profile["levels"],
            "total_size": profile["total_size"],
            "chunking": profile["chunking"],
            "chunk_size_per_variable": profile["chunk_size_per_variable"],
            "valid_time_start": dataset.attrs.get("valid_time_start", min_time),
            "valid_time_stop": dataset.attrs.get("valid_time_stop", max_time),
            "server_period_scan": f"{min_time} -> {max_time}",
            "available_variables": len(dataset.data_vars),
        },
    )

    if request.start_date > max_time or request.end_date < min_time:
        logger.warning("Requested period is outside the available dataset range.")

    return _normalize_arco_dataset(dataset)


# =============================================================================
# CDS CONFIGURATION
# =============================================================================


def _resolve_cds_credentials() -> tuple[str, str]:
    """Resolve CDS API URL and key from environment."""
    load_dotenv()

    cdsapi_url = os.getenv(
        CDSAPI_URL_ENV_KEY,
        DEFAULT_CDSAPI_URL,
    ).strip()

    cdsapi_key = os.getenv(
        CDSAPI_KEY_ENV_KEY,
        "",
    ).strip()

    if not cdsapi_key:
        raise ValueError(
            f"{CDSAPI_KEY_ENV_KEY} is missing. "
            "Set it in .env as CDSAPI_KEY=YOUR_UID:YOUR_API_KEY"
        )

    return cdsapi_url, cdsapi_key


def _build_cds_request_payload(
    request: WeatherRequest,
) -> dict[str, object]:
    """
    Build a CDS API payload for one request interval.

    One request must remain within a calendar month. ERA5-Land does not
    require product_type, whereas ERA5 single-level reanalysis does.
    """
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
        current_day = current_day + timedelta(days=1)

    profile = get_source_dataset_profile(
        request.source,
        request.dataset,
    )

    payload: dict[str, object] = {
        "variable": request.variables,
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


def _build_era5_land_retrieval_request(
    request: WeatherRequest,
) -> WeatherRequest:
    """
    Add one preceding UTC day for UTC+7 local-day aggregation.

    The request metadata still represents the target period. The extra day is
    retrieval context only and is retained in the RAW hourly checkpoint.
    """
    start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    dataset_start = datetime.strptime(ERA5_LAND_START_DATE, "%Y-%m-%d").date()

    buffered_start = max(
        start - timedelta(days=LOCAL_DAY_CONTEXT_DAYS),
        dataset_start,
    )

    return replace(
        request,
        start_date=buffered_start.isoformat(),
    )


# =============================================================================
# CDS DOWNLOAD
# =============================================================================


def _download_cds(
    request: WeatherRequest,
) -> xr.Dataset:
    """Download one calendar-month CDS request from Copernicus as NetCDF."""
    try:
        import cdsapi
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'cdsapi'. Install it to use source='cds'."
        ) from error

    cdsapi_url, cdsapi_key = _resolve_cds_credentials()
    profile = get_source_dataset_profile(
        request.source,
        request.dataset,
    )
    payload = _build_cds_request_payload(request)

    client = cdsapi.Client(
        url=cdsapi_url,
        key=cdsapi_key,
        quiet=True,
    )

    temp_path = (
        Path(tempfile.gettempdir())
        / (
            f"cds_{request.start_date}_{request.end_date}_"
            f"{'_'.join(request.variables)}.nc"
        )
    )

    if temp_path.exists():
        temp_path.unlink()

    logger.info(
        "Downloading CDS dataset from Copernicus API | period=%s -> %s.",
        request.start_date,
        request.end_date,
    )

    try:
        client.retrieve(
            profile["api_dataset"],
            payload,
            str(temp_path),
        )

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
            "available_variables": len(dataset.data_vars),
        },
    )

    return dataset


def _download_cds_period(request: WeatherRequest) -> xr.Dataset:
    """Download a possibly cross-month CDS retrieval period and concatenate it."""
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


# =============================================================================
# DOWNLOAD STRATEGIES
# =============================================================================


DOWNLOAD_STRATEGIES = {
    DataSource.ARCO: _download_arco,
    DataSource.CDS: _download_cds_period,
}


def _download_data(request: WeatherRequest) -> xr.Dataset:
    """Download a weather dataset using the configured source strategy."""
    strategy = DOWNLOAD_STRATEGIES.get(request.source)
    if strategy is None:
        raise ValueError(f"Unsupported data source: {request.source}")
    return strategy(request)


# =============================================================================
# DATASET SELECTION
# =============================================================================


def _select_time(
    dataset: xr.Dataset,
    request: WeatherRequest,
) -> xr.Dataset:
    """Select the requested range using canonical valid_time."""
    if "valid_time" not in dataset.coords and "valid_time" not in dataset.dims:
        raise ValueError(
            "Dataset does not contain the canonical 'valid_time' coordinate."
        )

    return dataset.sel(
        valid_time=slice(
            request.start_date,
            request.end_date,
        ),
    )


def _select_bbox(
    dataset: xr.Dataset,
    request: WeatherRequest,
) -> xr.Dataset:
    """Select the requested geographic bounding box."""
    bbox = request.bbox

    log_metric(
        "Bounding box",
        {
            "lat": f"{bbox.south_lat:.2f}..{bbox.north_lat:.2f}",
            "lon": f"{bbox.west_lon:.2f}..{bbox.east_lon:.2f}",
        },
    )

    subset = dataset.sel(
        latitude=slice(
            bbox.north_lat,
            bbox.south_lat,
        ),
        longitude=slice(
            bbox.west_lon,
            bbox.east_lon,
        ),
    )

    log_metric(
        "Spatial subset",
        {
            "latitude": subset.sizes["latitude"],
            "longitude": subset.sizes["longitude"],
        },
    )

    return subset


# =============================================================================
# GENERIC EXTRACTION
# =============================================================================


def extract_data(
    request: WeatherRequest,
    dataset: xr.Dataset | None = None,
) -> RawWeatherDataset:
    """Extract weather data from an already opened dataset."""
    if dataset is None:
        raise ValueError("Dataset must be provided.")

    filtered_time = _select_time(
        dataset,
        request,
    )

    return RawWeatherDataset(
        request=request,
        downloaded_at=datetime.now(timezone.utc),
        raw_data=filtered_time,
    )


def open_dataset(
    request: WeatherRequest,
) -> xr.Dataset:
    """Open the source dataset once."""
    validate_request(request)
    normalized_request = build_request(request)
    logger.info("Downloading dataset.")
    return _download_data(normalized_request)


# =============================================================================
# EXTRACTORS
# =============================================================================


class ArcoExtractor(WeatherExtractor):
    """Extraction implementation for ARCO ERA5 datasets."""

    def __init__(self, dataset: xr.Dataset):
        self.dataset = dataset

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        return extract_data(request, self.dataset)


class CdsExtractor(WeatherExtractor):
    """Extraction implementation for CDS datasets."""

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        validate_request(request)
        normalized_request = build_request(request)

        retrieval_request = normalized_request
        if normalized_request.dataset == ERA5_LAND_DATASET:
            retrieval_request = _build_era5_land_retrieval_request(
                normalized_request
            )

        dataset = _download_cds_period(retrieval_request)

        if normalized_request.dataset != ERA5_LAND_DATASET:
            dataset = _select_time(dataset, normalized_request)

        missing_variables = [
            variable
            for variable in normalized_request.variables
            if variable not in dataset.data_vars
        ]

        if missing_variables:
            available_variable_names: list[str] = []
            for variable_name in dataset.data_vars.keys():
                if not isinstance(variable_name, str):
                    raise ValueError(
                        "CDS dataset contains a non-string variable name."
                    )
                available_variable_names.append(variable_name)

            available_variable_names.sort()
            available_variables = ", ".join(available_variable_names)
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
