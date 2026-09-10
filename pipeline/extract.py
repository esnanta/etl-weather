from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import xarray as xr
from dotenv import load_dotenv

from pipeline.interfaces import WeatherExtractor
from pipeline.logging import log_metric
from pipeline.model import DataSource, RawWeatherDataset, WeatherRequest
from pipeline.utils import build_request, get_source_dataset_profile, validate_request

logger = logging.getLogger(__name__)

CDSAPI_URL_ENV_KEY = "CDSAPI_URL"
CDSAPI_KEY_ENV_KEY = "CDSAPI_KEY"
DEFAULT_CDSAPI_URL = "https://cds.climate.copernicus.eu/api"


def _download_arco(request: WeatherRequest) -> xr.Dataset:
    """
    Open the ARCO ERA5 analysis-ready dataset.
    """

    logger.info(
        "Opening %s.",
        request.dataset,
    )
    profile = get_source_dataset_profile(request.source, request.dataset)

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
        logger.warning(
            "Requested period is outside the available dataset range."
        )

    return dataset


def _resolve_cds_credentials() -> tuple[str, str]:
    """Resolve CDS API URL and key from environment."""
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
    """Build CDS API payload for one request interval."""
    start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(request.end_date, "%Y-%m-%d").date()

    request_days: list[str] = []
    current_day = start
    while current_day <= end:
        request_days.append(f"{current_day.day:02d}")
        current_day = current_day + timedelta(days=1)

    request_month = f"{start.month:02d}"
    request_year = f"{start.year}"

    if start.month != end.month or start.year != end.year:
        raise ValueError(
            "CDS request interval must stay in one month. "
            "Use split_request_by_week before extraction."
        )

    request_times = [f"{hour:02d}:00" for hour in range(24)]

    return {
        "product_type": "reanalysis",
        "variable": request.variables,
        "year": request_year,
        "month": request_month,
        "day": request_days,
        "time": request_times,
        "area": [
            request.bbox.north_lat,
            request.bbox.west_lon,
            request.bbox.south_lat,
            request.bbox.east_lon,
        ],
        "format": "netcdf",
    }


def _download_cds(request: WeatherRequest) -> xr.Dataset:
    """Download CDS ERA5 dataset from Copernicus API as NetCDF."""
    try:
        import cdsapi
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'cdsapi'. Install it to use source='cds'."
        ) from error

    cdsapi_url, cdsapi_key = _resolve_cds_credentials()
    profile = get_source_dataset_profile(request.source, request.dataset)
    payload = _build_cds_request_payload(request)

    client = cdsapi.Client(
        url=cdsapi_url,
        key=cdsapi_key,
        quiet=True,
    )

    temp_path = Path(
        tempfile.gettempdir()
    ) / (
        f"cds_{request.start_date}_{request.end_date}_"
        f"{'_'.join(request.variables)}.nc"
    )

    if temp_path.exists():
        temp_path.unlink()

    logger.info("Downloading CDS dataset from Copernicus API.")
    try:
        client.retrieve(
            profile["api_dataset"],
            payload,
            str(temp_path),
        )

        dataset = xr.open_dataset(temp_path)
        dataset.load()
        dataset.close()
    finally:
        if temp_path.exists():
            temp_path.unlink()

    log_metric(
        "CDS source profile",
        {
            "dataset_name": profile["dataset_name"],
            "data_reference": profile["data_reference"],
            "grid": profile["grid"],
            "horizontal_resolution": profile["horizontal_resolution"],
            "time_steps_utc": profile["time_steps_utc"],
            "levels": profile["levels"],
            "requested_period": f"{request.start_date} -> {request.end_date}",
            "requested_variables": ", ".join(request.variables),
            "available_variables": len(dataset.data_vars),
        },
    )

    return dataset


DOWNLOAD_STRATEGIES = {
    DataSource.ARCO: _download_arco,
    DataSource.CDS: _download_cds,
}


def _download_data(request: WeatherRequest) -> xr.Dataset:
    """
    Download a weather dataset
    from the configured source using a strategy pattern.
    """
    strategy = DOWNLOAD_STRATEGIES.get(request.source)
    if not strategy:
        raise ValueError(f"Unsupported data source: {request.source}")
    return strategy(request)


def _select_time(dataset: xr.Dataset, request: WeatherRequest) -> xr.Dataset:
    """
    Select the requested time range.
    """

    return dataset.sel(
        time=slice(
            request.start_date,
            request.end_date,
        ),
    )


def _select_bbox(dataset: xr.Dataset, request: WeatherRequest) -> xr.Dataset:
    """
    Select the requested geographic bounding box.

    ARCO analysis-ready dataset stores latitude and longitude
    as independent dimensions, therefore spatial
    subsetting is performed using xarray.sel().
    """

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


def extract_data(
    request: WeatherRequest,
    dataset: xr.Dataset | None = None,
) -> RawWeatherDataset:
    """
    Extract weather data from an already opened and spatially-sliced dataset.
    """
    if dataset is None:
        raise ValueError("Dataset must be provided.")

    filtered_time = _select_time(dataset, request)

    return RawWeatherDataset(
        request=request,
        downloaded_at=datetime.now(timezone.utc),
        raw_data=filtered_time,
    )


def open_dataset(request: WeatherRequest) -> xr.Dataset:
    """
    Open the source dataset once.
    """

    validate_request(
        request,
    )

    request = build_request(
        request,
    )

    logger.info(
        "Downloading dataset."
    )

    return _download_data(
        request,
    )


class ArcoExtractor(WeatherExtractor):
    """
    SOLID: Single Responsibility and Open/Closed.
    Provides an extraction implementation for ARCO datasets.
    """

    def __init__(self, dataset: xr.Dataset):
        self.dataset = dataset

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        return extract_data(request, self.dataset)


class CdsExtractor(WeatherExtractor):
    """Extraction implementation for CDS ERA5 datasets."""

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        validate_request(request)
        normalized_request = build_request(request)

        dataset = _download_cds(normalized_request)
        dataset = _select_time(dataset, normalized_request)
        dataset = dataset[normalized_request.variables]

        return RawWeatherDataset(
            request=normalized_request,
            downloaded_at=datetime.now(timezone.utc),
            raw_data=dataset,
        )
