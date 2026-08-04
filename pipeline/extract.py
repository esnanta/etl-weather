from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from calendar import monthrange
from datetime import datetime
from datetime import timezone
from enum import Enum

import pandas as pd
import aiohttp
import xarray as xr
import logging

from pipeline.logging import log_step

logger = logging.getLogger(__name__)


ERA5_SINGLE_LEVELS_VARIABLES = {
    "total_precipitation",
    "surface_solar_radiation_downwards",
    "t2m",
    "u10",
    "v10",
    "swh",
    "msl",
}


SUPPORTED_DATASETS = {
    "era5-single-levels": ERA5_SINGLE_LEVELS_VARIABLES,
}


ERA5_SINGLE_LEVELS_URL = (
    "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"
)

class DataSource(Enum):
    ARCO = "arco"
    CDS = "cds"


@dataclass(frozen=True)
class BoundingBox:
    """
    Geographic bounding box.
    """

    north_lat: float
    west_lon: float
    south_lat: float
    east_lon: float

@dataclass(frozen=True)
class WeatherRequest:
    """
    Weather extraction configuration.
    """
    source: DataSource
    dataset: str
    start_date: str
    end_date: str
    variables: list[str]
    bbox: BoundingBox


@dataclass(frozen=True)
class RawWeatherDataset:
    """
    Raw dataset immediately after extraction.

    The dataset is still a lazy xarray.Dataset.
    Materialization is delegated to the storage layer
    when the dataset is written to NetCDF.
    """

    request: WeatherRequest
    downloaded_at: datetime
    raw_data: xr.Dataset


@dataclass(frozen=True)
class LoadedWeatherDataset:
    """
    Raw dataset after being reloaded from a Parquet checkpoint.

    At this stage the data is represented as a Pandas DataFrame.
    """

    request: WeatherRequest
    downloaded_at: datetime
    raw_data: pd.DataFrame


def validate_request(
    request: WeatherRequest,
) -> None:
    """
    Validate a WeatherRequest.
    """

    if not isinstance(
        request.source,
        DataSource,
    ):
        raise ValueError(
            "Invalid data source."
        )

    if not request.dataset.strip():
        raise ValueError(
            "Dataset must not be empty."
        )

    if not request.variables:
        raise ValueError(
            "At least one variable "
            "must be provided."
        )

    if not request.start_date.strip():
        raise ValueError(
            "Start date must not be empty."
        )

    if not request.end_date.strip():
        raise ValueError(
            "End date must not be empty."
        )

    try:
        start_date = datetime.strptime(
            request.start_date,
            "%Y-%m-%d",
        )

        end_date = datetime.strptime(
            request.end_date,
            "%Y-%m-%d",
        )

    except ValueError:
        raise ValueError(
            "Date format must be "
            "YYYY-MM-DD."
        )

    if start_date > end_date:
        raise ValueError(
            "Start date must be "
            "before end date."
        )

    bbox = request.bbox

    if not (
        -90 <= bbox.south_lat <= 90
    ):
        raise ValueError(
            "Invalid south latitude."
        )

    if not (
        -90 <= bbox.north_lat <= 90
    ):
        raise ValueError(
            "Invalid north latitude."
        )

    if not (
        -180 <= bbox.west_lon <= 180
    ):
        raise ValueError(
            "Invalid west longitude."
        )

    if not (
        -180 <= bbox.east_lon <= 180
    ):
        raise ValueError(
            "Invalid east longitude."
        )

    if (
        bbox.south_lat
        > bbox.north_lat
    ):
        raise ValueError(
            "South latitude must be "
            "less than or equal to "
            "north latitude."
        )

    if (
        bbox.west_lon
        > bbox.east_lon
    ):
        raise ValueError(
            "West longitude must be "
            "less than or equal to "
            "east longitude."
        )


def build_request(
    request: WeatherRequest,
) -> WeatherRequest:
    """
    Build a normalized WeatherRequest.
    """

    dataset = (
        request.dataset
        .strip()
        .lower()
    )

    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(
            f"Unsupported dataset: {dataset}"
        )

    variables = [
        variable.strip().lower()
        for variable in request.variables
        if variable.strip()
    ]

    if not variables:
        raise ValueError(
            "At least one variable "
            "must be provided."
        )

    supported_variables = (
        SUPPORTED_DATASETS[
            dataset
        ]
    )

    for variable in variables:
        if variable not in supported_variables:
            raise ValueError(
                f"Variable '{variable}' "
                f"is not supported by "
                f"dataset '{dataset}'."
            )

    return WeatherRequest(
        source=request.source,
        dataset=dataset,
        start_date=request.start_date,
        end_date=request.end_date,
        variables=variables,
        bbox=request.bbox,
    )


@log_step
def _download_arco(
    request: WeatherRequest,
) -> xr.Dataset:
    """
    Open the ARCO ERA5 analysis-ready dataset.
    """

    logger.info(
        "Opening %s.",
        request.dataset,
    )

    # ---------------------------------------------------------
    # [REVISI] Cara aman membatasi koneksi tanpa memicu error Event Loop
    # ---------------------------------------------------------
    custom_storage_options = {
        "token": "anon",
        # Gunakan parameter bawaan 'connections' milik gcsfs (defaultnya 100)
        # Ini menggantikan aiohttp.TCPConnector(limit=10)
        "connections": 10,
        "client_kwargs": {
            # ClientTimeout hanyalah objek konfigurasi, jadi aman dipanggil di sini
            "timeout": aiohttp.ClientTimeout(total=1200)
        }
    }

    dataset = xr.open_zarr(
        ERA5_SINGLE_LEVELS_URL,
        chunks=None,
        storage_options=custom_storage_options,
    )

    logger.info(
        "Successfully opened ARCO dataset."
    )

    min_time = str(
        dataset.time.min().values
    )[:10]

    max_time = str(
        dataset.time.max().values
    )[:10]

    logger.info(
        "[METRIC] Server period | %s -> %s",
        min_time,
        max_time,
    )

    logger.info(
        "[METRIC] Available variables=%d",
        len(dataset.data_vars),
    )

    if (
        request.start_date > max_time
        or request.end_date < min_time
    ):
        logger.warning(
            "Requested period is outside "
            "the available dataset range."
        )

    return dataset


@log_step
def _download_data(
    request: WeatherRequest,
) -> xr.Dataset:
    """
    Download a weather dataset
    from the configured source.
    """

    if request.source is DataSource.ARCO:
        return _download_arco(
            request,
        )

    if request.source is DataSource.CDS:
        raise NotImplementedError(
            "CDS data source is not "
            "implemented yet."
        )

    raise ValueError(
        f"Unsupported data source: "
        f"{request.source}"
    )

@log_step
def _select_time(
    dataset: xr.Dataset,
    request: WeatherRequest,
) -> xr.Dataset:
    """
    Select the requested time range.
    """

    return dataset.sel(
        time=slice(
            request.start_date,
            request.end_date,
        ),
    )


@log_step
def _select_bbox(
    dataset: xr.Dataset,
    request: WeatherRequest,
) -> xr.Dataset:
    """
    Select the requested geographic bounding box.

    ARCO Analysis Ready stores latitude and longitude
    as independent dimensions, therefore spatial
    subsetting is performed using xarray.sel().
    """

    bbox = request.bbox

    logger.info(
        "[METRIC] Bounding box | "
        "lat=%.2f..%.2f | "
        "lon=%.2f..%.2f",
        bbox.south_lat,
        bbox.north_lat,
        bbox.west_lon,
        bbox.east_lon,
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

    logger.info(
        "[METRIC] Spatial subset | "
        "latitude=%d | "
        "longitude=%d",
        subset.sizes["latitude"],
        subset.sizes["longitude"],
    )

    return subset

@log_step
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

def split_request_by_month(
    request: WeatherRequest,
) -> list[WeatherRequest]:
    """
    Split a WeatherRequest into
    monthly WeatherRequests.
    """

    start_date = datetime.strptime(
        request.start_date,
        "%Y-%m-%d",
    ).date()

    end_date = datetime.strptime(
        request.end_date,
        "%Y-%m-%d",
    ).date()

    requests: list[
        WeatherRequest
    ] = []

    year = start_date.year
    month = start_date.month

    while True:

        month_start = datetime(
            year,
            month,
            1,
        ).date()

        last_day = monthrange(
            year,
            month,
        )[1]

        month_end = datetime(
            year,
            month,
            last_day,
        ).date()

        period_start = max(
            start_date,
            month_start,
        )

        period_end = min(
            end_date,
            month_end,
        )

        requests.append(
            replace(
                request,
                start_date=(
                    period_start.isoformat()
                ),
                end_date=(
                    period_end.isoformat()
                ),
            )
        )

        if (
            period_end
            >= end_date
        ):
            break

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    return requests

def open_dataset(
    request: WeatherRequest,
) -> xr.Dataset:
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
