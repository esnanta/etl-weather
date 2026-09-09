from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone

import aiohttp
import xarray as xr

from pipeline.interfaces import WeatherExtractor
from pipeline.logging import log_metric
from pipeline.model import WeatherRequest, DataSource, RawWeatherDataset
from pipeline.utils import (
    build_request,
    get_source_dataset_profile,
    validate_request,
)

logger = logging.getLogger(__name__)


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
    profile = get_source_dataset_profile(request.source, request.dataset)

    # Limit remote connections without relying on a custom event-loop connector.
    custom_storage_options = {
        "token": "anon",
        # Use the built-in gcsfs connection limit instead of a custom connector.
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

    logger.info(
        "Successfully opened ARCO dataset."
    )

    min_time = str(
        dataset.time.min().values
    )[:10]

    max_time = str(
        dataset.time.max().values
    )[:10]

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

    if (
        request.start_date > max_time
        or request.end_date < min_time
    ):
        logger.warning(
            "Requested period is outside "
            "the available dataset range."
        )

    return dataset


DOWNLOAD_STRATEGIES = {
    DataSource.ARCO: _download_arco,
}


def _download_data(
    request: WeatherRequest,
) -> xr.Dataset:
    """
    Download a weather dataset
    from the configured source using a strategy pattern.
    """
    strategy = DOWNLOAD_STRATEGIES.get(request.source)
    if not strategy:
        if request.source is DataSource.CDS:
            raise NotImplementedError(
                "CDS data source is not implemented yet."
            )
        raise ValueError(
            f"Unsupported data source: {request.source}"
        )
    return strategy(request)


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


class ArcoExtractor(WeatherExtractor):
    """
    SOLID: Single Responsibility and Open/Closed.
    Provides an extraction implementation for ARCO datasets.
    """
    def __init__(self, dataset: xr.Dataset):
        self.dataset = dataset

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        return extract_data(request, self.dataset)
