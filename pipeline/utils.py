# pipeline/extract.py
from __future__ import annotations

import logging
from calendar import monthrange
from dataclasses import replace
from datetime import datetime

from pipeline.model import WeatherRequest

logger = logging.getLogger(__name__)

# =============================================================================
# DOMAIN CONFIGURATIONS
# =============================================================================

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



def build_request(request: WeatherRequest) -> WeatherRequest:
    """Build a normalized WeatherRequest."""
    dataset = request.dataset.strip().lower()

    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    variables = [
        variable.strip().lower()
        for variable in request.variables
        if variable.strip()
    ]

    if not variables:
        raise ValueError("At least one variable must be provided.")

    supported_variables = SUPPORTED_DATASETS[dataset]
    for variable in variables:
        if variable not in supported_variables:
            raise ValueError(
                f"Variable '{variable}' is not supported by dataset '{dataset}'."
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