from __future__ import annotations

import logging
from calendar import monthrange
from dataclasses import replace
from datetime import datetime, timedelta
from calendar import monthrange
from pipeline.model import WeatherRequest, DataSource

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

VARIABLE_MAP = {
    "surface_solar_radiation_downwards": "ssrd",
    "total_precipitation": "tp",
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
            raise ValueError(f"Variable '{variable}' is not supported by dataset '{dataset}'.")

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
        # 1. Hitung tanggal 7 hari (current_start + 6 hari)
        week_end = current_start + timedelta(days=6)

        # 2. Hitung tanggal terakhir di bulan berjalan
        last_day_of_month = monthrange(current_start.year, current_start.month)[1]
        month_end = datetime(current_start.year, current_start.month, last_day_of_month).date()

        # 3. Ambil batas paling kecil antara: batas minggu (7 hari), batas akhir bulan, atau end_date request
        period_end = min(week_end, month_end, end_date)

        requests.append(
            replace(
                request,
                start_date=current_start.isoformat(),
                end_date=period_end.isoformat(),
            )
        )

        # 4. Pindah ke hari berikutnya untuk memulainya minggu/bulan baru
        current_start = period_end + timedelta(days=1)

    return requests

# =============================================================================
# STORAGE UTILITIES
# =============================================================================

def build_variable_signature(variables: list[str]) -> str:
    """Build a deterministic variable signature."""
    short_variables = [VARIABLE_MAP.get(var, var) for var in variables]
    return "_".join(sorted(short_variables))

def format_request_date(date: str) -> str:
    """Format request date for filenames."""
    return date.replace("-", "")

