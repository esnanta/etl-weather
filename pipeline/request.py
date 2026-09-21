from __future__ import annotations

import hashlib
import json
from calendar import monthrange
from dataclasses import replace
from datetime import datetime, timedelta

from pipeline.model import WeatherRequest
from pipeline.providers.cds import get_source_dataset_profile


def validate_request(request: WeatherRequest) -> None:
    """Validate a WeatherRequest."""
    from pipeline.model import DataSource

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
    """Build a normalized request while preserving provider variable names."""
    dataset = request.dataset.strip().lower()
    profile = get_source_dataset_profile(request.source, dataset)

    variables = [v.strip().lower() for v in request.variables if v.strip()]
    if not variables:
        raise ValueError("At least one variable must be provided.")
    if len(set(variables)) != len(variables):
        raise ValueError("Duplicate variables are not allowed.")

    supported = profile["supported_variables"]
    for variable in variables:
        if variable not in supported:
            raise ValueError(
                f"Variable '{variable}' is not supported by dataset '{dataset}' "
                f"for source '{request.source.value}'. Use the provider variable name."
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
    start_date = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    requests: list[WeatherRequest] = []
    current_start = start_date
    while current_start <= end_date:
        week_end = current_start + timedelta(days=6)
        last_day = monthrange(current_start.year, current_start.month)[1]
        month_end = datetime(current_start.year, current_start.month, last_day).date()
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


def build_variable_signature(variables: list[str]) -> str:
    """Build a deterministic signature using provider variable names."""
    return "_".join(sorted(v.strip().lower() for v in variables))


def build_request_key(request: WeatherRequest) -> str:
    """Build a stable logical partition key for retries and database upserts."""
    payload = {
        "source": request.source.value,
        "dataset": request.dataset.strip().lower(),
        "start_date": request.start_date,
        "end_date": request.end_date,
        "variables": sorted(v.strip().lower() for v in request.variables),
        "bbox": {
            "north_lat": request.bbox.north_lat,
            "south_lat": request.bbox.south_lat,
            "east_lon": request.bbox.east_lon,
            "west_lon": request.bbox.west_lon,
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def format_request_period(start_date: str, end_date: str) -> str:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return f"{start:%Y}_{start:%m%d}_{end:%m%d}"
