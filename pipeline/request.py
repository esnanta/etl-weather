from __future__ import annotations

import hashlib
import json
from calendar import monthrange
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import Any

from pipeline.model import BoundingBox, DataSource, WeatherRequest
from pipeline.providers.cds import get_source_dataset_profile


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


def weather_request_to_spec(request: WeatherRequest) -> dict[str, object]:
    """Serialize a validated request into small JSON-safe control metadata."""
    validate_request(request)
    normalized = build_request(request)
    return {
        "source": normalized.source.value,
        "dataset": normalized.dataset,
        "start_date": normalized.start_date,
        "end_date": normalized.end_date,
        "variables": normalized.variables.copy(),
        "bbox": {
            "north_lat": normalized.bbox.north_lat,
            "west_lon": normalized.bbox.west_lon,
            "south_lat": normalized.bbox.south_lat,
            "east_lon": normalized.bbox.east_lon,
        },
    }


def weather_request_from_spec(spec: Mapping[str, Any]) -> WeatherRequest:
    """Rebuild and validate a request from JSON-safe control metadata."""
    if not isinstance(spec, Mapping):
        raise ValueError("Weather request specification must be a mapping.")

    try:
        source_value = spec["source"]
        dataset = spec["dataset"]
        start_date = spec["start_date"]
        end_date = spec["end_date"]
        variables = spec["variables"]
        bbox_spec = spec["bbox"]
    except KeyError as error:
        raise ValueError(
            f"Weather request specification is missing '{error.args[0]}'."
        ) from error

    if not all(isinstance(value, str) for value in (source_value, dataset, start_date, end_date)):
        raise ValueError("Weather request text fields must be strings.")
    if not isinstance(variables, list) or not all(
        isinstance(variable, str) for variable in variables
    ):
        raise ValueError("Weather request variables must be a list of strings.")
    if not isinstance(bbox_spec, Mapping):
        raise ValueError("Weather request bbox must be a mapping.")

    coordinate_names = ("north_lat", "west_lon", "south_lat", "east_lon")
    try:
        coordinates = {name: bbox_spec[name] for name in coordinate_names}
    except KeyError as error:
        raise ValueError(
            f"Weather request bbox is missing '{error.args[0]}'."
        ) from error
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        for value in coordinates.values()
    ):
        raise ValueError("Weather request bbox coordinates must be numeric.")

    try:
        source = DataSource(source_value)
    except ValueError as error:
        raise ValueError(f"Unsupported data source '{source_value}'.") from error

    request = WeatherRequest(
        source=source,
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        variables=variables.copy(),
        bbox=BoundingBox(**coordinates),
    )
    validate_request(request)
    return build_request(request)


def _next_day(value: date) -> date:
    """Return the calendar day immediately after value."""
    return date.fromordinal(value.toordinal() + 1)


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
        current_start = _next_day(period_end)
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
        current_start = _next_day(period_end)
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
