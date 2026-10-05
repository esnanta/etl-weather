"""Validate RAW ERA5-Land hourly datasets and source-grid continuity."""

from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from pipeline.observability.logging import log_metric
from pipeline.models.weather import RawWeatherDataset, WeatherRequest
from pipeline.providers.catalog import get_source_dataset_profile


class ValidationError(Exception):
    """Raised when dataset validation fails."""


def _validate_dataset_type(dataset: xr.Dataset) -> None:
    """Validate xarray dataset type."""
    if not isinstance(dataset, xr.Dataset):
        raise ValidationError(f"Expected xarray.Dataset, got {type(dataset)}")

def _validate_dataset_structure(dataset: xr.Dataset) -> None:
    """Validate required coordinates."""
    required_coords = {"valid_time", "latitude", "longitude"}
    missing = required_coords - set(dataset.coords)
    if missing:
        raise ValidationError(f"Missing required coordinates: {missing}")

def _validate_request_variables(dataset: xr.Dataset, variables: list[str]) -> None:
    """Validate requested variables exist."""
    missing = set(variables) - set(dataset.data_vars)
    if missing:
        raise ValidationError(f"Missing requested variables: {missing}")

def _validate_time_dimension(dataset: xr.Dataset) -> None:
    """Validate canonical valid_time coordinate."""
    if "valid_time" not in dataset.coords:
        raise ValidationError("valid_time coordinate is missing.")

    if not np.issubdtype(dataset.valid_time.dtype, np.datetime64):
        raise ValidationError("valid_time coordinate must be datetime64.")

def _validate_raw_temporal_continuity(dataset: xr.Dataset) -> None:
    """Validate that RAW valid_time is unique, ordered, and hourly."""
    valid_time = pd.DatetimeIndex(pd.to_datetime(dataset["valid_time"].values))

    if valid_time.empty:
        raise ValidationError("RAW valid_time contains no timestamps.")

    if valid_time.has_duplicates:
        raise ValidationError("RAW valid_time contains duplicate timestamps.")

    if not valid_time.is_monotonic_increasing:
        raise ValidationError("RAW valid_time must be monotonically increasing.")

    expected_interval = pd.Timedelta(hours=1)
    if len(valid_time) > 1:
        intervals = pd.Series(valid_time[1:] - valid_time[:-1])
        invalid_intervals = intervals.ne(expected_interval)
        if bool(invalid_intervals.any()):
            raise ValidationError(
                "RAW valid_time must have a continuous 1-hour interval."
            )

    expected_steps = int(
        ((valid_time[-1] - valid_time[0]) / expected_interval) + 1
    )
    missing_steps = expected_steps - len(valid_time)

    log_metric(
        "RAW Temporal Validation PASSED",
        {
            "valid_time_start": valid_time[0],
            "valid_time_end": valid_time[-1],
            "unique_time_steps": len(valid_time),
            "expected_time_steps": expected_steps,
            "time_interval": expected_interval,
            "missing_time_steps": missing_steps,
        },
    )

def _validate_coordinate_axis(
    values: np.ndarray,
    *,
    axis_name: str,
    resolution: float,
) -> None:
    """Validate uniqueness, ordering, and regular spacing of one grid axis."""
    numeric_values = np.asarray(values, dtype=float)

    if numeric_values.size == 0:
        raise ValidationError(f"RAW {axis_name} coordinate is empty.")

    if np.unique(numeric_values).size != numeric_values.size:
        raise ValidationError(f"RAW {axis_name} contains duplicate coordinates.")

    if numeric_values.size == 1:
        return

    differences = np.diff(numeric_values)
    is_ascending = bool(np.all(differences > 0))
    is_descending = bool(np.all(differences < 0))
    if not (is_ascending or is_descending):
        raise ValidationError(
            f"RAW {axis_name} coordinates must be monotonic."
        )

    if not np.allclose(
        np.abs(differences),
        resolution,
        rtol=0,
        atol=1e-6,
    ):
        raise ValidationError(
            f"RAW {axis_name} spacing must be {resolution} degrees."
        )

def _expected_axis_count(start: float, end: float, resolution: float) -> int:
    """Return expected inclusive coordinate count for an aligned regular grid."""
    return int(round(abs(end - start) / resolution)) + 1

def _validate_raw_spatial_grid(
    dataset: xr.Dataset,
    request: WeatherRequest,
) -> None:
    """Validate RAW spatial extent and regular source-grid resolution."""
    profile = get_source_dataset_profile(request.source, request.dataset)
    resolution = float(profile["horizontal_resolution_degrees"])

    latitude = np.asarray(dataset["latitude"].values, dtype=float)
    longitude = np.asarray(dataset["longitude"].values, dtype=float)

    _validate_coordinate_axis(
        latitude,
        axis_name="latitude",
        resolution=resolution,
    )
    _validate_coordinate_axis(
        longitude,
        axis_name="longitude",
        resolution=resolution,
    )

    bbox = request.bbox
    tolerance = 1e-6
    if latitude.min() < bbox.south_lat - tolerance:
        raise ValidationError("RAW latitude extends south of requested bbox.")
    if latitude.max() > bbox.north_lat + tolerance:
        raise ValidationError("RAW latitude extends north of requested bbox.")
    if longitude.min() < bbox.west_lon - tolerance:
        raise ValidationError("RAW longitude extends west of requested bbox.")
    if longitude.max() > bbox.east_lon + tolerance:
        raise ValidationError("RAW longitude extends east of requested bbox.")

    expected_latitudes = _expected_axis_count(
        bbox.south_lat,
        bbox.north_lat,
        resolution,
    )
    expected_longitudes = _expected_axis_count(
        bbox.west_lon,
        bbox.east_lon,
        resolution,
    )

    if len(latitude) != expected_latitudes:
        raise ValidationError(
            "RAW latitude count does not match requested bbox and source resolution."
        )
    if len(longitude) != expected_longitudes:
        raise ValidationError(
            "RAW longitude count does not match requested bbox and source resolution."
        )

    log_metric(
        "RAW Spatial Validation PASSED",
        {
            "horizontal_resolution_degrees": resolution,
            "latitude_min": float(latitude.min()),
            "latitude_max": float(latitude.max()),
            "latitude_count": len(latitude),
            "expected_latitude_count": expected_latitudes,
            "longitude_min": float(longitude.min()),
            "longitude_max": float(longitude.max()),
            "longitude_count": len(longitude),
            "expected_longitude_count": expected_longitudes,
            "grid_points": len(latitude) * len(longitude),
        },
    )

def _record_count(dataset: xr.Dataset) -> int:
    """Return logical grid observation count."""
    return (
        dataset.sizes.get("valid_time", 0)
        * dataset.sizes.get("latitude", 1)
        * dataset.sizes.get("longitude", 1)
    )

def _log_raw_validation_result(dataset: xr.Dataset, requested_variables: list[str]) -> None:
    """Log raw dataset validation."""
    required_coords = {"valid_time", "latitude", "longitude"}
    variables_requested = sorted(requested_variables)
    data_variables = {str(name) for name in dataset.data_vars}
    coordinate_names = {str(name) for name in dataset.coords}
    variables_in_subset = sorted(data_variables)
    variables_missing = sorted(set(variables_requested) - data_variables)
    coordinates_missing = sorted(required_coords - coordinate_names)
    has_valid_time = "valid_time" in dataset.coords
    time_dtype = str(dataset.valid_time.dtype) if has_valid_time else "missing"
    time_is_datetime64 = bool(
        has_valid_time
        and np.issubdtype(dataset.valid_time.dtype, np.datetime64)
    )

    log_metric(
        "RAW Validation PASSED",
        {
            "dataset_type": f"{type(dataset).__module__}.{type(dataset).__name__}",
            "coordinates_required": sorted(required_coords),
            "coordinates_missing": coordinates_missing,
            "variables_requested": variables_requested,
            "variables_in_subset": variables_in_subset,
            "variables_missing": variables_missing,
            "valid_time_dtype": time_dtype,
            "valid_time_is_datetime64": time_is_datetime64,
            "records": _record_count(dataset),
            "dimensions": dict(dataset.sizes),
        },
    )

def validate_raw_dataset(raw_dataset: RawWeatherDataset) -> None:
    """Validate raw xarray dataset."""
    if raw_dataset is None:
        raise ValidationError("RawWeatherDataset must not be None.")

    dataset = raw_dataset.raw_data

    _validate_dataset_type(dataset)
    _validate_dataset_structure(dataset)
    _validate_request_variables(dataset, raw_dataset.request.variables)
    _validate_time_dimension(dataset)
    _validate_raw_temporal_continuity(dataset)
    _validate_raw_spatial_grid(dataset, raw_dataset.request)
    _log_raw_validation_result(dataset, raw_dataset.request.variables)
