from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import xarray as xr

from pipeline.logging import log_metric
from pipeline.model import RawWeatherDataset, WeatherRequest
from pipeline.transform import (
    ERA5_LAND_DATASET,
    LOCAL_MIDNIGHT_IN_UTC_HOUR,
    LOCAL_UTC_OFFSET_HOURS,
    TransformedWeatherDataset,
)
from pipeline.providers.cds import get_source_dataset_profile

logger = logging.getLogger(__name__)

ERA5_LAND_TRANSFORMED_HARD_RANGES = {
    "t2m": (-273.15, None),
    "d2m": (-273.15, None),
    "swvl1": (0.0, 1.0),
}


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


def _validate_dataframe_type(dataframe: pd.DataFrame) -> None:
    """Validate DataFrame type."""
    if not isinstance(dataframe, pd.DataFrame):
        raise ValidationError(f"Expected pandas.DataFrame, got {type(dataframe)}")


def _required_dataframe_columns(
    *,
    dataset_name: str,
    variables: list[str],
) -> list[str]:
    required_columns = ["valid_time"]
    if dataset_name == ERA5_LAND_DATASET:
        required_columns.append("valid_time_local")
    required_columns.extend(["latitude", "longitude", *variables])
    return required_columns


def _validate_dataframe_columns(
    dataframe: pd.DataFrame,
    *,
    dataset_name: str,
    variables: list[str],
) -> None:
    """Validate required canonical DataFrame columns."""
    required_columns = set(
        _required_dataframe_columns(
            dataset_name=dataset_name,
            variables=variables,
        )
    )

    missing = required_columns - set(dataframe.columns)
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


def _validate_era5_land_transformed_time(
    dataframe: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> None:
    """Validate UTC/local daily temporal contract for ERA5-Land output."""
    valid_time = pd.to_datetime(dataframe["valid_time"])
    valid_time_local = pd.to_datetime(dataframe["valid_time_local"])

    expected_offset = pd.Timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
    if not ((valid_time_local - valid_time) == expected_offset).all():
        raise ValidationError(
            "valid_time_local must equal valid_time + UTC+7."
        )

    if not (
        valid_time_local.dt.hour.eq(0)
        & valid_time_local.dt.minute.eq(0)
        & valid_time_local.dt.second.eq(0)
    ).all():
        raise ValidationError(
            "valid_time_local must represent local midnight."
        )

    if not valid_time.dt.hour.eq(LOCAL_MIDNIGHT_IN_UTC_HOUR).all():
        raise ValidationError(
            "valid_time must represent the UTC instant of local midnight."
        )

    duplicate_keys = dataframe.duplicated(
        subset=["valid_time_local", "latitude", "longitude"]
    )
    if bool(duplicate_keys.any()):
        raise ValidationError(
            "Duplicate ERA5-Land daily grid records detected."
        )

    unique_local_days = pd.DatetimeIndex(
        sorted(valid_time_local.drop_duplicates())
    )
    expected_local_days = pd.date_range(
        start=pd.Timestamp(start_date),
        end=pd.Timestamp(end_date),
        freq="D",
    )

    if not unique_local_days.equals(expected_local_days):
        raise ValidationError(
            "ERA5-Land transformed data does not cover every requested local day."
        )

    log_metric(
        "ERA5-Land Temporal Validation PASSED",
        {
            "valid_time_utc_start": valid_time.min(),
            "valid_time_utc_end": valid_time.max(),
            "valid_time_local_start": valid_time_local.min(),
            "valid_time_local_end": valid_time_local.max(),
            "utc_to_local_offset": expected_offset,
            "local_midnight_in_utc": f"{LOCAL_MIDNIGHT_IN_UTC_HOUR:02d}:00",
            "local_days": len(unique_local_days),
            "expected_local_days": len(expected_local_days),
            "duplicate_grid_records": int(duplicate_keys.sum()),
        },
    )


def _coordinates_align_to_resolution(
    values: pd.Series,
    *,
    origin: float,
    resolution: float,
) -> bool:
    """Return whether coordinates lie on the expected regular-grid lattice."""
    steps = (values.astype(float) - origin) / resolution
    return bool(np.allclose(steps, np.round(steps), rtol=0, atol=1e-6))


def _validate_era5_land_transformed_space(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> None:
    """Validate spatial bounds and stable land-grid membership for each day."""
    profile = get_source_dataset_profile(request.source, request.dataset)
    resolution = float(profile["horizontal_resolution_degrees"])
    bbox = request.bbox
    tolerance = 1e-6

    latitude = dataframe["latitude"].astype(float)
    longitude = dataframe["longitude"].astype(float)

    if latitude.isna().any() or longitude.isna().any():
        raise ValidationError("Transformed spatial coordinates must not be null.")

    if latitude.min() < bbox.south_lat - tolerance:
        raise ValidationError("Transformed latitude extends south of requested bbox.")
    if latitude.max() > bbox.north_lat + tolerance:
        raise ValidationError("Transformed latitude extends north of requested bbox.")
    if longitude.min() < bbox.west_lon - tolerance:
        raise ValidationError("Transformed longitude extends west of requested bbox.")
    if longitude.max() > bbox.east_lon + tolerance:
        raise ValidationError("Transformed longitude extends east of requested bbox.")

    if not _coordinates_align_to_resolution(
        latitude,
        origin=bbox.south_lat,
        resolution=resolution,
    ):
        raise ValidationError(
            "Transformed latitude coordinates do not align with source resolution."
        )
    if not _coordinates_align_to_resolution(
        longitude,
        origin=bbox.west_lon,
        resolution=resolution,
    ):
        raise ValidationError(
            "Transformed longitude coordinates do not align with source resolution."
        )

    grid_by_day: dict[pd.Timestamp, set[tuple[float, float]]] = {}
    for local_day, daily_frame in dataframe.groupby("valid_time_local", sort=True):
        grid_by_day[pd.Timestamp(local_day)] = set(
            zip(
                daily_frame["latitude"].astype(float),
                daily_frame["longitude"].astype(float),
                strict=True,
            )
        )

    if not grid_by_day:
        raise ValidationError("Transformed dataset contains no spatial grid records.")

    first_day = next(iter(grid_by_day))
    reference_grid = grid_by_day[first_day]
    for local_day, grid in grid_by_day.items():
        if grid != reference_grid:
            raise ValidationError(
                "ERA5-Land transformed spatial grid must be identical for every local day."
            )

    records_per_day = [len(grid) for grid in grid_by_day.values()]

    log_metric(
        "ERA5-Land Spatial Validation PASSED",
        {
            "horizontal_resolution_degrees": resolution,
            "latitude_min": float(latitude.min()),
            "latitude_max": float(latitude.max()),
            "longitude_min": float(longitude.min()),
            "longitude_max": float(longitude.max()),
            "local_days": len(grid_by_day),
            "valid_land_grid_cells": len(reference_grid),
            "grid_cells_per_day_min": min(records_per_day),
            "grid_cells_per_day_max": max(records_per_day),
            "grid_membership_stable": True,
        },
    )


def _validate_era5_land_transformed_values(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> None:
    """Validate finite values and only scientifically hard physical bounds."""
    for variable in request.variables:
        values = pd.to_numeric(dataframe[variable], errors="coerce")
        non_numeric = dataframe[variable].notna() & values.isna()
        if bool(non_numeric.any()):
            raise ValidationError(
                f"ERA5-Land variable '{variable}' must be numeric or missing."
            )

        observed = values.dropna()
        if not np.isfinite(observed.to_numpy(dtype=float)).all():
            raise ValidationError(
                f"ERA5-Land variable '{variable}' contains non-finite values."
            )

        bounds = ERA5_LAND_TRANSFORMED_HARD_RANGES.get(variable)
        if bounds is None:
            continue

        minimum, maximum = bounds
        if minimum is not None and bool(observed.lt(minimum).any()):
            raise ValidationError(
                f"ERA5-Land variable '{variable}' must be >= {minimum}."
            )
        if maximum is not None and bool(observed.gt(maximum).any()):
            raise ValidationError(
                f"ERA5-Land variable '{variable}' must be <= {maximum}."
            )


def _log_raw_validation_result(dataset: xr.Dataset, requested_variables: list[str]) -> None:
    """Log raw dataset validation."""
    required_coords = {"valid_time", "latitude", "longitude"}
    variables_requested = sorted(requested_variables)
    variables_in_subset = sorted(list(dataset.data_vars))
    variables_missing = sorted(set(variables_requested) - set(dataset.data_vars))
    coordinates_missing = sorted(required_coords - set(dataset.coords))
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


def _log_dataframe_validation_result(
    dataframe: pd.DataFrame,
    *,
    dataset_name: str,
    requested_variables: list[str],
) -> None:
    """Log DataFrame validation."""
    required_columns = _required_dataframe_columns(
        dataset_name=dataset_name,
        variables=requested_variables,
    )
    columns_missing = sorted(set(required_columns) - set(dataframe.columns))

    log_metric(
        "DataFrame Validation PASSED",
        {
            "dataframe_type": f"{type(dataframe).__module__}.{type(dataframe).__name__}",
            "columns_required": required_columns,
            "columns_missing": columns_missing,
            "rows": len(dataframe),
            "cols": len(dataframe.columns),
            "columns": list(dataframe.columns),
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


def validate_transformed_dataset(transformed_dataset: TransformedWeatherDataset) -> None:
    """Validate transformed DataFrame."""
    if transformed_dataset is None:
        raise ValidationError("TransformedWeatherDataset must not be None.")

    dataframe = transformed_dataset.dataset

    _validate_dataframe_type(dataframe)
    _validate_dataframe_columns(
        dataframe,
        dataset_name=transformed_dataset.request.dataset,
        variables=transformed_dataset.request.variables,
    )

    if transformed_dataset.request.dataset == ERA5_LAND_DATASET:
        _validate_era5_land_transformed_time(
            dataframe,
            start_date=transformed_dataset.request.start_date,
            end_date=transformed_dataset.request.end_date,
        )
        _validate_era5_land_transformed_space(
            dataframe,
            transformed_dataset.request,
        )
        _validate_era5_land_transformed_values(
            dataframe,
            transformed_dataset.request,
        )

    _log_dataframe_validation_result(
        dataframe,
        dataset_name=transformed_dataset.request.dataset,
        requested_variables=transformed_dataset.request.variables,
    )
