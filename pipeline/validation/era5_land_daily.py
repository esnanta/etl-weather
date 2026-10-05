"""Validate transformed ERA5-Land local-daily DataFrames."""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from pipeline.contracts.era5_land import (
    ERA5_LAND_DATASET,
    LOCAL_MIDNIGHT_IN_UTC_HOUR,
    LOCAL_UTC_OFFSET_HOURS,
)
from pipeline.logging import log_metric
from pipeline.model import TransformedWeatherDataset, WeatherRequest
from pipeline.providers.catalog import get_source_dataset_profile
from pipeline.validation.era5_land_raw import ValidationError

ERA5_LAND_TRANSFORMED_HARD_RANGES = {
    "t2m": (-273.15, None),
    "d2m": (-273.15, None),
    "swvl1": (0.0, 1.0),
}

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
        local_day_timestamp = pd.Timestamp(cast(Any, local_day))
        grid_by_day[local_day_timestamp] = set(
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
        variable_series = cast(pd.Series, dataframe[variable])
        values = cast(
            pd.Series,
            pd.to_numeric(variable_series, errors="coerce"),
        )
        non_numeric = variable_series.notna() & values.isna()
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
