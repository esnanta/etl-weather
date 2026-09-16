from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import xarray as xr

from pipeline.logging import log_metric
from pipeline.model import RawWeatherDataset
from pipeline.transform import (
    ERA5_LAND_DATASET,
    LOCAL_MIDNIGHT_IN_UTC_HOUR,
    LOCAL_UTC_OFFSET_HOURS,
    TransformedWeatherDataset,
)

logger = logging.getLogger(__name__)


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

    _log_dataframe_validation_result(
        dataframe,
        dataset_name=transformed_dataset.request.dataset,
        requested_variables=transformed_dataset.request.variables,
    )
