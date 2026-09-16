from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import xarray as xr

from pipeline.model import RawWeatherDataset
from pipeline.logging import log_metric
from pipeline.transform import TransformedWeatherDataset

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


def _validate_dataframe_columns(dataframe: pd.DataFrame, variables: list[str]) -> None:
    """Validate required canonical DataFrame columns."""
    required_columns = {"valid_time", "latitude", "longitude"}
    required_columns.update(variables)

    missing = required_columns - set(dataframe.columns)
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


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


def _log_dataframe_validation_result(dataframe: pd.DataFrame, requested_variables: list[str]) -> None:
    """Log DataFrame validation."""
    required_columns = ["valid_time", "latitude", "longitude", *requested_variables]
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
    dataset = raw_dataset.raw_data

    _validate_dataset_type(dataset)
    _validate_dataset_structure(dataset)
    _validate_request_variables(dataset, raw_dataset.request.variables)
    _validate_time_dimension(dataset)
    _log_raw_validation_result(dataset, raw_dataset.request.variables)


def validate_transformed_dataset(transformed_dataset: TransformedWeatherDataset) -> None:
    """Validate transformed DataFrame."""
    dataframe = transformed_dataset.dataset

    _validate_dataframe_type(dataframe)
    _validate_dataframe_columns(dataframe, transformed_dataset.request.variables)
    _log_dataframe_validation_result(dataframe, transformed_dataset.request.variables)
