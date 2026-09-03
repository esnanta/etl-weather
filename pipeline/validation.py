from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import xarray as xr

from pipeline.model import RawWeatherDataset
from pipeline.logging import log_step
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
    required_coords = {"time", "latitude", "longitude"}
    missing = required_coords - set(dataset.coords)
    if missing:
        raise ValidationError(f"Missing required coordinates: {missing}")


def _validate_request_variables(dataset: xr.Dataset, variables: list[str]) -> None:
    """Validate requested variables exist."""
    missing = set(variables) - set(dataset.data_vars)
    if missing:
        raise ValidationError(f"Missing requested variables: {missing}")


def _validate_time_dimension(dataset: xr.Dataset) -> None:
    """Validate time coordinate."""
    if "time" not in dataset.coords:
        raise ValidationError("Time coordinate is missing.")

    if not np.issubdtype(dataset.time.dtype, np.datetime64):
        raise ValidationError("Time coordinate must be datetime64.")


def _record_count(dataset: xr.Dataset) -> int:
    """
    Return logical observation count.
    Count grid-based observations as time * latitude * longitude.
    """
    return (
        dataset.sizes.get("time", 0)
        * dataset.sizes.get("latitude", 1)
        * dataset.sizes.get("longitude", 1)
    )


def _validate_dataframe_type(dataframe: pd.DataFrame) -> None:
    """Validate DataFrame type."""
    if not isinstance(dataframe, pd.DataFrame):
        raise ValidationError(f"Expected pandas.DataFrame, got {type(dataframe)}")


def _validate_dataframe_columns(dataframe: pd.DataFrame, variables: list[str]) -> None:
    """
    Validate required DataFrame columns.
    Require time, latitude, longitude, and the requested variables.
    """
    required_columns = {"time", "latitude", "longitude"}
    required_columns.update(variables)

    missing = required_columns - set(dataframe.columns)
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


def _log_raw_validation_result(dataset: xr.Dataset) -> None:
    """Log raw dataset validation."""
    logger.info(
        "[METRIC] RAW Validation PASSED | "
        "records=%d | "
        "variables=%s | "
        "dimensions=%s",
        _record_count(dataset),
        list(dataset.data_vars),
        dict(dataset.sizes),
    )


def _log_dataframe_validation_result(dataframe: pd.DataFrame) -> None:
    """Log DataFrame validation."""
    logger.info(
        "[METRIC] DataFrame Validation PASSED | "
        "rows=%d | "
        "cols=%d",
        len(dataframe),
        len(dataframe.columns),
    )


@log_step
def validate_raw_dataset(raw_dataset: RawWeatherDataset) -> None:
    """Validate raw xarray dataset."""
    _log_validation_plan(
        target="Raw Dataset",
        checks=["dataset_type", "coordinates", "variables", "time_dimension"],
    )

    dataset = raw_dataset.raw_data

    _validate_dataset_type(dataset)
    _validate_dataset_structure(dataset)
    _validate_request_variables(dataset, raw_dataset.request.variables)
    _validate_time_dimension(dataset)
    _log_raw_validation_result(dataset)


@log_step
def validate_transformed_dataset(transformed_dataset: TransformedWeatherDataset) -> None:
    """Validate transformed DataFrame."""
    _log_validation_plan(
        target="Transformed DataFrame",
        checks=["dataframe_type", "required_columns"],
    )

    dataframe = transformed_dataset.dataset

    _validate_dataframe_type(dataframe)
    _validate_dataframe_columns(dataframe, transformed_dataset.request.variables)
    _log_dataframe_validation_result(dataframe)


def _log_validation_plan(*, target: str, checks: list[str]) -> None:
    """Log validation checklist."""
    logger.info("[METRIC] Validating %s | checks=%s", target, checks)
