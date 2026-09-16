from __future__ import annotations

import logging
from datetime import UTC, datetime

import pandas as pd

from pipeline.interfaces import DataTransformer
from pipeline.logging import (
    _log_dataframe_preview,
    log_dataframe_changes,
    log_dataframe_info,
    log_metric,
)
from pipeline.model import LoadedWeatherDataset, TransformedWeatherDataset, WeatherRequest

logger = logging.getLogger(__name__)

ERA5_LAND_DATASET = "reanalysis-era5-land"
LOCAL_TIMEZONE = "Asia/Jakarta"
LOCAL_UTC_OFFSET_HOURS = 7
EXPECTED_HOURLY_SAMPLES_PER_DAY = 24

ERA5_LAND_ACCUMULATED_VARIABLES = {
    "total_precipitation",
    "surface_solar_radiation_downwards",
}

ERA5_LAND_DAILY_MEAN_VARIABLES = {
    "2m_temperature",
    "2m_dewpoint_temperature",
    "volumetric_soil_water_layer_1",
}


# =============================================================================
# SHARED PREPARATION
# =============================================================================


def _select_observation_columns(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    """
    Keep only the canonical observation schema required downstream.

    Source metadata such as number, surface and expver is intentionally
    excluded rather than maintained through an ever-growing drop list.
    """
    required_columns = [
        "valid_time",
        "latitude",
        "longitude",
        *request.variables,
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError(
            "Transformation input is missing required columns: "
            f"{', '.join(missing_columns)}"
        )

    selected = dataframe.loc[:, required_columns].copy()
    selected["valid_time"] = pd.to_datetime(selected["valid_time"])

    return selected.sort_values(
        ["latitude", "longitude", "valid_time"]
    ).reset_index(drop=True)


# =============================================================================
# ERA5-LAND DAILY AGGREGATION
# =============================================================================


def _aggregate_instantaneous_variable(
    dataframe: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    """Aggregate an instantaneous ERA5-Land variable to a UTC+7 daily mean."""
    working = dataframe[
        ["valid_time", "latitude", "longitude", variable]
    ].copy()

    working["_local_day"] = (
        working["valid_time"]
        + pd.Timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
    ).dt.floor("D")

    group_columns = ["_local_day", "latitude", "longitude"]
    grouped = working.groupby(group_columns, sort=True)[variable]

    daily_mean = grouped.mean()
    sample_count = grouped.count()
    complete = sample_count == EXPECTED_HOURLY_SAMPLES_PER_DAY

    return (
        daily_mean[complete]
        .rename(variable)
        .reset_index()
    )


def _calculate_accumulated_hourly_increment(
    dataframe: pd.DataFrame,
    variable: str,
) -> pd.Series:
    """
    Convert ERA5-Land forecast-step accumulations into hourly increments.

    ERA5-Land accumulated variables build from 00 UTC through the forecast
    step. The validity time 00 UTC contains the completed 24-hour accumulation
    of the preceding UTC day, while 01 UTC starts a new accumulation cycle.
    """
    grouped = dataframe.groupby(
        ["latitude", "longitude"],
        sort=False,
    )[variable]

    increment = grouped.diff()

    # At 01 UTC a new accumulation cycle begins, so the cumulative value is
    # itself the 00-01 UTC increment rather than a difference from 00 UTC.
    reset_mask = dataframe["valid_time"].dt.hour.eq(1)
    increment.loc[reset_mask] = dataframe.loc[reset_mask, variable]

    negative_mask = increment < -1e-10
    negative_count = int(negative_mask.sum())
    if negative_count:
        logger.warning(
            "Detected %s negative hourly increments for accumulated variable '%s'; "
            "they will be clipped to zero.",
            negative_count,
            variable,
        )

    return increment.clip(lower=0)


def _aggregate_accumulated_variable(
    dataframe: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    """
    Aggregate an ERA5-Land accumulated variable to a UTC+7 local-day total.

    An accumulated value stamped at a given hour represents the interval ending
    at that timestamp. Therefore the effective grouping shift is UTC+6 rather
    than UTC+7, assigning an interval ending exactly at local midnight to the
    preceding local day.
    """
    working = dataframe[
        ["valid_time", "latitude", "longitude", variable]
    ].copy()

    working["_hourly_increment"] = _calculate_accumulated_hourly_increment(
        working,
        variable,
    )

    working["_local_day"] = (
        working["valid_time"]
        + pd.Timedelta(hours=LOCAL_UTC_OFFSET_HOURS - 1)
    ).dt.floor("D")

    group_columns = ["_local_day", "latitude", "longitude"]
    grouped = working.groupby(group_columns, sort=True)["_hourly_increment"]

    daily_sum = grouped.sum(min_count=EXPECTED_HOURLY_SAMPLES_PER_DAY)
    sample_count = grouped.count()
    complete = sample_count == EXPECTED_HOURLY_SAMPLES_PER_DAY

    return (
        daily_sum[complete]
        .rename(variable)
        .reset_index()
    )


def _filter_target_period(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    """Keep only local days belonging to the requested target period."""
    start = pd.Timestamp(request.start_date)
    end = pd.Timestamp(request.end_date)

    return dataframe.loc[
        dataframe["valid_time"].between(start, end)
    ].copy()


def _aggregate_era5_land_daily(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    """Build one complete UTC+7 daily row per ERA5-Land grid cell."""
    selected = _select_observation_columns(
        dataframe,
        request,
    )

    unsupported_variables = [
        variable
        for variable in request.variables
        if variable not in ERA5_LAND_ACCUMULATED_VARIABLES
        and variable not in ERA5_LAND_DAILY_MEAN_VARIABLES
    ]
    if unsupported_variables:
        raise ValueError(
            "No ERA5-Land daily aggregation rule is defined for: "
            f"{', '.join(unsupported_variables)}"
        )

    log_metric(
        "ERA5-Land daily aggregation",
        {
            "timezone": f"{LOCAL_TIMEZONE} (UTC+{LOCAL_UTC_OFFSET_HOURS:02d}:00)",
            "target_period": f"{request.start_date} -> {request.end_date}",
            "expected_hourly_samples_per_day": EXPECTED_HOURLY_SAMPLES_PER_DAY,
            "accumulated_variables": sorted(
                set(request.variables) & ERA5_LAND_ACCUMULATED_VARIABLES
            ),
            "daily_mean_variables": sorted(
                set(request.variables) & ERA5_LAND_DAILY_MEAN_VARIABLES
            ),
        },
    )

    daily: pd.DataFrame | None = None

    for variable in request.variables:
        if variable in ERA5_LAND_ACCUMULATED_VARIABLES:
            variable_daily = _aggregate_accumulated_variable(
                selected,
                variable,
            )
        else:
            variable_daily = _aggregate_instantaneous_variable(
                selected,
                variable,
            )

        if daily is None:
            daily = variable_daily
        else:
            daily = daily.merge(
                variable_daily,
                on=["_local_day", "latitude", "longitude"],
                how="inner",
                validate="one_to_one",
            )

    if daily is None or daily.empty:
        raise ValueError(
            "ERA5-Land daily aggregation produced no complete local days."
        )

    daily = daily.rename(
        columns={"_local_day": "valid_time"}
    )
    daily = _filter_target_period(
        daily,
        request,
    )

    if daily.empty:
        raise ValueError(
            "ERA5-Land daily aggregation contains no rows inside the target period."
        )

    daily = daily[
        [
            "valid_time",
            "latitude",
            "longitude",
            *request.variables,
        ]
    ].sort_values(
        ["valid_time", "latitude", "longitude"]
    ).reset_index(drop=True)

    log_metric(
        "Daily aggregation result",
        {
            "local_days": int(daily["valid_time"].nunique()),
            "grid_cells": int(
                daily[["latitude", "longitude"]]
                .drop_duplicates()
                .shape[0]
            ),
            "rows": len(daily),
        },
    )

    return daily


# =============================================================================
# TRANSFORMATION ENTRY POINT
# =============================================================================


def _transform_dataset(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    """Apply source-aware transformations to a weather DataFrame."""
    if request.dataset == ERA5_LAND_DATASET:
        return _aggregate_era5_land_daily(
            dataframe,
            request,
        )

    return _select_observation_columns(
        dataframe,
        request,
    )


def transform_data(
    raw_dataset: LoadedWeatherDataset,
) -> TransformedWeatherDataset:
    """Transform the RAW hourly DataFrame into the downstream representation."""
    log_dataframe_info(
        raw_dataset.raw_data,
        title="Transformation input",
    )

    transformed = _transform_dataset(
        raw_dataset.raw_data,
        raw_dataset.request,
    )

    log_dataframe_changes(
        before=raw_dataset.raw_data,
        after=transformed,
        step="Transformation",
    )

    log_dataframe_info(
        transformed,
        title="Transformation output",
    )

    _log_dataframe_preview(
        transformed,
    )

    return TransformedWeatherDataset(
        request=raw_dataset.request,
        transformed_at=datetime.now(UTC),
        dataset=transformed,
    )


class DefaultDataTransformer(DataTransformer):
    """Transform weather data using the default source-aware rules."""

    def transform(
        self,
        raw_dataset: LoadedWeatherDataset,
    ) -> TransformedWeatherDataset:
        return transform_data(raw_dataset)
