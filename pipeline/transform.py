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
LOCAL_MIDNIGHT_IN_UTC_HOUR = (24 - LOCAL_UTC_OFFSET_HOURS) % 24
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


def _aggregate_accumulated_variable(
    dataframe: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    """
    Aggregate an ERA5-Land accumulated variable to a UTC+7 local-day total.

    ERA5-Land accumulations run from 00 UTC to the forecast step. For a
    positive UTC offset, ECMWF's documented non-UTC daily accumulation uses
    the UTC-midnight value together with the local-midnight boundary values.

    For UTC+7, local midnight is 17 UTC on the preceding UTC day. Therefore,
    for local day D:

        daily(D) = A(D, 17 UTC)
                   + A(D, 00 UTC)
                   - A(D-1, 17 UTC)

    where A is the ERA5-Land accumulated value. This avoids reconstructing
    hourly increments and does not clip negative differences.
    """
    working = dataframe[
        ["valid_time", "latitude", "longitude", variable]
    ].copy()

    utc_midnight = working.loc[
        working["valid_time"].dt.hour.eq(0)
    ].copy()
    utc_midnight["_local_day"] = utc_midnight["valid_time"].dt.floor("D")
    utc_midnight = utc_midnight.rename(
        columns={variable: "_utc_midnight_value"}
    )[
        [
            "_local_day",
            "latitude",
            "longitude",
            "_utc_midnight_value",
        ]
    ]

    local_midnight = working.loc[
        working["valid_time"].dt.hour.eq(LOCAL_MIDNIGHT_IN_UTC_HOUR)
    ].copy()
    local_midnight["_utc_day"] = local_midnight["valid_time"].dt.floor("D")

    current_local_midnight = local_midnight.rename(
        columns={
            "_utc_day": "_local_day",
            variable: "_current_local_midnight_value",
        }
    )[
        [
            "_local_day",
            "latitude",
            "longitude",
            "_current_local_midnight_value",
        ]
    ]

    previous_local_midnight = local_midnight.copy()
    previous_local_midnight["_local_day"] = (
        previous_local_midnight["_utc_day"]
        + pd.Timedelta(days=1)
    )
    previous_local_midnight = previous_local_midnight.rename(
        columns={variable: "_previous_local_midnight_value"}
    )[
        [
            "_local_day",
            "latitude",
            "longitude",
            "_previous_local_midnight_value",
        ]
    ]

    group_columns = ["_local_day", "latitude", "longitude"]

    boundary = utc_midnight.merge(
        current_local_midnight,
        on=group_columns,
        how="inner",
        validate="one_to_one",
    ).merge(
        previous_local_midnight,
        on=group_columns,
        how="inner",
        validate="one_to_one",
    )

    boundary[variable] = (
        boundary["_current_local_midnight_value"]
        + boundary["_utc_midnight_value"]
        - boundary["_previous_local_midnight_value"]
    )

    negative_count = int((boundary[variable] < -1e-10).sum())
    if negative_count:
        logger.warning(
            "Detected %s negative daily accumulated values for '%s'. "
            "Values are retained unchanged for validation.",
            negative_count,
            variable,
        )

    return boundary[
        [
            "_local_day",
            "latitude",
            "longitude",
            variable,
        ]
    ]


def _attach_daily_time_columns(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Keep UTC and local daily boundaries as separate columns.

    valid_time is the UTC instant corresponding to the start of the local day.
    valid_time_local is the same boundary expressed as UTC+7 wall-clock time.
    """
    result = dataframe.copy()
    result["valid_time_local"] = pd.to_datetime(result["_local_day"])
    result["valid_time"] = (
        result["valid_time_local"]
        - pd.Timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
    )
    return result


def _filter_target_period(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    """Keep only local days belonging to the requested target period."""
    start = pd.Timestamp(request.start_date)
    end = pd.Timestamp(request.end_date)

    return dataframe.loc[
        dataframe["valid_time_local"].between(start, end)
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
            "local_midnight_in_utc": f"{LOCAL_MIDNIGHT_IN_UTC_HOUR:02d}:00",
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

    daily = _attach_daily_time_columns(daily)
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
            "valid_time_local",
            "latitude",
            "longitude",
            *request.variables,
        ]
    ].sort_values(
        ["valid_time_local", "latitude", "longitude"]
    ).reset_index(drop=True)

    log_metric(
        "Daily aggregation result",
        {
            "local_days": int(daily["valid_time_local"].nunique()),
            "valid_time_utc_start": daily["valid_time"].min(),
            "valid_time_utc_end": daily["valid_time"].max(),
            "valid_time_local_start": daily["valid_time_local"].min(),
            "valid_time_local_end": daily["valid_time_local"].max(),
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
