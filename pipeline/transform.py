from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import cast

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
ERA5_LAND_FIRST_LOCAL_DAY = pd.Timestamp("1950-01-01")

ERA5_LAND_ACCUMULATED_VARIABLES = {"tp", "ssrd"}
ERA5_LAND_DAILY_MEAN_VARIABLES = {"t2m", "d2m", "swvl1"}

ERA5_LAND_OUTPUT_UNITS = {
    "tp": "mm/day",
    "t2m": "degC",
    "d2m": "degC",
    "swvl1": "m3/m3",
    "ssrd": "J/m2/day",
}


def _validate_era5_land_local_day_context(request: WeatherRequest) -> None:
    """Reject the first local day because its UTC+7 context predates ERA5-Land."""
    target_start = pd.Timestamp(request.start_date)
    target_end = pd.Timestamp(request.end_date)
    if target_start <= ERA5_LAND_FIRST_LOCAL_DAY <= target_end:
        required_context_start = ERA5_LAND_FIRST_LOCAL_DAY - pd.Timedelta(
            hours=LOCAL_UTC_OFFSET_HOURS
        )
        raise ValueError(
            "ERA5-Land local day 1950-01-01 cannot be transformed for UTC+7: "
            "it requires UTC context starting at "
            f"{required_context_start:%Y-%m-%d %H:%M}, before ERA5-Land begins."
        )


def _select_observation_columns(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    """Keep only coordinates and requested provider variables."""
    required_columns = ["valid_time", "latitude", "longitude", *request.variables]
    missing_columns = [c for c in required_columns if c not in dataframe.columns]
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


def _aggregate_instantaneous_variable(
    dataframe: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    working = dataframe[["valid_time", "latitude", "longitude", variable]].copy()
    working["_local_day"] = (
        working["valid_time"] + pd.Timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
    ).dt.floor("D")

    group_columns = ["_local_day", "latitude", "longitude"]
    grouped = working.groupby(group_columns, sort=True)[variable]
    daily_mean = grouped.mean()
    sample_count = grouped.count()
    complete = sample_count == EXPECTED_HOURLY_SAMPLES_PER_DAY
    return daily_mean[complete].rename(variable).reset_index()


def _aggregate_accumulated_variable(
    dataframe: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    """Aggregate an ERA5-Land accumulated variable to a UTC+7 local-day total."""
    working = cast(
        pd.DataFrame,
        dataframe[["valid_time", "latitude", "longitude", variable]].copy(),
    )

    utc_midnight = cast(
        pd.DataFrame,
        working.loc[working["valid_time"].dt.hour.eq(0)].copy(),
    )
    utc_midnight["_local_day"] = utc_midnight["valid_time"].dt.floor("D")
    utc_midnight = utc_midnight.rename(columns={variable: "_utc_midnight_value"})[
        ["_local_day", "latitude", "longitude", "_utc_midnight_value"]
    ]

    local_midnight = cast(
        pd.DataFrame,
        working.loc[
            working["valid_time"].dt.hour.eq(LOCAL_MIDNIGHT_IN_UTC_HOUR)
        ].copy(),
    )
    local_midnight["_utc_day"] = local_midnight["valid_time"].dt.floor("D")

    current_local_midnight = local_midnight.rename(
        columns={
            "_utc_day": "_local_day",
            variable: "_current_local_midnight_value",
        }
    )[["_local_day", "latitude", "longitude", "_current_local_midnight_value"]]

    previous_local_midnight = local_midnight.copy()
    previous_local_midnight["_local_day"] = (
        previous_local_midnight["_utc_day"] + pd.Timedelta(days=1)
    )
    previous_local_midnight = previous_local_midnight.rename(
        columns={variable: "_previous_local_midnight_value"}
    )[["_local_day", "latitude", "longitude", "_previous_local_midnight_value"]]

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
            "Detected %s negative daily accumulated values for '%s'. Values retained.",
            negative_count,
            variable,
        )

    return boundary[["_local_day", "latitude", "longitude", variable]]


def _attach_daily_time_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    result = dataframe.copy()
    result["valid_time_local"] = pd.to_datetime(result["_local_day"])
    result["valid_time"] = (
        result["valid_time_local"] - pd.Timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
    )
    return result


def _filter_target_period(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    start = pd.Timestamp(request.start_date)
    end = pd.Timestamp(request.end_date)
    return dataframe.loc[
        dataframe["valid_time_local"].between(start, end)
    ].copy()


def _convert_era5_land_units(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    result = dataframe.copy()
    conversions: dict[str, str] = {}

    if "tp" in request.variables:
        result["tp"] = result["tp"] * 1000.0
        conversions["tp"] = "m -> mm"

    for variable in ("t2m", "d2m"):
        if variable in request.variables:
            result[variable] = result[variable] - 273.15
            conversions[variable] = "K -> degC"

    log_metric(
        "ERA5-Land unit conversion",
        {
            "variables": ", ".join(request.variables),
            "conversions": conversions,
            "output_units": {
                variable: ERA5_LAND_OUTPUT_UNITS[variable]
                for variable in request.variables
                if variable in ERA5_LAND_OUTPUT_UNITS
            },
        },
    )
    return result


def _aggregate_era5_land_daily(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    _validate_era5_land_local_day_context(request)
    selected = _select_observation_columns(dataframe, request)

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
            "accumulated_variables": ", ".join(
                sorted(set(request.variables) & ERA5_LAND_ACCUMULATED_VARIABLES)
            ),
            "daily_mean_variables": ", ".join(
                sorted(set(request.variables) & ERA5_LAND_DAILY_MEAN_VARIABLES)
            ),
        },
    )

    daily: pd.DataFrame | None = None
    for variable in request.variables:
        variable_daily = (
            _aggregate_accumulated_variable(selected, variable)
            if variable in ERA5_LAND_ACCUMULATED_VARIABLES
            else _aggregate_instantaneous_variable(selected, variable)
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
        raise ValueError("ERA5-Land daily aggregation produced no complete local days.")

    daily_frame = _attach_daily_time_columns(daily)
    daily_frame = _filter_target_period(daily_frame, request)
    if daily_frame.empty:
        raise ValueError(
            "ERA5-Land daily aggregation contains no rows inside the target period."
        )

    daily_frame = _convert_era5_land_units(daily_frame, request)
    daily_frame = daily_frame[
        ["valid_time", "valid_time_local", "latitude", "longitude", *request.variables]
    ].sort_values(
        ["valid_time_local", "latitude", "longitude"]
    ).reset_index(drop=True)

    log_metric(
        "Daily aggregation result",
        {
            "local_days": int(daily_frame["valid_time_local"].nunique()),
            "valid_time_utc_start": daily_frame["valid_time"].min(),
            "valid_time_utc_end": daily_frame["valid_time"].max(),
            "valid_time_local_start": daily_frame["valid_time_local"].min(),
            "valid_time_local_end": daily_frame["valid_time_local"].max(),
            "grid_cells": int(
                daily_frame[["latitude", "longitude"]].drop_duplicates().shape[0]
            ),
            "rows": len(daily_frame),
        },
    )
    return daily_frame


def _transform_dataset(
    dataframe: pd.DataFrame,
    request: WeatherRequest,
) -> pd.DataFrame:
    if request.dataset == ERA5_LAND_DATASET:
        return _aggregate_era5_land_daily(dataframe, request)
    return _select_observation_columns(dataframe, request)


def transform_data(raw_dataset: LoadedWeatherDataset) -> TransformedWeatherDataset:
    log_dataframe_info(raw_dataset.raw_data, title="Transformation input")
    transformed = _transform_dataset(raw_dataset.raw_data, raw_dataset.request)
    log_dataframe_changes(
        before=raw_dataset.raw_data,
        after=transformed,
        step="Transformation",
    )
    log_dataframe_info(transformed, title="Transformation output")
    _log_dataframe_preview(transformed)
    return TransformedWeatherDataset(
        request=raw_dataset.request,
        transformed_at=datetime.now(UTC),
        dataset=transformed,
    )


class DefaultDataTransformer(DataTransformer):
    def transform(
        self,
        raw_dataset: LoadedWeatherDataset,
    ) -> TransformedWeatherDataset:
        return transform_data(raw_dataset)
