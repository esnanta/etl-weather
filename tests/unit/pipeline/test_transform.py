from dataclasses import replace
from datetime import datetime, timezone
import logging

import pandas as pd
import pytest

from pipeline.models.weather import BoundingBox, DataSource, LoadedWeatherDataset, WeatherRequest
from pipeline.transformation.era5_land_daily import transform_data


def _create_era5_land_request() -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-01",
        variables=["tp", "t2m", "swvl1", "ssrd", "d2m"],
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )


def _era5_land_cumulative_value(timestamp: pd.Timestamp, hourly_increment: float) -> float:
    if timestamp.hour == 0:
        return 24 * hourly_increment
    return timestamp.hour * hourly_increment


def _create_hourly_dataframe() -> pd.DataFrame:
    valid_time = pd.date_range(
        "2021-12-31 00:00:00",
        "2022-01-01 23:00:00",
        freq="h",
    )
    return pd.DataFrame(
        {
            "valid_time": valid_time,
            "latitude": 4.5,
            "longitude": 96.8,
            "tp": [
                _era5_land_cumulative_value(timestamp, 0.001)
                for timestamp in valid_time
            ],
            "t2m": 300.0,
            "swvl1": 0.30,
            "ssrd": [
                _era5_land_cumulative_value(timestamp, 3600.0)
                for timestamp in valid_time
            ],
            "d2m": 295.0,
            "number": 0,
            "surface": 0.0,
            "expver": "0001",
        }
    )


def _create_loaded_dataset() -> LoadedWeatherDataset:
    return LoadedWeatherDataset(
        request=_create_era5_land_request(),
        downloaded_at=datetime.now(timezone.utc),
        raw_data=_create_hourly_dataframe(),
    )


def test_era5_land_daily_aggregation_keeps_utc_and_local_time() -> None:
    transformed = transform_data(_create_loaded_dataset()).dataset
    assert len(transformed) == 1
    assert transformed.loc[0, "valid_time"] == pd.Timestamp("2021-12-31 17:00:00")
    assert transformed.loc[0, "valid_time_local"] == pd.Timestamp("2022-01-01 00:00:00")


def test_era5_land_accumulated_variables_use_boundary_formula() -> None:
    transformed = transform_data(_create_loaded_dataset()).dataset
    assert transformed.loc[0, "tp"] == pytest.approx(24.0)
    assert transformed.loc[0, "ssrd"] == pytest.approx(24 * 3600.0)


def test_era5_land_daily_mean_uses_all_24_local_hours() -> None:
    loaded = _create_loaded_dataset()
    local_day_mask = loaded.raw_data["valid_time"].between(
        pd.Timestamp("2021-12-31 17:00:00"),
        pd.Timestamp("2022-01-01 16:00:00"),
    )
    loaded.raw_data.loc[local_day_mask, "t2m"] = [
        273.15 + local_hour for local_hour in range(24)
    ]

    transformed = transform_data(loaded).dataset

    assert transformed.loc[0, "t2m"] == pytest.approx(11.5)


def test_era5_land_daily_formulas_remain_independent_across_grid_cells() -> None:
    request = replace(
        _create_era5_land_request(),
        variables=["tp", "ssrd", "t2m"],
    )
    valid_time = pd.date_range(
        "2021-12-31 00:00:00",
        "2022-01-01 23:00:00",
        freq="h",
    )
    grid_profiles = (
        {
            "latitude": 4.5,
            "longitude": 96.8,
            "tp_increment": 0.001,
            "ssrd_increment": 3600.0,
            "temperature_base_c": 10.0,
            "temperature_hour_scale": 1.0,
        },
        {
            "latitude": 4.6,
            "longitude": 96.9,
            "tp_increment": 0.002,
            "ssrd_increment": 7200.0,
            "temperature_base_c": 20.0,
            "temperature_hour_scale": 2.0,
        },
    )
    rows = []
    for profile in grid_profiles:
        for timestamp in valid_time:
            local_hour = (timestamp + pd.Timedelta(hours=7)).hour
            rows.append(
                {
                    "valid_time": timestamp,
                    "latitude": profile["latitude"],
                    "longitude": profile["longitude"],
                    "tp": _era5_land_cumulative_value(
                        timestamp,
                        profile["tp_increment"],
                    ),
                    "ssrd": _era5_land_cumulative_value(
                        timestamp,
                        profile["ssrd_increment"],
                    ),
                    "t2m": (
                        273.15
                        + profile["temperature_base_c"]
                        + profile["temperature_hour_scale"] * local_hour
                    ),
                }
            )

    loaded = LoadedWeatherDataset(
        request=request,
        downloaded_at=datetime.now(timezone.utc),
        raw_data=pd.DataFrame(rows),
    )

    transformed = transform_data(loaded).dataset.set_index(
        ["latitude", "longitude"]
    )

    assert len(transformed) == 2
    assert transformed.loc[(4.5, 96.8), "tp"] == pytest.approx(24.0)
    assert transformed.loc[(4.6, 96.9), "tp"] == pytest.approx(48.0)
    assert transformed.loc[(4.5, 96.8), "ssrd"] == pytest.approx(86_400.0)
    assert transformed.loc[(4.6, 96.9), "ssrd"] == pytest.approx(172_800.0)
    assert transformed.loc[(4.5, 96.8), "t2m"] == pytest.approx(21.5)
    assert transformed.loc[(4.6, 96.9), "t2m"] == pytest.approx(43.0)


def test_era5_land_temperature_units_are_celsius() -> None:
    transformed = transform_data(_create_loaded_dataset()).dataset
    assert transformed.loc[0, "t2m"] == pytest.approx(26.85)
    assert transformed.loc[0, "d2m"] == pytest.approx(21.85)


def test_era5_land_soil_moisture_remains_m3_per_m3() -> None:
    transformed = transform_data(_create_loaded_dataset()).dataset
    assert transformed.loc[0, "swvl1"] == pytest.approx(0.30)


def test_source_metadata_is_not_propagated_to_transformed_dataset() -> None:
    transformed = transform_data(_create_loaded_dataset()).dataset
    assert "number" not in transformed.columns
    assert "surface" not in transformed.columns
    assert "expver" not in transformed.columns
    assert {"tp", "t2m", "d2m", "swvl1", "ssrd"}.issubset(transformed.columns)


def test_era5_land_transform_rejects_missing_requested_column() -> None:
    loaded = _create_loaded_dataset()
    loaded.raw_data.drop(columns=["d2m"], inplace=True)

    with pytest.raises(ValueError, match="missing required columns: d2m"):
        transform_data(loaded)


def test_era5_land_transform_rejects_incomplete_local_day() -> None:
    loaded = _create_loaded_dataset()
    missing_hour = pd.Timestamp("2022-01-01 06:00:00")
    loaded.raw_data.drop(
        loaded.raw_data.index[loaded.raw_data["valid_time"].eq(missing_hour)],
        inplace=True,
    )

    with pytest.raises(ValueError, match="produced no complete local days"):
        transform_data(loaded)


def test_era5_land_transform_rejects_first_local_day_without_prior_context() -> None:
    request = replace(
        _create_era5_land_request(),
        start_date="1950-01-01",
        end_date="1950-01-01",
    )
    available_hourly_data = _create_hourly_dataframe().iloc[:24].copy()
    available_hourly_data["valid_time"] = pd.date_range(
        "1950-01-01 00:00:00",
        "1950-01-01 23:00:00",
        freq="h",
    )
    loaded = LoadedWeatherDataset(
        request=request,
        downloaded_at=datetime.now(timezone.utc),
        raw_data=available_hourly_data,
    )

    with pytest.raises(
        ValueError,
        match="requires UTC context starting at 1949-12-31 17:00",
    ):
        transform_data(loaded)


def test_era5_land_transform_rejects_null_state_sample() -> None:
    loaded = _create_loaded_dataset()
    missing_sample = pd.Timestamp("2022-01-01 06:00:00")
    loaded.raw_data.loc[
        loaded.raw_data["valid_time"].eq(missing_sample),
        "t2m",
    ] = float("nan")

    with pytest.raises(ValueError, match="produced no complete local days"):
        transform_data(loaded)


def test_era5_land_accumulated_null_boundary_remains_missing() -> None:
    loaded = _create_loaded_dataset()
    loaded.raw_data.loc[
        loaded.raw_data["valid_time"].eq(pd.Timestamp("2022-01-01 17:00:00")),
        "tp",
    ] = float("nan")

    transformed = transform_data(loaded).dataset

    assert pd.isna(transformed.loc[0, "tp"])


def test_era5_land_negative_accumulation_is_retained_with_warning(caplog) -> None:
    loaded = _create_loaded_dataset()
    boundary_values = {
        pd.Timestamp("2021-12-31 17:00:00"): 0.010,
        pd.Timestamp("2022-01-01 00:00:00"): 0.002,
        pd.Timestamp("2022-01-01 17:00:00"): 0.001,
    }
    for timestamp, value in boundary_values.items():
        loaded.raw_data.loc[
            loaded.raw_data["valid_time"].eq(timestamp),
            "tp",
        ] = value

    with caplog.at_level(logging.WARNING):
        transformed = transform_data(loaded).dataset

    assert transformed.loc[0, "tp"] == pytest.approx(-7.0)
    assert "negative daily accumulated values" in caplog.text
