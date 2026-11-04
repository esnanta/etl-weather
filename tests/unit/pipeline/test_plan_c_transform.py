"""Verify Plan C daily soil and wind science against explicit hourly fixtures.

Tests invoke the production transform_data() without mocking its calculations.
No network, Airflow, or database is needed; persistence is verified separately.
"""

from dataclasses import replace
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from pipeline.models.weather import BoundingBox, DataSource, LoadedWeatherDataset, WeatherRequest
from pipeline.transformation.era5_land_daily import transform_data


VARIABLES = ["tp", "t2m", "d2m", "swvl1", "swvl2", "swvl3", "ssrd", "u10", "v10"]


def _loaded(*, u: float = 3.0, v: float = 4.0) -> LoadedWeatherDataset:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-01",
        variables=VARIABLES.copy(),
        bbox=BoundingBox(north_lat=4.6, south_lat=4.5, east_lon=96.9, west_lon=96.8),
    )
    times = pd.date_range("2021-12-31", periods=48, freq="h")
    dataframe = pd.DataFrame({
        "valid_time": times,
        "latitude": 4.5,
        "longitude": 96.8,
        "tp": [0.024 if t.hour == 0 else t.hour * 0.001 for t in times],
        "t2m": 300.0,
        "d2m": 295.0,
        "swvl1": 0.3,
        "swvl2": 0.4,
        "swvl3": 0.5,
        "ssrd": [86400.0 if t.hour == 0 else t.hour * 3600.0 for t in times],
        "u10": u,
        "v10": v,
    })
    return LoadedWeatherDataset(
        request=request,
        downloaded_at=datetime.now(UTC),
        raw_data=dataframe,
    )


def _local_hour_mask(frame: pd.DataFrame) -> pd.Series:
    return frame["valid_time"].between(
        pd.Timestamp("2021-12-31 17:00:00"),
        pd.Timestamp("2022-01-01 16:00:00"),
    )


def test_nine_variable_daily_columns_and_legacy_statistics() -> None:
    row = transform_data(_loaded()).dataset
    assert list(row.columns) == [
        "valid_time", "valid_time_local", "latitude", "longitude",
        *VARIABLES, "wind_speed", "wind_direction",
    ]
    assert len(row) == 1
    assert row.loc[0, "valid_time"] == pd.Timestamp("2021-12-31 17:00:00")
    assert row.loc[0, "valid_time_local"] == pd.Timestamp("2022-01-01")
    assert row.loc[0, "tp"] == pytest.approx(24.0)
    assert row.loc[0, "ssrd"] == pytest.approx(86400.0)
    assert row.loc[0, "t2m"] == pytest.approx(26.85)
    assert row.loc[0, "d2m"] == pytest.approx(21.85)
    assert row.loc[0, "swvl1"] == pytest.approx(0.3)
    assert row.loc[0, "swvl2"] == pytest.approx(0.4)
    assert row.loc[0, "swvl3"] == pytest.approx(0.5)
    assert row.loc[0, "u10"] == pytest.approx(3.0)
    assert row.loc[0, "v10"] == pytest.approx(4.0)
    assert row.loc[0, "wind_speed"] == pytest.approx(5.0)


def test_soil_daily_means_include_all_24_local_hours() -> None:
    loaded = _loaded()
    loaded.raw_data.loc[_local_hour_mask(loaded.raw_data), "swvl2"] = [
        0.1 if hour < 12 else 0.5 for hour in range(24)
    ]
    loaded.raw_data.loc[_local_hour_mask(loaded.raw_data), "swvl3"] = [
        0.2 if hour < 8 else 0.8 for hour in range(24)
    ]

    row = transform_data(loaded).dataset.iloc[0]
    assert row["swvl2"] == pytest.approx(0.3)
    assert row["swvl3"] == pytest.approx(0.6)


def test_reversing_hourly_winds_keep_scalar_mean_speed() -> None:
    loaded = _loaded()
    loaded.raw_data.loc[_local_hour_mask(loaded.raw_data), "u10"] = [
        5.0 if hour < 12 else -5.0 for hour in range(24)
    ]
    loaded.raw_data.loc[_local_hour_mask(loaded.raw_data), "v10"] = 0.0

    row = transform_data(loaded).dataset.iloc[0]
    assert row["u10"] == pytest.approx(0.0)
    assert row["v10"] == pytest.approx(0.0)
    assert row["wind_speed"] == pytest.approx(5.0)
    assert pd.isna(row["wind_direction"])


@pytest.mark.parametrize(
    ("u", "v", "expected_from_direction"),
    [
        (0.0, -5.0, 0.0),
        (-5.0, 0.0, 90.0),
        (0.0, 5.0, 180.0),
        (5.0, 0.0, 270.0),
    ],
)
def test_wind_direction_uses_meteorological_from_convention(
    u: float, v: float, expected_from_direction: float
) -> None:
    row = transform_data(_loaded(u=u, v=v)).dataset.iloc[0]
    assert row["wind_direction"] == pytest.approx(expected_from_direction)
    assert row["wind_speed"] == pytest.approx(5.0)


def test_near_zero_resultant_has_missing_direction_not_north() -> None:
    row = transform_data(_loaded(u=1e-12, v=0.0)).dataset.iloc[0]
    assert pd.isna(row["wind_direction"])
    assert row["wind_speed"] == pytest.approx(1e-12, abs=1e-15)


@pytest.mark.parametrize("missing_variable", ["swvl2", "swvl3", "u10", "v10"])
def test_missing_hourly_observation_does_not_create_partial_daily_mean(
    missing_variable: str,
) -> None:
    loaded = _loaded()
    mask = loaded.raw_data["valid_time"].eq(pd.Timestamp("2022-01-01 06:00:00"))
    loaded.raw_data.loc[mask, missing_variable] = np.nan

    with pytest.raises(ValueError, match="produced no complete local days"):
        transform_data(loaded)


def test_missing_hourly_row_prevents_daily_wind_and_soil_outputs() -> None:
    loaded = _loaded()
    missing = loaded.raw_data["valid_time"].eq(pd.Timestamp("2022-01-01 06:00:00"))
    loaded.raw_data = loaded.raw_data.loc[~missing].copy()
    with pytest.raises(ValueError, match="produced no complete local days"):
        transform_data(loaded)


def test_legacy_five_variable_request_keeps_original_output_columns() -> None:
    loaded = _loaded()
    old_variables = ["tp", "t2m", "d2m", "swvl1", "ssrd"]
    loaded.request = replace(loaded.request, variables=old_variables)
    transformed = transform_data(loaded).dataset
    assert list(transformed.columns) == [
        "valid_time", "valid_time_local", "latitude", "longitude", *old_variables
    ]
