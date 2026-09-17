from datetime import datetime, timezone

import pandas as pd
import pytest

from pipeline.model import BoundingBox, DataSource, LoadedWeatherDataset, WeatherRequest
from pipeline.transform import transform_data


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
    assert set(["tp", "t2m", "d2m", "swvl1", "ssrd"]).issubset(transformed.columns)


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
