from datetime import datetime, timezone

import pandas as pd
import pytest

from pipeline.model import (
    BoundingBox,
    DataSource,
    LoadedWeatherDataset,
    WeatherRequest,
)
from pipeline.transform import transform_data


def _create_era5_land_request() -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-01",
        variables=[
            "total_precipitation",
            "2m_temperature",
            "volumetric_soil_water_layer_1",
            "surface_solar_radiation_downwards",
            "2m_dewpoint_temperature",
        ],
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )


def _era5_land_cumulative_value(
    timestamp: pd.Timestamp,
    hourly_increment: float,
) -> float:
    """Build a synthetic ERA5-Land cumulative forecast-step value."""
    if timestamp.hour == 0:
        return 24 * hourly_increment
    return timestamp.hour * hourly_increment


def _create_hourly_dataframe() -> pd.DataFrame:
    valid_time = pd.date_range(
        "2021-12-31 00:00:00",
        "2022-01-01 23:00:00",
        freq="h",
    )

    dataframe = pd.DataFrame(
        {
            "valid_time": valid_time,
            "latitude": 4.5,
            "longitude": 96.8,
            "total_precipitation": [
                _era5_land_cumulative_value(timestamp, 0.001)
                for timestamp in valid_time
            ],
            "2m_temperature": 300.0,
            "volumetric_soil_water_layer_1": 0.30,
            "surface_solar_radiation_downwards": [
                _era5_land_cumulative_value(timestamp, 3600.0)
                for timestamp in valid_time
            ],
            "2m_dewpoint_temperature": 295.0,
            "number": 0,
            "surface": 0.0,
            "expver": "0001",
        }
    )

    return dataframe


def _create_loaded_dataset() -> LoadedWeatherDataset:
    return LoadedWeatherDataset(
        request=_create_era5_land_request(),
        downloaded_at=datetime.now(timezone.utc),
        raw_data=_create_hourly_dataframe(),
    )


def test_era5_land_daily_aggregation_uses_utc_plus_7() -> None:
    transformed = transform_data(
        _create_loaded_dataset()
    ).dataset

    assert len(transformed) == 1
    assert transformed.loc[0, "valid_time"] == pd.Timestamp("2022-01-01")


def test_era5_land_accumulated_variables_are_daily_totals() -> None:
    transformed = transform_data(
        _create_loaded_dataset()
    ).dataset

    assert transformed.loc[0, "total_precipitation"] == pytest.approx(0.024)
    assert transformed.loc[0, "surface_solar_radiation_downwards"] == pytest.approx(
        24 * 3600.0
    )


def test_era5_land_instantaneous_variables_are_daily_means() -> None:
    transformed = transform_data(
        _create_loaded_dataset()
    ).dataset

    assert transformed.loc[0, "2m_temperature"] == pytest.approx(300.0)
    assert transformed.loc[0, "volumetric_soil_water_layer_1"] == pytest.approx(0.30)
    assert transformed.loc[0, "2m_dewpoint_temperature"] == pytest.approx(295.0)


def test_source_metadata_is_not_propagated_to_transformed_dataset() -> None:
    transformed = transform_data(
        _create_loaded_dataset()
    ).dataset

    assert "number" not in transformed.columns
    assert "surface" not in transformed.columns
    assert "expver" not in transformed.columns
