from datetime import datetime, timezone

import pandas as pd
import pytest
import xarray as xr

from pipeline.model import (
    BoundingBox,
    DataSource,
    RawWeatherDataset,
    TransformedWeatherDataset,
    WeatherRequest,
)
from pipeline.validation import (
    ValidationError,
    validate_raw_dataset,
    validate_transformed_dataset,
)


def _request(
    bbox: BoundingBox | None = None,
) -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-03",
        variables=[
            "total_precipitation",
            "2m_temperature",
        ],
        bbox=bbox
        or BoundingBox(
            north_lat=4.5,
            west_lon=96.8,
            south_lat=4.5,
            east_lon=96.8,
        ),
    )


def _raw_dataset(
    valid_time: pd.DatetimeIndex | None = None,
    *,
    latitude: list[float] | None = None,
    longitude: list[float] | None = None,
    request: WeatherRequest | None = None,
) -> RawWeatherDataset:
    if valid_time is None:
        valid_time = pd.date_range(
            "2021-12-31 00:00:00",
            "2022-01-03 23:00:00",
            freq="h",
        )

    latitude = latitude or [4.5]
    longitude = longitude or [96.8]

    shape = (len(valid_time), len(latitude), len(longitude))
    dataset = xr.Dataset(
        data_vars={
            "total_precipitation": (
                ("valid_time", "latitude", "longitude"),
                0.001 * xr.ones_like(
                    xr.DataArray(
                        [[[0.0] * len(longitude)] * len(latitude)] * len(valid_time),
                        dims=("valid_time", "latitude", "longitude"),
                    )
                ).values,
            ),
            "2m_temperature": (
                ("valid_time", "latitude", "longitude"),
                300.0 * xr.ones_like(
                    xr.DataArray(
                        [[[0.0] * len(longitude)] * len(latitude)] * len(valid_time),
                        dims=("valid_time", "latitude", "longitude"),
                    )
                ).values,
            ),
        },
        coords={
            "valid_time": valid_time,
            "latitude": latitude,
            "longitude": longitude,
        },
    )

    return RawWeatherDataset(
        request=request or _request(),
        downloaded_at=datetime.now(timezone.utc),
        raw_data=dataset,
    )


def _transformed_dataframe() -> pd.DataFrame:
    valid_time_local = pd.date_range(
        "2022-01-01 00:00:00",
        "2022-01-03 00:00:00",
        freq="D",
    )
    valid_time = valid_time_local - pd.Timedelta(hours=7)

    return pd.DataFrame(
        {
            "valid_time": valid_time,
            "valid_time_local": valid_time_local,
            "latitude": 4.5,
            "longitude": 96.8,
            "total_precipitation": [10.0, 20.0, 30.0],
            "2m_temperature": [26.0, 27.0, 28.0],
        }
    )


def _transformed_dataset(
    dataframe: pd.DataFrame | None = None,
) -> TransformedWeatherDataset:
    return TransformedWeatherDataset(
        request=_request(),
        transformed_at=datetime.now(timezone.utc),
        dataset=(
            _transformed_dataframe()
            if dataframe is None
            else dataframe
        ),
    )


def test_raw_temporal_validation_accepts_continuous_hourly_data() -> None:
    validate_raw_dataset(_raw_dataset())


def test_raw_temporal_validation_rejects_missing_hour() -> None:
    valid_time = pd.date_range(
        "2021-12-31 00:00:00",
        "2022-01-03 23:00:00",
        freq="h",
    ).delete(12)

    with pytest.raises(ValidationError):
        validate_raw_dataset(_raw_dataset(valid_time))


def test_raw_spatial_validation_accepts_regular_point_grid() -> None:
    validate_raw_dataset(_raw_dataset())


def test_raw_spatial_validation_accepts_regular_era5_land_grid() -> None:
    bbox = BoundingBox(
        north_lat=4.6,
        west_lon=96.8,
        south_lat=4.5,
        east_lon=96.9,
    )
    request = _request(bbox)

    validate_raw_dataset(
        _raw_dataset(
            latitude=[4.6, 4.5],
            longitude=[96.8, 96.9],
            request=request,
        )
    )


def test_raw_spatial_validation_rejects_wrong_resolution() -> None:
    bbox = BoundingBox(
        north_lat=4.7,
        west_lon=96.8,
        south_lat=4.5,
        east_lon=96.8,
    )
    request = _request(bbox)

    with pytest.raises(ValidationError):
        validate_raw_dataset(
            _raw_dataset(
                latitude=[4.7, 4.5],
                longitude=[96.8],
                request=request,
            )
        )


def test_transformed_temporal_validation_accepts_utc_plus_7_contract() -> None:
    validate_transformed_dataset(_transformed_dataset())


def test_transformed_temporal_validation_rejects_wrong_local_offset() -> None:
    dataframe = _transformed_dataframe()
    dataframe.loc[0, "valid_time_local"] = pd.Timestamp(
        "2022-01-01 01:00:00"
    )

    with pytest.raises(ValidationError):
        validate_transformed_dataset(_transformed_dataset(dataframe))


def test_transformed_temporal_validation_rejects_missing_local_day() -> None:
    dataframe = _transformed_dataframe().iloc[[0, 2]].copy()

    with pytest.raises(ValidationError):
        validate_transformed_dataset(_transformed_dataset(dataframe))


def test_transformed_temporal_validation_rejects_duplicate_grid_record() -> None:
    dataframe = pd.concat(
        [
            _transformed_dataframe(),
            _transformed_dataframe().iloc[[0]],
        ],
        ignore_index=True,
    )

    with pytest.raises(ValidationError):
        validate_transformed_dataset(_transformed_dataset(dataframe))


def test_transformed_spatial_validation_rejects_grid_membership_change() -> None:
    dataframe = _transformed_dataframe()
    extra = dataframe.iloc[[0]].copy()
    extra["longitude"] = 96.9
    dataframe = pd.concat([dataframe, extra], ignore_index=True)

    with pytest.raises(ValidationError):
        validate_transformed_dataset(_transformed_dataset(dataframe))


def test_transformed_spatial_validation_rejects_off_grid_coordinate() -> None:
    dataframe = _transformed_dataframe()
    dataframe.loc[:, "longitude"] = 96.85

    with pytest.raises(ValidationError):
        validate_transformed_dataset(_transformed_dataset(dataframe))
