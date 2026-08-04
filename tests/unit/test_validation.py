# tests/unit/test_validation.py

from datetime import datetime
from datetime import timezone

import numpy as np
import pytest
import xarray as xr

from pipeline.extract import (
    BoundingBox,
    RawWeatherDataset,
    WeatherRequest,
)

from pipeline.transform import (
    TransformedWeatherDataset,
)

from pipeline.validation import (
    ValidationError,
    validate_raw_dataset,
    validate_transformed_dataset,
)


def create_request():

    return WeatherRequest(
        dataset="era5-single-levels",
        start_date="2015-01-01",
        end_date="2015-12-31",
        variables=[
            "tp",
        ],
        bbox=BoundingBox(
            north_lat=6.0,
            west_lon=95.0,
            south_lat=1.0,
            east_lon=98.5,
        ),
    )


def create_raw_dataset():

    return xr.Dataset(
        {
            "tp": (
                (
                    "time",
                    "values",
                ),
                [[1.0]],
            ),
        },
        coords={
            "time": [
                "2015-01-01",
            ],
            "values": [
                0,
            ],
            "latitude": (
                "values",
                [5.0],
            ),
            "longitude": (
                "values",
                [95.0],
            ),
        },
    )


def create_transformed_dataset():

    return xr.Dataset(
        {
            "total_precipitation": (
                (
                    "time",
                    "values",
                ),
                np.array(
                    [[1.0]],
                    dtype="float32",
                ),
            ),
        },
        coords={
            "time": [
                "2015-01-01",
            ],
            "values": np.array(
                [0],
                dtype="int32",
            ),
            "latitude": (
                "values",
                np.array(
                    [5.0],
                    dtype="float32",
                ),
            ),
            "longitude": (
                "values",
                np.array(
                    [95.0],
                    dtype="float32",
                ),
            ),
        },
    )


def create_transformed_weather_dataset():

    return (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=(
                create_transformed_dataset()
            ),
        )
    )


# ------------------------------------------------------------------
# RAW DATASET
# ------------------------------------------------------------------


def test_validation_success():

    raw_dataset = (
        RawWeatherDataset(
            request=create_request(),
            downloaded_at=datetime.now(
                timezone.utc,
            ),
            raw_data=create_raw_dataset(),
        )
    )

    validate_raw_dataset(
        raw_dataset,
    )


def test_empty_dataset():

    raw_dataset = (
        RawWeatherDataset(
            request=create_request(),
            downloaded_at=datetime.now(
                timezone.utc,
            ),
            raw_data=xr.Dataset(),
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_raw_dataset(
            raw_dataset,
        )


def test_none_raw_dataset():

    with pytest.raises(
        ValidationError,
    ):
        validate_raw_dataset(
            None,
        )


# ------------------------------------------------------------------
# TRANSFORMED DATASET
# ------------------------------------------------------------------


def test_validate_transformed_dataset_success():

    validate_transformed_dataset(
        create_transformed_weather_dataset(),
    )


def test_none_transformed_dataset():

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            None,
        )


def test_empty_transformed_dataset():

    dataset = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=xr.Dataset(),
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            dataset,
        )


def test_missing_time_dimension():

    dataset = (
        create_transformed_dataset()
        .drop_dims("time")
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_missing_values_dimension():

    dataset = (
        create_transformed_dataset()
        .drop_dims("values")
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_missing_latitude_coordinate():

    dataset = (
        create_transformed_dataset()
        .drop_vars("latitude")
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_missing_longitude_coordinate():

    dataset = (
        create_transformed_dataset()
        .drop_vars("longitude")
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_missing_values_coordinate():

    dataset = (
        create_transformed_dataset()
        .drop_vars("values")
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_invalid_latitude_dtype():

    dataset = (
        create_transformed_dataset()
    )

    dataset["latitude"] = (
        dataset.latitude.astype(
            "float64",
        )
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_invalid_longitude_dtype():

    dataset = (
        create_transformed_dataset()
    )

    dataset["longitude"] = (
        dataset.longitude.astype(
            "float64",
        )
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_invalid_values_dtype():

    dataset = (
        create_transformed_dataset()
    )

    dataset["values"] = (
        dataset["values"].astype(
            "int64",
        )
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_variable_not_transformed():

    dataset = (
        create_transformed_dataset()
    )

    dataset = dataset.rename(
        {
            "total_precipitation":
            "tp",
        }
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )


def test_invalid_variable_dtype():

    dataset = (
        create_transformed_dataset()
    )

    dataset[
        "total_precipitation"
    ] = (
        dataset[
            "total_precipitation"
        ].astype(
            "float64",
        )
    )

    transformed = (
        TransformedWeatherDataset(
            transformed_at=datetime.now(
                timezone.utc,
            ),
            dataset=dataset,
        )
    )

    with pytest.raises(
        ValidationError,
    ):
        validate_transformed_dataset(
            transformed,
        )