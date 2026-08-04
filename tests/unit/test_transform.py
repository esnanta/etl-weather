# tests/unit/test_transform.py

from datetime import datetime

import numpy as np
import xarray as xr

from pipeline.extract import (
    BoundingBox,
    RawWeatherDataset,
    WeatherRequest,
)

from pipeline.transform import (
    rename_variables,
    normalize_coordinates,
    normalize_datatypes,
    transform_data,
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


def create_dataset():

    return xr.Dataset(
        {
            "tp": (
                (
                    "time",
                    "values",
                ),
                np.array(
                    [[1.5]],
                    dtype="float64",
                ),
            ),
        },
        coords={
            "time": [
                "2015-01-01",
            ],
            "values": np.array(
                [0],
                dtype="int64",
            ),
            "latitude": (
                "values",
                np.array(
                    [5.0],
                    dtype="float64",
                ),
            ),
            "longitude": (
                "values",
                np.array(
                    [95.0],
                    dtype="float64",
                ),
            ),
        },
    )


def create_raw_dataset():

    return RawWeatherDataset(
        request=create_request(),
        downloaded_at=datetime.now(),
        raw_data=create_dataset(),
    )


def test_rename_variables():

    dataset = rename_variables(
        create_dataset(),
    )

    assert (
        "total_precipitation"
        in dataset.data_vars
    )

    assert (
        "tp"
        not in dataset.data_vars
    )


def test_normalize_coordinates():

    dataset = (
        normalize_coordinates(
            create_dataset(),
        )
    )

    assert (
        dataset.latitude.dtype
        == "float32"
    )

    assert (
        dataset.longitude.dtype
        == "float32"
    )

    assert (
        dataset["values"].dtype
        == "int32"
    )


def test_normalize_datatypes():

    dataset = (
        normalize_datatypes(
            create_dataset(),
        )
    )

    assert (
        dataset["tp"].dtype
        == "float32"
    )


def test_transform_data():

    transformed = (
        transform_data(
            create_raw_dataset(),
        )
    )

    dataset = (
        transformed.dataset
    )

    assert (
        "total_precipitation"
        in dataset.data_vars
    )

    assert (
        dataset[
            "total_precipitation"
        ].dtype
        == "float32"
    )

    assert (
        dataset.latitude.dtype
        == "float32"
    )

    assert (
        dataset.longitude.dtype
        == "float32"
    )

    assert (
        dataset["values"].dtype
        == "int32"
    )

def test_multiple_variables():

    dataset = xr.Dataset(
        {
            "tp": (
                (
                    "time",
                    "values",
                ),
                [[1.0]],
            ),
            "ssrd": (
                (
                    "time",
                    "values",
                ),
                [[100.0]],
            ),
        },
        coords={
            "time": [
                "2015-01-01",
            ],
            "values": [0],
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

    dataset = (
        rename_variables(
            dataset,
        )
    )

    assert (
        "total_precipitation"
        in dataset.data_vars
    )

    assert (
        "surface_solar_radiation_downwards"
        in dataset.data_vars
    )