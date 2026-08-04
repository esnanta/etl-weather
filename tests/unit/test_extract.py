# tests/unit/test_extract.py

import pytest

from pipeline.extract import (
    BoundingBox,
    WeatherRequest,
    build_request,
    validate_request,
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


def test_validate_request():

    validate_request(
        create_request(),
    )


def test_invalid_date():

    request = create_request()

    request = WeatherRequest(
        dataset=request.dataset,
        start_date="invalid-date",
        end_date=request.end_date,
        variables=request.variables,
        bbox=request.bbox,
    )

    with pytest.raises(
        ValueError,
    ):
        validate_request(
            request,
        )


def test_build_request():

    request = build_request(
        create_request(),
    )

    assert (
        request.dataset
        == "era5-single-levels"
    )

    assert (
        request.variables
        == ["tp"]
    )