from __future__ import annotations

import pytest

from pipeline.model import BoundingBox, DataSource, WeatherRequest
from pipeline.utils import (
    CANONICAL_VARIABLE_NAMES,
    build_request,
    build_variable_signature,
)


def _bbox() -> BoundingBox:
    return BoundingBox(
        north_lat=6.2,
        south_lat=1.8,
        east_lon=98.6,
        west_lon=94.8,
    )


def test_build_request_keeps_explicit_canonical_cds_variables() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=[
            "total_precipitation",
            "2m_temperature",
            "2m_dewpoint_temperature",
            "volumetric_soil_water_layer_1",
            "surface_solar_radiation_downwards",
        ],
        bbox=_bbox(),
    )

    normalized_request = build_request(request)

    assert normalized_request.variables == request.variables
    assert set(normalized_request.variables).issubset(CANONICAL_VARIABLE_NAMES)


def test_build_request_rejects_short_provider_variable_name() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["tp"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError, match="canonical variable name"):
        build_request(request)


def test_build_request_rejects_unknown_cds_variable() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["unknown_weather_variable"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError):
        build_request(request)


def test_build_request_rejects_duplicate_variables() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["total_precipitation", "total_precipitation"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError, match="Duplicate variables"):
        build_request(request)


def test_build_variable_signature_uses_canonical_names() -> None:
    signature = build_variable_signature(
        [
            "total_precipitation",
            "2m_temperature",
            "volumetric_soil_water_layer_1",
            "surface_solar_radiation_downwards",
        ]
    )

    assert signature == (
        "2m_temperature_surface_solar_radiation_downwards_"
        "total_precipitation_volumetric_soil_water_layer_1"
    )
