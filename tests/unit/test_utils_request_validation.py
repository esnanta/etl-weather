from __future__ import annotations

import pytest

from pipeline.model import BoundingBox, DataSource, WeatherRequest
from pipeline.utils import (
    CANONICAL_VARIABLE_NAMES,
    build_request,
    build_variable_signature,
    canonicalize_variable_name,
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
        dataset="reanalysis-era5-single-levels",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=[
            "total_precipitation",
            "surface_solar_radiation_downwards",
        ],
        bbox=_bbox(),
    )

    normalized_request = build_request(request)

    assert normalized_request.dataset == "reanalysis-era5-single-levels"
    assert normalized_request.variables == [
        "total_precipitation",
        "surface_solar_radiation_downwards",
    ]


def test_build_request_normalizes_provider_aliases_to_canonical_names() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["tp", "t2m", "d2m", "swvl1", "ssrd"],
        bbox=_bbox(),
    )

    normalized_request = build_request(request)

    assert normalized_request.variables == [
        "total_precipitation",
        "2m_temperature",
        "2m_dewpoint_temperature",
        "volumetric_soil_water_layer_1",
        "surface_solar_radiation_downwards",
    ]
    assert set(normalized_request.variables).issubset(CANONICAL_VARIABLE_NAMES)


def test_build_request_rejects_unknown_cds_variable() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-single-levels",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["unknown_weather_variable"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError):
        build_request(request)


def test_build_request_rejects_duplicate_canonical_variables_after_normalization() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["tp", "total_precipitation"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError):
        build_request(request)


def test_canonicalize_variable_name_is_stable_for_canonical_name() -> None:
    assert canonicalize_variable_name("total_precipitation") == "total_precipitation"
    assert canonicalize_variable_name("TP") == "total_precipitation"


def test_build_variable_signature_uses_short_names_only_for_artifacts() -> None:
    signature = build_variable_signature(
        [
            "total_precipitation",
            "2m_temperature",
            "volumetric_soil_water_layer_1",
            "surface_solar_radiation_downwards",
        ]
    )

    assert signature == "t2m_ssrd_tp_swvl1"
