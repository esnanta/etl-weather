from __future__ import annotations

import pytest

from pipeline.model import BoundingBox, DataSource, WeatherRequest
from pipeline.utils import build_request, build_variable_signature


def test_build_request_keeps_explicit_cds_dataset_and_variables() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-single-levels",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=[
            "total_precipitation",
            "surface_solar_radiation_downwards",
        ],
        bbox=BoundingBox(
            north_lat=6.2,
            south_lat=1.8,
            east_lon=98.6,
            west_lon=94.8,
        ),
    )

    normalized_request = build_request(request)

    assert normalized_request.dataset == "reanalysis-era5-single-levels"
    assert normalized_request.variables == [
        "total_precipitation",
        "surface_solar_radiation_downwards",
    ]


def test_build_request_rejects_unknown_cds_variable() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-single-levels",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["tp"],
        bbox=BoundingBox(
            north_lat=6.2,
            south_lat=1.8,
            east_lon=98.6,
            west_lon=94.8,
        ),
    )

    with pytest.raises(ValueError):
        build_request(request)


def test_build_variable_signature_uses_short_variable_names() -> None:
    signature = build_variable_signature(
        [
            "total_precipitation",
            "2m_temperature",
            "volumetric_soil_water_layer_1",
            "surface_solar_radiation_downwards",
        ]
    )

    assert signature == "t2m_ssrd_tp_swvl1"
