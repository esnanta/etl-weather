from __future__ import annotations

import pytest

from pipeline.model import BoundingBox, DataSource, WeatherRequest
from pipeline.utils import build_request, build_variable_signature


def _bbox() -> BoundingBox:
    return BoundingBox(
        north_lat=6.2,
        south_lat=1.8,
        east_lon=98.6,
        west_lon=94.8,
    )


def test_build_request_keeps_provider_variable_names() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["tp", "t2m", "d2m", "swvl1", "ssrd"],
        bbox=_bbox(),
    )

    normalized_request = build_request(request)
    assert normalized_request.variables == request.variables


def test_build_request_rejects_long_api_variable_name() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=["total_precipitation"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError, match="provider variable name"):
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
        variables=["tp", "tp"],
        bbox=_bbox(),
    )

    with pytest.raises(ValueError, match="Duplicate variables"):
        build_request(request)


def test_build_variable_signature_uses_provider_names() -> None:
    signature = build_variable_signature(["tp", "t2m", "swvl1", "ssrd"])
    assert signature == "ssrd_swvl1_t2m_tp"
