from __future__ import annotations

import pytest

from pipeline.model import BoundingBox, DataSource, WeatherRequest
from pipeline.utils import build_request, build_request_key, build_variable_signature


def _bbox() -> BoundingBox:
    return BoundingBox(
        north_lat=6.2,
        south_lat=1.8,
        east_lon=98.6,
        west_lon=94.8,
    )


def _request(variables: list[str] | None = None) -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-07",
        variables=variables or ["tp", "t2m", "d2m", "swvl1", "ssrd"],
        bbox=_bbox(),
    )


def test_build_request_keeps_provider_variable_names() -> None:
    request = _request()
    normalized_request = build_request(request)
    assert normalized_request.variables == request.variables


def test_build_request_rejects_long_api_variable_name() -> None:
    request = _request(["total_precipitation"])
    with pytest.raises(ValueError, match="provider variable name"):
        build_request(request)


def test_build_request_rejects_unknown_cds_variable() -> None:
    request = _request(["unknown_weather_variable"])
    with pytest.raises(ValueError):
        build_request(request)


def test_build_request_rejects_duplicate_variables() -> None:
    request = _request(["tp", "tp"])
    with pytest.raises(ValueError, match="Duplicate variables"):
        build_request(request)


def test_build_variable_signature_uses_provider_names() -> None:
    signature = build_variable_signature(["tp", "t2m", "swvl1", "ssrd"])
    assert signature == "ssrd_swvl1_t2m_tp"


def test_build_request_key_is_stable_when_variable_order_changes() -> None:
    first = _request(["tp", "t2m", "d2m"])
    second = _request(["d2m", "tp", "t2m"])

    assert build_request_key(first) == build_request_key(second)


def test_build_request_key_changes_when_partition_identity_changes() -> None:
    first = _request(["tp", "t2m"])
    second = WeatherRequest(
        source=first.source,
        dataset=first.dataset,
        start_date=first.start_date,
        end_date="2024-01-08",
        variables=first.variables,
        bbox=first.bbox,
    )

    assert build_request_key(first) != build_request_key(second)
