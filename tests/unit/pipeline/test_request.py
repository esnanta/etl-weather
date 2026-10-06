from __future__ import annotations

import pytest

from pipeline.models.weather import BoundingBox, DataSource, WeatherRequest
from pipeline.requests.weather import (
    build_request,
    build_request_key,
    build_variable_signature,
    split_request_by_month,
    weather_request_from_spec,
    weather_request_to_spec,
)


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


def test_build_request_key_changes_when_bounding_box_changes() -> None:
    first = _request(["tp", "t2m"])
    second = WeatherRequest(
        source=first.source,
        dataset=first.dataset,
        start_date=first.start_date,
        end_date=first.end_date,
        variables=first.variables,
        bbox=BoundingBox(
            north_lat=5.2,
            south_lat=3.4,
            east_lon=97.6,
            west_lon=96.1,
        ),
    )

    assert build_request_key(first) != build_request_key(second)


def test_split_request_by_month_preserves_partial_period_boundaries() -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-20",
        end_date="2024-03-10",
        variables=["tp"],
        bbox=_bbox(),
    )

    monthly_requests = split_request_by_month(request)

    assert [
        (monthly.start_date, monthly.end_date)
        for monthly in monthly_requests
    ] == [
        ("2024-01-20", "2024-01-31"),
        ("2024-02-01", "2024-02-29"),
        ("2024-03-01", "2024-03-10"),
    ]
    assert all(monthly.source is DataSource.CDS for monthly in monthly_requests)
    assert all(monthly.dataset == request.dataset for monthly in monthly_requests)
    assert all(monthly.variables == request.variables for monthly in monthly_requests)
    assert all(monthly.bbox == request.bbox for monthly in monthly_requests)


def test_weather_request_spec_round_trip_is_json_safe() -> None:
    request = _request()

    spec = weather_request_to_spec(request)
    restored = weather_request_from_spec(spec)

    assert restored == request
    assert spec == {
        "source": "cds",
        "dataset": "reanalysis-era5-land",
        "start_date": "2024-01-01",
        "end_date": "2024-01-07",
        "variables": ["tp", "t2m", "d2m", "swvl1", "ssrd"],
        "bbox": {
            "north_lat": 6.2,
            "west_lon": 94.8,
            "south_lat": 1.8,
            "east_lon": 98.6,
        },
    }


def test_weather_request_from_spec_rejects_missing_field() -> None:
    spec = weather_request_to_spec(_request())
    del spec["start_date"]

    with pytest.raises(ValueError, match="missing 'start_date'"):
        weather_request_from_spec(spec)


def test_weather_request_from_spec_rejects_non_numeric_bbox() -> None:
    spec = weather_request_to_spec(_request())
    bbox = spec["bbox"]
    assert isinstance(bbox, dict)
    bbox["north_lat"] = "6.2"

    with pytest.raises(ValueError, match="coordinates must be numeric"):
        weather_request_from_spec(spec)
