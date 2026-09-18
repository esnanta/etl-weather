from __future__ import annotations

import pytest

from pipeline.extract import (
    _build_cds_request_payload,
    _build_era5_land_retrieval_request,
)
from pipeline.model import BoundingBox, DataSource, WeatherRequest


def _request(
    *,
    start_date: str = "2022-01-01",
    end_date: str = "2022-01-03",
) -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date=start_date,
        end_date=end_date,
        variables=["tp", "t2m", "d2m", "swvl1", "ssrd"],
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )


def test_era5_land_retrieval_adds_previous_day_context() -> None:
    request = _request()

    retrieval_request = _build_era5_land_retrieval_request(request)

    assert retrieval_request.start_date == "2021-12-31"
    assert retrieval_request.end_date == request.end_date
    assert retrieval_request.variables == request.variables
    assert retrieval_request.bbox == request.bbox
    assert request.start_date == "2022-01-01"


def test_era5_land_retrieval_does_not_precede_dataset_start() -> None:
    request = _request(start_date="1950-01-01", end_date="1950-01-01")

    retrieval_request = _build_era5_land_retrieval_request(request)

    assert retrieval_request.start_date == "1950-01-01"
    assert retrieval_request.end_date == "1950-01-01"


def test_cds_payload_uses_hourly_era5_land_request_contract() -> None:
    payload = _build_cds_request_payload(_request())

    assert payload == {
        "variable": [
            "total_precipitation",
            "2m_temperature",
            "2m_dewpoint_temperature",
            "volumetric_soil_water_layer_1",
            "surface_solar_radiation_downwards",
        ],
        "year": "2022",
        "month": "01",
        "day": ["01", "02", "03"],
        "time": [f"{hour:02d}:00" for hour in range(24)],
        "area": [5.2, 96.1, 3.4, 97.6],
        "data_format": "netcdf",
        "download_format": "unarchived",
    }


def test_cds_payload_rejects_interval_across_calendar_months() -> None:
    request = _request(start_date="2022-01-31", end_date="2022-02-01")

    with pytest.raises(ValueError, match="must stay in one month"):
        _build_cds_request_payload(request)
