from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from pipeline.extract import (
    CdsExtractor,
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


def test_cds_extractor_integrates_context_normalization_and_selection(
    monkeypatch,
) -> None:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset=" REANALYSIS-ERA5-LAND ",
        start_date="2022-01-02",
        end_date="2022-01-02",
        variables=[" T2M ", "tp"],
        bbox=_request().bbox,
    )
    retrieve_calls: list[tuple[str, dict[str, object], Path]] = []

    class FakeCdsClient:
        def __init__(self, *, url: str, key: str, quiet: bool) -> None:
            assert url == "https://example.invalid/api"
            assert key == "1:offline-test-key"
            assert quiet is True

        def retrieve(
            self,
            dataset_name: str,
            payload: dict[str, object],
            target: str,
        ) -> None:
            target_path = Path(target)
            retrieve_calls.append((dataset_name, payload, target_path))
            xr.Dataset(
                {
                    "2m_temperature": (
                        ("time", "latitude", "longitude"),
                        np.array([[[300.0]], [[301.0]]]),
                    ),
                    "total_precipitation": (
                        ("time", "latitude", "longitude"),
                        np.array([[[0.001]], [[0.002]]]),
                    ),
                    "snow_depth": (
                        ("time", "latitude", "longitude"),
                        np.array([[[0.0]], [[0.0]]]),
                    ),
                },
                coords={
                    "time": np.array(
                        ["2022-01-01T00:00:00", "2022-01-02T00:00:00"],
                        dtype="datetime64[ns]",
                    ),
                    "latitude": [5.2],
                    "longitude": [96.1],
                },
            ).to_netcdf(target_path)

    monkeypatch.setitem(
        sys.modules,
        "cdsapi",
        SimpleNamespace(Client=FakeCdsClient),
    )
    monkeypatch.setattr("pipeline.extract.load_dotenv", lambda: False)
    monkeypatch.setenv("CDSAPI_URL", "https://example.invalid/api")
    monkeypatch.setenv("CDSAPI_KEY", "1:offline-test-key")

    extracted = CdsExtractor().extract(request)

    assert len(retrieve_calls) == 1
    dataset_name, payload, temporary_path = retrieve_calls[0]
    assert dataset_name == "reanalysis-era5-land"
    assert payload["day"] == ["01", "02"]
    assert payload["variable"] == ["2m_temperature", "total_precipitation"]
    assert payload["area"] == [5.2, 96.1, 3.4, 97.6]
    assert not temporary_path.exists()

    assert extracted.request.dataset == "reanalysis-era5-land"
    assert extracted.request.start_date == "2022-01-02"
    assert extracted.request.end_date == "2022-01-02"
    assert extracted.request.variables == ["t2m", "tp"]
    assert list(extracted.raw_data.data_vars) == ["t2m", "tp"]
    assert "valid_time" in extracted.raw_data.coords
    assert "time" not in extracted.raw_data.coords
    assert "snow_depth" not in extracted.raw_data.data_vars
    assert extracted.downloaded_at.tzinfo is not None
