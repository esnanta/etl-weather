from __future__ import annotations

import pytest

from pipeline.model import DataSource
from scripts.config import (
    build_arco_request,
    build_cds_request,
    build_request_from_env,
)


def test_build_arco_request_defaults() -> None:
    request = build_arco_request()

    assert request.source is DataSource.ARCO
    assert request.dataset == "era5-single-levels"
    assert request.bbox.north_lat == 6.2
    assert request.bbox.west_lon == 94.8
    assert request.bbox.south_lat == 1.8
    assert request.bbox.east_lon == 98.6


def test_build_cds_request_requires_api_key(monkeypatch) -> None:
    monkeypatch.delenv("CDSAPI_KEY", raising=False)

    with pytest.raises(ValueError):
        build_cds_request()


def test_build_request_from_env_arco(monkeypatch) -> None:
    monkeypatch.setenv("WEATHER_DATA_SOURCE", "arco")

    request = build_request_from_env()

    assert request.source is DataSource.ARCO


def test_build_request_from_env_cds(monkeypatch) -> None:
    monkeypatch.setenv("WEATHER_DATA_SOURCE", "cds")
    monkeypatch.setenv("CDSAPI_KEY", "1:mock-key")

    request = build_request_from_env()

    assert request.source is DataSource.CDS


def test_build_request_from_env_invalid_source(monkeypatch) -> None:
    monkeypatch.setenv("WEATHER_DATA_SOURCE", "invalid-source")

    with pytest.raises(ValueError):
        build_request_from_env()
