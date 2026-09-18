from __future__ import annotations

import pytest

from pipeline.model import DataSource
from scripts.config import (
    build_cds_request,
)


def test_build_cds_request_requires_api_key(monkeypatch) -> None:
    # Isolate the missing-key scenario from a developer's real .env file.
    monkeypatch.setattr("scripts.config.load_dotenv", lambda: False)
    monkeypatch.delenv("CDSAPI_KEY", raising=False)

    with pytest.raises(ValueError):
        build_cds_request()


def test_build_cds_request_uses_era5_land_contract(monkeypatch) -> None:
    monkeypatch.setenv("CDSAPI_KEY", "1:mock-key")

    request = build_cds_request()

    assert request.source is DataSource.CDS
    assert request.dataset == "reanalysis-era5-land"
    assert request.variables == ["tp", "t2m", "swvl1", "ssrd", "d2m"]
    assert request.bbox.north_lat == 5.2
    assert request.bbox.west_lon == 96.1
    assert request.bbox.south_lat == 3.4
    assert request.bbox.east_lon == 97.6
