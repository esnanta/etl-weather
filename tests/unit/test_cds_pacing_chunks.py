"""Prove that a deferred monthly extraction resumes its completed CDS chunks.

The first chunk must remain reusable when the second submission is postponed,
so pacing does not cause duplicate provider requests or mutate final RAW data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import sys

import pytest
import xarray as xr

from pipeline.extraction import cds
from pipeline.model import BoundingBox, DataSource, WeatherRequest


def _request(start: str, end: str) -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date=start,
        end_date=end,
        variables=["tp"],
        bbox=BoundingBox(
            north_lat=6.1, south_lat=1.9, west_lon=94.9, east_lon=98.3
        ),
    )


def test_second_chunk_defers_and_resumes_without_retrieving_first(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    calls: list[str] = []
    blocked = True
    retrieval = _request("2023-12-31", "2024-01-02")
    target = _request("2024-01-01", "2024-01-02")

    def guard(chunk: WeatherRequest) -> None:
        if chunk.start_date == "2024-01-01" and blocked:
            raise cds.CdsSubmissionDeferred(
                datetime(2024, 1, 2, tzinfo=timezone.utc)
            )

    def download(chunk, *, purpose, before_submission=None):
        assert before_submission is guard
        before_submission(chunk)
        calls.append(chunk.start_date)
        return xr.Dataset(
            {"tp": ("valid_time", [1.0])},
            coords={"valid_time": [chunk.start_date]},
        )

    monkeypatch.setattr(cds, "_download_cds", download)
    cache = tmp_path / "retrieval"
    with pytest.raises(cds.CdsSubmissionDeferred):
        cds._download_cds_period(
            retrieval, target_request=target,
            before_submission=guard, chunk_cache_directory=cache,
        )
    assert calls == ["2023-12-31"]
    assert len(list(cache.glob("*.nc"))) == 1

    blocked = False
    result = cds._download_cds_period(
        retrieval, target_request=target,
        before_submission=guard, chunk_cache_directory=cache,
    )
    assert calls == ["2023-12-31", "2024-01-01"]
    assert result.sizes["valid_time"] == 2
    cds._download_cds_period(
        retrieval, target_request=target,
        before_submission=guard, chunk_cache_directory=cache,
    )
    assert len(calls) == 2


def test_corrupt_chunk_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    request = _request("2024-02-01", "2024-02-02")
    cache = tmp_path / "retrieval"
    cache.mkdir()
    from pipeline.request import build_request_key

    xr.Dataset(
        {"tp": ("valid_time", [1.0])},
        coords={"valid_time": ["2024-02-01"]},
        attrs={"retrieval_request_key": "incorrect"},
    ).to_netcdf(cache / f"{build_request_key(request)}.nc")
    monkeypatch.setattr(
        cds, "_download_cds", lambda *args, **kwargs: pytest.fail("Unexpected retrieve")
    )
    with pytest.raises(ValueError, match="identity"):
        cds._download_cds_period(
            request, target_request=request, chunk_cache_directory=cache
        )


def test_guard_precedes_actual_post(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.session = kwargs["session"]

        def retrieve(self, *args):
            self.session.post("https://cds.example/resources/reanalysis-era5-land")

    def transport(_session, method, url, *args, **kwargs):
        calls.append(method)
        return None

    import requests

    monkeypatch.setattr(requests.Session, "request", transport)
    monkeypatch.setitem(sys.modules, "cdsapi", SimpleNamespace(Client=FakeClient))
    monkeypatch.setattr(cds, "_resolve_cds_credentials", lambda: ("url", "key"))

    def denied(_request):
        raise cds.CdsSubmissionDeferred(
            datetime(2026, 9, 26, tzinfo=timezone.utc)
        )

    with pytest.raises(cds.CdsSubmissionDeferred):
        cds._download_cds(
            _request("2024-02-01", "2024-02-01"),
            purpose="target period", before_submission=denied,
        )
    assert calls == []


def test_internal_post_retry_requires_new_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    events: list[str] = []

    def transport(_session, method, url, *args, **kwargs):
        events.append(method.upper())
        return None

    monkeypatch.setattr(requests.Session, "request", transport)
    request = _request("2024-02-01", "2024-02-01")

    def reserve(chunk: WeatherRequest) -> None:
        assert chunk is request
        events.append("reserve")
        if events.count("reserve") == 2:
            raise cds.CdsSubmissionDeferred(
                datetime(2026, 9, 26, tzinfo=timezone.utc)
            )

    session = cds._PacedSubmissionSession(request, reserve)
    session.get("https://cds.example/status")
    session.get("https://cdn.example/download.nc")
    session.post("https://cds.example/resources/reanalysis-era5-land")
    with pytest.raises(cds.CdsSubmissionDeferred):
        session.post("https://cds.example/resources/reanalysis-era5-land")

    assert events == ["GET", "GET", "reserve", "POST", "reserve"]


def test_real_cdsapi_retry_is_deferred_before_second_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the pinned library's robust retry through the paced session."""
    import cdsapi
    import requests

    events: list[str] = []
    request = _request("2024-06-01", "2024-06-30")

    def reserve(chunk: WeatherRequest) -> None:
        assert chunk is request
        events.append("reserve")
        if events.count("reserve") == 2:
            raise cds.CdsSubmissionDeferred(
                datetime(2026, 9, 26, tzinfo=timezone.utc)
            )

    def transport(_session, method, url, *args, **kwargs):
        events.append(method.upper())
        if method.upper() == "POST":
            raise requests.exceptions.ReadTimeout("simulated submission timeout")
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {},
        )

    monkeypatch.setattr(requests.Session, "request", transport)
    client = cdsapi.Client(
        url="https://cds.example/api", key="123:fake",
        session=cds._PacedSubmissionSession(request, reserve),
        quiet=True, retry_max=3, sleep_max=0, forget=True,
    )

    with pytest.raises(cds.CdsSubmissionDeferred):
        client.retrieve("reanalysis-era5-land", {"variable": ["tp"]})

    assert events == ["GET", "reserve", "POST", "reserve"]
