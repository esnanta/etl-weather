from __future__ import annotations

import pytest

from config import (
    database_recreation_allowed,
    resolve_app_environment,
    validate_and_log_app_environment,
)
from pipeline.models.weather import DataSource
from pipeline.requests.factory import build_cds_request


def test_build_cds_request_requires_api_key(monkeypatch) -> None:
    # Isolate the missing-key scenario from a developer's real .env file.
    monkeypatch.setattr("pipeline.requests.factory.load_dotenv", lambda: False)
    monkeypatch.delenv("CDSAPI_KEY", raising=False)

    with pytest.raises(ValueError):
        build_cds_request()


def test_build_cds_request_uses_era5_land_contract(monkeypatch) -> None:
    monkeypatch.setenv("CDSAPI_KEY", "1:mock-key")

    request = build_cds_request()

    assert request.source is DataSource.CDS
    assert request.dataset == "reanalysis-era5-land"
    assert request.variables == ["tp", "t2m", "swvl1", "ssrd", "d2m"]
    assert request.bbox.north_lat == 6.1
    assert request.bbox.west_lon == 94.9
    assert request.bbox.south_lat == 1.9
    assert request.bbox.east_lon == 98.3


def test_build_cds_request_accepts_explicit_period_without_secret_check(
    monkeypatch,
) -> None:
    monkeypatch.delenv("CDSAPI_KEY", raising=False)

    request = build_cds_request(
        "2016-01-01",
        "2025-12-31",
        require_api_key=False,
    )

    assert request.start_date == "2016-01-01"
    assert request.end_date == "2025-12-31"



@pytest.mark.parametrize(
    ("value", "expected"),
    [("dev", "dev"), ("DEV", "dev"), ("prod", "prod"), ("PROD", "prod")],
)
def test_application_environment_accepts_only_explicit_modes(value, expected) -> None:
    assert resolve_app_environment({"APP_ENV": value}) == expected


def test_application_environment_requires_explicit_value() -> None:
    with pytest.raises(ValueError, match="APP_ENV is required"):
        resolve_app_environment({})
    with pytest.raises(ValueError, match="APP_ENV is required"):
        database_recreation_allowed({})


def test_database_recreation_is_allowed_only_in_dev() -> None:
    assert database_recreation_allowed({"APP_ENV": "dev"}) is True
    assert database_recreation_allowed({"APP_ENV": "prod"}) is False


def test_application_environment_rejects_ambiguous_values() -> None:
    with pytest.raises(
        ValueError,
        match="Set APP_ENV=dev or APP_ENV=prod",
    ):
        resolve_app_environment({"APP_ENV": "development"})



def test_application_environment_rejects_blank_value() -> None:
    with pytest.raises(ValueError, match="APP_ENV is required"):
        resolve_app_environment({"APP_ENV": "   "})


def test_application_environment_logs_valid_mode(caplog) -> None:
    import logging

    logger = logging.getLogger("test.app_env")
    with caplog.at_level(logging.INFO, logger="test.app_env"):
        assert validate_and_log_app_environment(
            logger, {"APP_ENV": "dev"}
        ) == "dev"
    assert "Application environment | APP_ENV=dev" in caplog.text


def test_application_environment_logs_warning_before_abort(caplog) -> None:
    import logging

    logger = logging.getLogger("test.app_env.invalid")
    with caplog.at_level(logging.WARNING, logger="test.app_env.invalid"):
        with pytest.raises(ValueError, match="Set APP_ENV=dev or APP_ENV=prod"):
            validate_and_log_app_environment(logger, {})
    assert "Application startup aborted" in caplog.text
