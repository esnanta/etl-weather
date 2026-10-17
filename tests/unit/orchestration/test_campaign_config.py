"""Validate campaign identity and year bounds loaded from environment."""

from __future__ import annotations

import pytest

import config as orchestration_config


def test_required_campaign_environment_rejects_missing_value() -> None:
    with pytest.raises(RuntimeError, match="CAMPAIGN_KEY is required"):
        orchestration_config._required_environment("CAMPAIGN_KEY", {})


def test_required_campaign_environment_strips_whitespace() -> None:
    assert (
        orchestration_config._required_environment(
            "CAMPAIGN_KEY",
            {"CAMPAIGN_KEY": "  era5_land_test  "},
        )
        == "era5_land_test"
    )


def test_loaded_campaign_years_define_full_calendar_years() -> None:
    assert orchestration_config.CAMPAIGN_KEY
    assert (
        orchestration_config.CAMPAIGN_START_YEAR
        <= orchestration_config.CAMPAIGN_END_YEAR
    )
    assert orchestration_config.CAMPAIGN_START_DATE.month == 1
    assert orchestration_config.CAMPAIGN_START_DATE.day == 1
    assert orchestration_config.CAMPAIGN_END_DATE.month == 12
    assert orchestration_config.CAMPAIGN_END_DATE.day == 31


def test_campaign_year_rejects_non_four_digit_value() -> None:
    with pytest.raises(RuntimeError, match="four-digit year"):
        orchestration_config._campaign_year(
            "CAMPAIGN_START_YEAR",
            {"CAMPAIGN_START_YEAR": "16"},
        )


def test_campaign_years_reject_inverted_range() -> None:
    with pytest.raises(RuntimeError, match="less than or equal"):
        orchestration_config._validate_campaign_years(2017, 2016)


def test_campaign_years_allow_one_full_year() -> None:
    orchestration_config._validate_campaign_years(2016, 2016)



def test_root_config_import_does_not_require_campaign_environment() -> None:
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env.pop("CAMPAIGN_KEY", None)
    env.pop("CAMPAIGN_START_YEAR", None)
    env.pop("CAMPAIGN_END_YEAR", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import config; print(config.APP_ENV_KEY)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "APP_ENV"


def test_database_schema_import_does_not_require_campaign_environment() -> None:
    """Import the actual database configuration with campaign settings absent."""
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    for key in ("CAMPAIGN_KEY", "CAMPAIGN_START_YEAR", "CAMPAIGN_END_YEAR"):
        env.pop(key, None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from database.config import APPLICATION_SCHEMA; "
            "print(APPLICATION_SCHEMA)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "climate_data"
