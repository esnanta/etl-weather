"""Validate campaign identity and year bounds loaded from environment."""

from __future__ import annotations

import pytest

import config as orchestration_config


def test_required_campaign_environment_rejects_missing_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CAMPAIGN_KEY", raising=False)
    with pytest.raises(RuntimeError, match="CAMPAIGN_KEY is required"):
        orchestration_config._required_environment("CAMPAIGN_KEY")


def test_required_campaign_environment_strips_whitespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CAMPAIGN_KEY", "  era5_land_test  ")
    assert orchestration_config._required_environment("CAMPAIGN_KEY") == "era5_land_test"


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


def test_campaign_year_rejects_non_four_digit_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CAMPAIGN_START_YEAR", "16")
    with pytest.raises(RuntimeError, match="four-digit year"):
        orchestration_config._campaign_year("CAMPAIGN_START_YEAR")


def test_campaign_years_reject_inverted_range() -> None:
    with pytest.raises(RuntimeError, match="less than or equal"):
        orchestration_config._validate_campaign_years(2017, 2016)


def test_campaign_years_allow_one_full_year() -> None:
    orchestration_config._validate_campaign_years(2016, 2016)
