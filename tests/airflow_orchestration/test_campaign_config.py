"""Validate campaign identity and date bounds loaded from environment."""

from __future__ import annotations

from datetime import date

import pytest

from orchestration import config as orchestration_config


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


def test_loaded_campaign_bounds_are_full_calendar_years() -> None:
    assert orchestration_config.CAMPAIGN_KEY
    assert orchestration_config.CAMPAIGN_START_DATE <= orchestration_config.CAMPAIGN_END_DATE
    assert (
        orchestration_config.CAMPAIGN_START_DATE.month,
        orchestration_config.CAMPAIGN_START_DATE.day,
    ) == (1, 1)
    assert (
        orchestration_config.CAMPAIGN_END_DATE.month,
        orchestration_config.CAMPAIGN_END_DATE.day,
    ) == (12, 31)


def test_campaign_bounds_reject_inverted_range() -> None:
    with pytest.raises(RuntimeError, match="less than or equal"):
        orchestration_config._validate_campaign_bounds(
            date(2018, 1, 1),
            date(2017, 12, 31),
        )


def test_campaign_bounds_require_full_calendar_year_boundaries() -> None:
    with pytest.raises(RuntimeError, match="January 1"):
        orchestration_config._validate_campaign_bounds(
            date(2016, 4, 1),
            date(2017, 12, 31),
        )
    with pytest.raises(RuntimeError, match="December 31"):
        orchestration_config._validate_campaign_bounds(
            date(2016, 1, 1),
            date(2017, 9, 30),
        )


def test_campaign_bounds_allow_one_full_year() -> None:
    orchestration_config._validate_campaign_bounds(
        date(2016, 1, 1),
        date(2016, 12, 31),
    )
