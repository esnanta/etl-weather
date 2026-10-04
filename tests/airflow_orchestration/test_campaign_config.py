"""Validate campaign identity and bounds loaded from environment."""

from __future__ import annotations

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

    assert (
        orchestration_config._required_environment("CAMPAIGN_KEY")
        == "era5_land_test"
    )


def test_campaign_year_requires_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CAMPAIGN_START_YEAR", "twenty-sixteen")

    with pytest.raises(RuntimeError, match="CAMPAIGN_START_YEAR must be an integer"):
        orchestration_config._campaign_year("CAMPAIGN_START_YEAR")


def test_loaded_campaign_bounds_match_environment() -> None:
    assert orchestration_config.CAMPAIGN_KEY
    assert orchestration_config.CAMPAIGN_START_YEAR <= orchestration_config.CAMPAIGN_END_YEAR
