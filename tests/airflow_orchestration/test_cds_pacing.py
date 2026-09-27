"""Protect the operational CDS interval contract before task integration.

Pacing settings must stay independent of campaign identity and reject values
that could silently remove the intended delay.
"""

import pytest

from orchestration.repositories.cds_pacing import (
    DEFAULT_INTERVAL_SECONDS,
    INTERVAL_ENV,
    submission_interval_seconds,
)


def test_default_interval_is_conservative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(INTERVAL_ENV, raising=False)
    assert submission_interval_seconds() == DEFAULT_INTERVAL_SECONDS == 1800


@pytest.mark.parametrize("value", ["", "1.5", "-1", "unlimited"])
def test_reject_invalid_interval(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv(INTERVAL_ENV, value)
    with pytest.raises(ValueError, match=INTERVAL_ENV):
        submission_interval_seconds()


def test_interval_is_operational_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(INTERVAL_ENV, "900")
    assert submission_interval_seconds() == 900
