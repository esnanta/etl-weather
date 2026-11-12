"""Test the P2 analytics CLI as a read-only source-plan entrypoint.

Only the source-discovery boundary is replaced. The parser, date resolution,
and preview behavior are real; source content validation is covered by the
offline selection tests. P3 owns tests for actual Parquet writes.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.analytics.selection import AnalyticsSelection, SourceMonth
from scripts import run_analytics


def test_cli_explicit_months_does_not_require_campaign_config(monkeypatch, capsys):
    seen = []

    def discover(resolution, variables, start, end):
        seen.append((resolution, variables, start, end))
        return AnalyticsSelection(
            resolution,
            ("t2m", "tp"),
            start,
            end,
            (SourceMonth(Path("fixture.nc"), date(2024, 5, 1), date(2024, 5, 31)),),
        )

    monkeypatch.setattr(run_analytics, "discover_sources", discover)
    monkeypatch.setattr(run_analytics, "validate_sources", lambda plan: None)
    assert run_analytics.main([
        "--resolution", "hourly",
        "--variables", "tp,t2m",
        "--start-date", "2024-05-01",
        "--end-date", "2024-06-30",
    ]) == 0
    assert seen == [
        ("hourly", "tp,t2m", date(2024, 5, 1), date(2024, 6, 30))
    ]
    assert "Preview only" in capsys.readouterr().out


def test_cli_rejects_mixed_date_year_flags():
    with pytest.raises(SystemExit) as error:
        run_analytics.main([
            "--resolution", "hourly",
            "--start-date", "2024-05-01",
            "--end-date", "2024-06-30",
            "--start-year", "2024",
        ])
    assert error.value.code == 2


def test_cli_rejects_missing_inputs(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError("missing June checkpoint")

    monkeypatch.setattr(run_analytics, "discover_sources", missing)
    with pytest.raises(SystemExit) as error:
        run_analytics.main([
            "--resolution", "daily",
            "--start-date", "2024-06-01",
            "--end-date", "2024-06-30",
        ])
    assert error.value.code == 2


def test_cli_has_no_execute_option():
    with pytest.raises(SystemExit) as error:
        run_analytics.main(["--resolution", "hourly", "--execute"])
    assert error.value.code == 2
