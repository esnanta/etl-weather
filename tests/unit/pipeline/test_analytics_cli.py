"""Test analytics CLI preview and explicit P3 execution routing.

Source discovery and actual file writes are isolated because this module
tests CLI dispatch. Production selection and Parquet integrity have separate
offline tests, without live CDS or PostgreSQL.
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


def test_cli_rejects_obsolete_year_flags():
    with pytest.raises(SystemExit) as error:
        run_analytics.main([
            "--resolution", "hourly",
            "--start-date", "2024-05-01",
            "--end-date", "2024-06-30",
            "--start-year", "2024",
        ])
    assert error.value.code == 2


def test_cli_requires_explicit_date_bounds():
    with pytest.raises(SystemExit) as error:
        run_analytics.main(["--resolution", "hourly"])
    assert error.value.code == 2


def test_cli_rejects_reversed_dates():
    with pytest.raises(SystemExit) as error:
        run_analytics.main([
            "--resolution", "hourly",
            "--start-date", "2024-06-30",
            "--end-date", "2024-05-01",
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


def test_cli_execute_dispatches_after_validation(monkeypatch, tmp_path, capsys):
    calls = []

    def discover(*args):
        calls.append("discover")
        return AnalyticsSelection(
            "daily", ("tp",), date(2024, 5, 1), date(2024, 5, 31),
            (SourceMonth(Path("fixture.parquet"), date(2024, 5, 1), date(2024, 5, 31)),),
        )

    monkeypatch.setattr(run_analytics, "discover_sources", discover)
    monkeypatch.setattr(run_analytics, "validate_sources", lambda plan: calls.append("validate"))
    monkeypatch.setattr(
        run_analytics, "export_selection",
        lambda plan, output_directory: (
            calls.append(("export", output_directory))
            or ((output_directory / "2024.parquet", 2),)
        ),
    )
    assert run_analytics.main([
        "--resolution", "daily",
        "--start-date", "2024-05-01",
        "--end-date", "2024-05-31",
        "--output", str(tmp_path),
        "--execute",
    ]) == 0
    assert calls == ["discover", "validate", ("export", tmp_path)]
    assert "P3 export completed" in capsys.readouterr().out


def test_cli_preview_never_calls_export(monkeypatch):
    monkeypatch.setattr(
        run_analytics, "discover_sources",
        lambda *args: AnalyticsSelection(
            "daily", ("tp",), date(2024, 5, 1), date(2024, 5, 31),
            (SourceMonth(Path("fixture.parquet"), date(2024, 5, 1), date(2024, 5, 31)),),
        ),
    )
    monkeypatch.setattr(run_analytics, "validate_sources", lambda plan: None)
    monkeypatch.setattr(
        run_analytics, "export_selection",
        lambda *args, **kwargs: pytest.fail("Preview must not export"),
    )
    assert run_analytics.main([
        "--resolution", "daily",
        "--start-date", "2024-05-01",
        "--end-date", "2024-05-31",
    ]) == 0
