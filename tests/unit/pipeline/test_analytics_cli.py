"""Test source-driven analytics CLI preview, dispatch, and input failures.

Only discovery, validation and export boundaries are stubbed; real artifact
selection and fidelity are covered by offline pipeline integration-style tests.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.analytics.selection import AnalyticsSelection, SourceMonth
from scripts import run_analytics


def _year(year=2024):
    return AnalyticsSelection(
        "daily", ("t2m", "tp"), date(year, 5, 1), date(year, 5, 31),
        (SourceMonth(Path(f"{year}.parquet"), date(year, 5, 1), date(year, 5, 31)),),
    )


def test_cli_discovers_all_available_months_without_campaign_dates(monkeypatch, capsys):
    seen = []
    def discover(resolution, variables):
        seen.append((resolution, variables))
        return (_year(),)
    monkeypatch.setattr(run_analytics, "discover_available_sources", discover)
    monkeypatch.setattr(run_analytics, "validate_sources", lambda plan: None)
    assert run_analytics.main(["--resolution", "daily", "--variables", "tp,t2m"]) == 0
    assert seen == [("daily", "tp,t2m")]
    stdout = capsys.readouterr().out
    assert "2024-05" in stdout
    assert "Preview only" in stdout


def test_cli_rejects_obsolete_date_and_year_flags():
    for old in ("--start-date", "--end-date", "--start-year", "--end-year"):
        with pytest.raises(SystemExit) as error:
            run_analytics.main(["--resolution", "daily", old, "2024-01-01"])
        assert error.value.code == 2


def test_cli_rejects_missing_inputs(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError("no source checkpoints")
    monkeypatch.setattr(run_analytics, "discover_available_sources", missing)
    with pytest.raises(SystemExit) as error:
        run_analytics.main(["--resolution", "daily"])
    assert error.value.code == 2


def test_cli_execute_dispatches_each_year_after_global_validation(monkeypatch, tmp_path, capsys):
    calls = []
    monkeypatch.setattr(
        run_analytics, "discover_available_sources", lambda *args: (_year(2023), _year(2024)),
    )
    monkeypatch.setattr(run_analytics, "validate_sources", lambda plan: calls.append(
        ("validate", len(plan.months))
    ))
    monkeypatch.setattr(run_analytics, "export_selection", lambda plan, output_directory: (
        calls.append(("export", plan.start.year, output_directory))
        or ((output_directory / f"{plan.start.year}_daily.parquet", 2),)
    ))
    assert run_analytics.main([
        "--resolution", "daily", "--output", str(tmp_path), "--execute",
    ]) == 0
    assert calls == [
        ("validate", 2),
        ("export", 2023, tmp_path),
        ("export", 2024, tmp_path),
    ]
    assert "Analytics export verification and manifest publication completed" in capsys.readouterr().out


def test_cli_preview_never_calls_export(monkeypatch):
    monkeypatch.setattr(
        run_analytics, "discover_available_sources", lambda *args: (_year(),),
    )
    monkeypatch.setattr(run_analytics, "validate_sources", lambda plan: None)
    monkeypatch.setattr(
        run_analytics, "export_selection",
        lambda *args, **kwargs: pytest.fail("Preview must not export"),
    )
    assert run_analytics.main(["--resolution", "daily"]) == 0
