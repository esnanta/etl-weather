"""Verify P3 Parquet writing against real production selection and synthetic sources.

Synthetic NetCDF and transformed Parquet fixtures replace CDS and PostgreSQL.
P4 validates checksum/provenance and safely adopts already published outputs.
These tests protect row fidelity, period ownership and failed-write safety.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pipeline.analytics import discover_sources, validate_sources
from pipeline.analytics import export as analytics_export
from pipeline.analytics.export import export_selection, output_paths
from tests.unit.pipeline.test_analytics_selection import _daily_month, _raw_month


def _plan(tmp_path, resolution, start=date(2024, 5, 1), end=date(2024, 5, 31)):
    selection = discover_sources(
        resolution, "tp,t2m", start, end, data_directory=tmp_path
    )
    validate_sources(selection)
    return selection


def test_partial_hourly_export_excludes_context_and_preserves_values(tmp_path):
    _raw_month(tmp_path, 5)
    selection = _plan(tmp_path, "hourly", date(2024, 5, 2), date(2024, 5, 2))
    target = output_paths(selection, data_directory=tmp_path)[0]
    assert target.name == "2024_hourly.parquet"
    assert target.parent.name == "hourly"

    exported = export_selection(selection, data_directory=tmp_path)
    assert exported == ((target, 24 * 43 * 35),)
    frame = pd.read_parquet(target)
    assert list(frame.columns) == ["valid_time", "latitude", "longitude", "t2m", "tp"]
    assert frame.valid_time.min() == pd.Timestamp("2024-05-02T00:00:00")
    assert frame.valid_time.max() == pd.Timestamp("2024-05-02T23:00:00")
    assert frame.tp.eq(1).all()
    assert frame.t2m.eq(280).all()
    assert pq.read_metadata(target).metadata[b"analytics_coverage"] == b"partial_year"


def test_daily_export_retains_utc_and_local_and_existing_values(tmp_path):
    _daily_month(tmp_path, 5)
    selection = _plan(tmp_path, "daily", date(2024, 5, 15), date(2024, 5, 16))
    target, rows = export_selection(selection, data_directory=tmp_path)[0]
    assert rows == 4
    frame = pd.read_parquet(target)
    assert list(frame.columns) == [
        "valid_time", "valid_time_local", "latitude", "longitude", "t2m", "tp"
    ]
    assert set(frame.valid_time_local) == {
        pd.Timestamp("2024-05-15"), pd.Timestamp("2024-05-16")
    }
    assert (frame.valid_time_local - frame.valid_time == pd.Timedelta(hours=7)).all()
    assert frame.t2m.eq(25).all()
    assert frame.tp.eq(3).all()


def test_partial_and_full_year_share_destination_but_conflict_safely(tmp_path):
    _daily_month(tmp_path, 5)
    partial = _plan(tmp_path, "daily")
    target = output_paths(partial, data_directory=tmp_path)[0]
    assert target.name == "2024_daily.parquet"
    assert target.parent.name == "daily"
    complete = type(partial)(
        partial.resolution,
        partial.variables,
        date(2024, 1, 1),
        date(2024, 12, 31),
        partial.months,
    )
    assert output_paths(complete, data_directory=tmp_path) == (target,)
    export_selection(partial, data_directory=tmp_path)
    with pytest.raises(ValueError, match="incompatible analytics metadata"):
        from pipeline.analytics.verification import verify_output
        verify_output(complete, 2024, target)


def test_existing_output_is_not_overwritten(tmp_path):
    _daily_month(tmp_path, 5)
    selection = _plan(tmp_path, "daily")
    target = output_paths(selection, data_directory=tmp_path)[0]
    target.parent.mkdir(parents=True)
    target.write_bytes(b"existing")
    with pytest.raises((ValueError, OSError)):
        export_selection(selection, data_directory=tmp_path)
    assert target.read_bytes() == b"existing"
    assert not (target.parent / "manifest.json").exists()


def test_failed_write_cleans_temp_and_preserves_missing_final(tmp_path, monkeypatch):
    _daily_month(tmp_path, 5)
    selection = _plan(tmp_path, "daily")
    target = output_paths(selection, data_directory=tmp_path)[0]

    def interrupted(*args, **kwargs):
        raise OSError("simulated failed input read")
        yield  # Keep the iterator protocol; failure occurs during the write stage.

    monkeypatch.setattr(analytics_export, "_daily_tables", interrupted)
    with pytest.raises(OSError, match="simulated failed input read"):
        export_selection(selection, data_directory=tmp_path)
    assert not target.exists()
    assert not list(target.parent.glob(".*.tmp"))
    assert not (target.parent / "manifest.json").exists()


def test_two_month_daily_export_has_no_duplicate_days(tmp_path):
    _daily_month(tmp_path, 5)
    _daily_month(tmp_path, 6)
    selection = _plan(
        tmp_path, "daily", date(2024, 5, 1), date(2024, 6, 30)
    )
    target, rows = export_selection(selection, data_directory=tmp_path)[0]
    frame = pd.read_parquet(target)
    assert rows == (31 + 30) * 2
    assert frame.valid_time_local.nunique() == 61
    assert not frame.duplicated(
        ["valid_time_local", "latitude", "longitude"]
    ).any()
