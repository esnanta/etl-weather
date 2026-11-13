"""Verify P4 output fidelity, provenance, and non-destructive reruns.

Real production selection, streaming writer, verifier and manifest publisher
are exercised against temporary synthetic checkpoints. Only failure injection
uses monkeypatch; live CDS, PostgreSQL and production files are not accessed.
"""

from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import xarray as xr

from pipeline.analytics.export import export_selection, output_paths
from pipeline.analytics.selection import discover_sources, validate_sources
from pipeline.analytics.verification import verify_output
from tests.unit.pipeline.test_analytics_selection import _daily_month, _raw_month


def _selection(tmp_path, resolution, start=date(2024, 5, 1), end=date(2024, 5, 31)):
    result = discover_sources(
        resolution, "tp,t2m", start, end, data_directory=tmp_path
    )
    validate_sources(result)
    return result


@pytest.mark.parametrize("resolution", ["hourly", "daily"])
def test_manifest_tracks_source_and_output_sha256(tmp_path, resolution):
    (_raw_month if resolution == "hourly" else _daily_month)(tmp_path, 5)
    selected = _selection(tmp_path, resolution)
    (output, count), = export_selection(selected, data_directory=tmp_path)
    record = json.loads((output.parent / "manifest.json").read_text())
    assert record["schema_version"] == 1
    assert record["dataset"] == "reanalysis-era5-land"
    assert record["resolution"] == resolution
    assert record["variables"] == ["t2m", "tp"]
    assert list(record["partitions"]) == [f"2024_{resolution}.parquet"]
    partition = record["partitions"][f"2024_{resolution}.parquet"]
    assert partition["coverage"] == "partial_year"
    assert partition["target_start"] == "2024-05-01"
    assert partition["target_end"] == "2024-05-31"
    assert partition["rows"] == count
    assert len(partition["sha256"]) == 64
    assert len(partition["sources"]) == 1
    assert len(partition["sources"][0]["sha256"]) == 64
    assert record["treatment"] == (
        "raw_preserving" if resolution == "hourly"
        else "upstream_daily_values_unchanged"
    )


def test_adopt_existing_p3_without_replacing_parquet(tmp_path):
    _daily_month(tmp_path, 5)
    selected = _selection(tmp_path, "daily")
    output = output_paths(selected, data_directory=tmp_path)[0]
    output.parent.mkdir(parents=True, exist_ok=True)

    from pipeline.analytics.export import _write_year
    _write_year(selected, 2024, output)
    before = output.stat().st_ino
    assert not (output.parent / "manifest.json").exists()

    results = export_selection(selected, data_directory=tmp_path)
    assert results[0][0] == output
    assert output.stat().st_ino == before
    assert (output.parent / "manifest.json").exists()

    manifest_before = (output.parent / "manifest.json").read_bytes()
    assert export_selection(selected, data_directory=tmp_path) == results
    assert output.stat().st_ino == before
    assert (output.parent / "manifest.json").read_bytes() == manifest_before


@pytest.mark.parametrize("resolution", ["hourly", "daily"])
def test_existing_legacy_pilot_is_linked_without_rewriting_bytes(tmp_path, resolution):
    (_raw_month if resolution == "hourly" else _daily_month)(tmp_path, 5)
    selected = _selection(tmp_path, resolution)
    new_path = output_paths(selected, data_directory=tmp_path)[0]
    legacy = new_path.parent / "pilot_20240501_20240531" / "2024.parquet"
    legacy.parent.mkdir(parents=True, exist_ok=True)

    from pipeline.analytics.export import _write_year
    _write_year(selected, 2024, legacy)
    original_bytes = legacy.read_bytes()
    original_inode = legacy.stat().st_ino

    result = export_selection(selected, data_directory=tmp_path)
    assert result[0][0] == new_path
    assert new_path.name == f"2024_{resolution}.parquet"
    assert new_path.parent.name == resolution
    assert new_path.read_bytes() == original_bytes
    assert legacy.read_bytes() == original_bytes
    assert new_path.stat().st_ino == original_inode
    assert (new_path.parent / "manifest.json").exists()

    assert export_selection(selected, data_directory=tmp_path) == result


def test_legacy_pilot_with_changed_values_is_not_linked(tmp_path):
    _daily_month(tmp_path, 5)
    selected = _selection(tmp_path, "daily")
    new_path = output_paths(selected, data_directory=tmp_path)[0]
    legacy = new_path.parent / "pilot_20240501_20240531" / "2024.parquet"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    from pipeline.analytics.export import _write_year
    _write_year(selected, 2024, legacy)

    frame = pd.read_parquet(legacy)
    frame.loc[0, "tp"] = 888.0
    frame.to_parquet(legacy, index=False)

    with pytest.raises(ValueError, match="mismatch"):
        export_selection(selected, data_directory=tmp_path)
    assert not new_path.exists()
    assert not (new_path.parent / "manifest.json").exists()


def test_changed_output_is_rejected_without_overwrite(tmp_path):
    _daily_month(tmp_path, 5)
    selected = _selection(tmp_path, "daily")
    (output, _), = export_selection(selected, data_directory=tmp_path)
    manifest = output.parent / "manifest.json"
    previous = manifest.read_bytes()

    frame = pd.read_parquet(output)
    frame.loc[0, "tp"] = 999
    frame.to_parquet(output, index=False)
    corrupt = output.read_bytes()
    with pytest.raises(ValueError, match="mismatch|metadata"):
        export_selection(selected, data_directory=tmp_path)
    assert output.read_bytes() == corrupt
    assert manifest.read_bytes() == previous


def test_changed_source_is_rejected_without_manifest_update(tmp_path):
    source = _daily_month(tmp_path, 5)
    selected = _selection(tmp_path, "daily")
    (output, _), = export_selection(selected, data_directory=tmp_path)
    manifest = output.parent / "manifest.json"
    old_manifest = manifest.read_bytes()

    frame = pd.read_parquet(source)
    frame.loc[0, "tp"] = 44.0
    frame.to_parquet(source, index=False)
    with pytest.raises(ValueError, match="mismatch"):
        export_selection(selected, data_directory=tmp_path)
    assert manifest.read_bytes() == old_manifest


def test_fidelity_preserves_missing_positions_in_daily(tmp_path):
    source = _daily_month(tmp_path, 5)
    frame = pd.read_parquet(source)
    frame.loc[0, "tp"] = np.nan
    frame.to_parquet(source, index=False)
    selected = _selection(tmp_path, "daily")
    (output, count), = export_selection(selected, data_directory=tmp_path)
    assert count == len(frame)
    exported = pd.read_parquet(output)
    assert exported.tp.isna().equals(frame.tp.isna())
    assert exported.tp.dropna().equals(frame.tp.dropna())
    assert verify_output(selected, 2024, output) == count


def test_fidelity_preserves_missing_positions_in_raw(tmp_path):
    source = _raw_month(tmp_path, 5)
    with xr.open_dataset(source) as opened:
        ds = opened.load()
    ds["tp"].values[24, 0, 0] = np.nan
    ds.to_netcdf(source)
    selected = _selection(tmp_path, "hourly", date(2024, 5, 1), date(2024, 5, 1))
    (output, count), = export_selection(selected, data_directory=tmp_path)
    assert count == 24 * 43 * 35
    exported = pd.read_parquet(output)
    assert exported.tp.isna().sum() == 1
    assert verify_output(selected, 2024, output) == count


def test_existing_p3_file_with_wrong_coverage_metadata_is_not_adopted(tmp_path):
    _daily_month(tmp_path, 5)
    selected = _selection(tmp_path, "daily")
    output = output_paths(selected, data_directory=tmp_path)[0]
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"tp": [1]}).to_parquet(output)
    with pytest.raises(ValueError, match="incompatible analytics metadata"):
        export_selection(selected, data_directory=tmp_path)
    assert not (output.parent / "manifest.json").exists()


def test_manifest_publishing_failure_keeps_existing_parquet(tmp_path, monkeypatch):
    _daily_month(tmp_path, 5)
    selected = _selection(tmp_path, "daily")
    import pipeline.analytics.verification as verification
    def broken_replace(*args):
        raise OSError("simulated manifest publish interruption")
    monkeypatch.setattr(verification.os, "replace", broken_replace)
    with pytest.raises(OSError, match="simulated manifest"):
        export_selection(selected, data_directory=tmp_path)
    output = output_paths(selected, data_directory=tmp_path)[0]
    assert output.exists()
    assert not (output.parent / "manifest.json").exists()
    assert not list(output.parent.glob(".manifest.*.tmp"))
