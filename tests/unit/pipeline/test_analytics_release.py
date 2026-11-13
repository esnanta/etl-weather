"""Exercise analytics export contracts not covered by the May–June pilot.

Real discovery, validation, streaming writer and manifest publishing operate on
offline NetCDF/Parquet fixtures under tmp_path. No CDS, PostgreSQL or source
checkpoint mutation is performed outside those temporary fixtures.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from pipeline.analytics.export import export_selection, output_paths
from pipeline.analytics.selection import discover_available_sources, discover_sources, validate_sources
from tests.unit.pipeline.test_analytics_selection import _daily_month, _raw_month


def _selection(tmp_path, resolution, variables, first, last):
    plan = discover_sources(
        resolution, variables, first, last, data_directory=tmp_path
    )
    validate_sources(plan)
    return plan


def test_reordered_subset_is_the_same_dataset_and_reuses_manifest(tmp_path):
    _daily_month(tmp_path, 5)
    first = date(2024, 5, 1)
    last = date(2024, 5, 31)
    a = _selection(tmp_path, "daily", "tp,t2m", first, last)
    b = _selection(tmp_path, "daily", "T2M,TP", first, last)
    assert output_paths(a, data_directory=tmp_path) == output_paths(
        b, data_directory=tmp_path
    )
    (path, count), = export_selection(a, data_directory=tmp_path)
    manifest = path.parent / "manifest.json"
    before = manifest.read_bytes()
    assert export_selection(b, data_directory=tmp_path) == ((path, count),)
    assert manifest.read_bytes() == before


def test_manifest_rejects_modified_output_checksum_before_new_publish(tmp_path):
    _daily_month(tmp_path, 5)
    plan = _selection(
        tmp_path, "daily", "tp,t2m", date(2024, 5, 1), date(2024, 5, 31)
    )
    (path, _), = export_selection(plan, data_directory=tmp_path)
    manifest_path = path.parent / "manifest.json"
    document = json.loads(manifest_path.read_text())
    document["partitions"][path.name]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(document))
    before = manifest_path.read_bytes()
    with pytest.raises(ValueError, match="missing/changed file"):
        export_selection(plan, data_directory=tmp_path)
    assert manifest_path.read_bytes() == before
    assert path.exists()


def test_hourly_subset_keeps_context_outside_target_period(tmp_path):
    _raw_month(tmp_path, 5)
    plan = _selection(
        tmp_path, "hourly", "tp", date(2024, 5, 1), date(2024, 5, 1)
    )
    (path, rows), = export_selection(plan, data_directory=tmp_path)
    data = pd.read_parquet(path)
    assert rows == 24 * 43 * 35
    assert list(data.columns) == ["valid_time", "latitude", "longitude", "tp"]
    assert data.valid_time.min() == pd.Timestamp("2024-05-01")
    assert data.valid_time.max() == pd.Timestamp("2024-05-01 23:00:00")


def test_two_year_daily_export_has_independent_files_and_manifest_inventory(tmp_path):
    folder = (
        tmp_path / "cds_reanalysis-era5-land"
        / "bbox_n6p1_w94p9_s1p9_e98p3" / "transformed"
    )
    for year, month in ((2023, 12), (2024, 1)):
        start = pd.Timestamp(year, month, 1)
        days = pd.date_range(start, start + pd.offsets.MonthEnd(0))
        frame = pd.DataFrame(
            [
                {
                    "valid_time": day - pd.Timedelta(hours=7),
                    "valid_time_local": day,
                    "latitude": 5.0,
                    "longitude": 95.2,
                    "tp": float(year),
                    "t2m": 25.0,
                }
                for day in days
            ]
        )
        path = folder / str(year) / f"{year}_{month:02d}01_{month:02d}{days[-1].day:02d}_t2m_tp.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)

    plan = _selection(
        tmp_path, "daily", "tp,t2m",
        date(2023, 12, 1), date(2024, 1, 31),
    )
    targets = output_paths(plan, data_directory=tmp_path)
    assert [target.name for target in targets] == [
        "2023_daily.parquet", "2024_daily.parquet"
    ]
    result = export_selection(plan, data_directory=tmp_path)
    assert [rows for _, rows in result] == [31, 31]
    assert pd.read_parquet(targets[0]).tp.eq(2023).all()
    assert pd.read_parquet(targets[1]).tp.eq(2024).all()
    document = json.loads((targets[0].parent / "manifest.json").read_text())
    assert set(document["partitions"]) == {
        "2023_daily.parquet", "2024_daily.parquet"
    }
    assert all(part["coverage"] == "partial_year" for part in document["partitions"].values())
    assert export_selection(plan, data_directory=tmp_path) == result


def test_source_driven_nonconsecutive_months_are_not_marked_complete(tmp_path):
    _daily_month(tmp_path, 5)
    _daily_month(tmp_path, 7)
    (plan,) = discover_available_sources(
        "daily", "tp,t2m", data_directory=tmp_path
    )
    (path, rows), = export_selection(plan, data_directory=tmp_path)
    frame = pd.read_parquet(path)
    assert rows == (31 + 31) * 2
    assert set(frame.valid_time_local.dt.month.unique()) == {5, 7}
    manifest = json.loads((path.parent / "manifest.json").read_text())
    partition = manifest["partitions"][path.name]
    assert partition["coverage"] == "partial_year"
    assert partition["target_start"] == "2024-05-01"
    assert partition["target_end"] == "2024-07-31"
    assert partition["source_months"] == ["2024-05", "2024-07"]
    assert export_selection(plan, data_directory=tmp_path) == ((path, rows),)
