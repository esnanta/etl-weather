"""Test analytics source discovery and validation with offline monthly checkpoints.

The production selection module is under test. Synthetic NetCDF/Parquet files
replace live CDS and PostgreSQL, which are not relevant to read-only selection.
End-to-end export fidelity and atomic writes belong to the later export tests.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pipeline.analytics.selection import (
    discover_available_sources,
    discover_sources,
    normalize_variables,
    validate_sources,
)

BBOX = "bbox_n6p1_w94p9_s1p9_e98p3"
SIGNATURE = "t2m_tp"


def _folder(tmp_path, layer):
    return tmp_path / "cds_reanalysis-era5-land" / BBOX / layer / "2024"


def _raw_month(tmp_path, month, *, value=1.0, context_override=None):
    folder = _folder(tmp_path, "raw")
    folder.mkdir(parents=True, exist_ok=True)
    first = pd.Timestamp(f"2024-{month:02d}-01")
    last = first + pd.offsets.MonthEnd(0)
    times = pd.date_range(first - pd.Timedelta(days=1), last + pd.Timedelta(hours=23), freq="h")
    latitude = np.linspace(6.1, 1.9, 43)
    longitude = np.linspace(94.9, 98.3, 35)
    shape = (len(times), len(latitude), len(longitude))
    tp = np.full(shape, value, dtype=np.float32)
    if context_override is not None:
        tp[:24] = context_override
    ds = xr.Dataset(
        {
            "tp": (("valid_time", "latitude", "longitude"), tp),
            "t2m": (("valid_time", "latitude", "longitude"), np.full(shape, 280, dtype=np.float32)),
        },
        coords={"valid_time": times, "latitude": latitude, "longitude": longitude},
    )
    path = folder / f"2024_{first:%m%d}_{last:%m%d}_{SIGNATURE}.nc"
    ds.to_netcdf(path)
    return path


def _daily_month(tmp_path, month, *, missing_day=False):
    folder = _folder(tmp_path, "transformed")
    folder.mkdir(parents=True, exist_ok=True)
    first = pd.Timestamp(f"2024-{month:02d}-01")
    last = first + pd.offsets.MonthEnd(0)
    days = pd.date_range(first, last, freq="D")
    if missing_day:
        days = days[:-1]
    df = pd.DataFrame(
        [
            {
                "valid_time": local - pd.Timedelta(hours=7),
                "valid_time_local": local,
                "latitude": latitude,
                "longitude": 95.2,
                "tp": 3.0,
                "t2m": 25.0,
            }
            for local in days
            for latitude in (5.0, 5.1)
        ]
    )
    path = folder / f"2024_{first:%m%d}_{last:%m%d}_{SIGNATURE}.parquet"
    df.to_parquet(path, index=False)
    return path


def test_variable_normalization():
    assert normalize_variables(" TP, t2m ") == ("t2m", "tp")
    assert len(normalize_variables("all")) == 9
    with pytest.raises(ValueError, match="Duplicate"):
        normalize_variables("tp,tp")
    with pytest.raises(ValueError, match="Unsupported"):
        normalize_variables("swvl4")
    with pytest.raises(ValueError, match="comma-separated"):
        normalize_variables("tp,")


def test_two_month_raw_selection_validates_identical_context(tmp_path):
    _raw_month(tmp_path, 5)
    _raw_month(tmp_path, 6)
    selected = discover_sources(
        "hourly", "tp,t2m", date(2024, 5, 1), date(2024, 6, 30),
        data_directory=tmp_path,
    )
    assert len(selected.months) == 2
    assert selected.months[0].start == date(2024, 5, 1)
    validate_sources(selected)


def test_conflicting_raw_context_fails(tmp_path):
    _raw_month(tmp_path, 5)
    _raw_month(tmp_path, 6, context_override=8)
    selected = discover_sources(
        "hourly", "tp,t2m", date(2024, 5, 1), date(2024, 6, 30),
        data_directory=tmp_path,
    )
    with pytest.raises(ValueError, match="Conflicting RAW overlap tp"):
        validate_sources(selected)


def test_missing_month_or_requested_variable_fails(tmp_path):
    _raw_month(tmp_path, 5)
    with pytest.raises(FileNotFoundError, match="2024-06"):
        discover_sources(
            "hourly", "tp", date(2024, 5, 1), date(2024, 6, 30),
            data_directory=tmp_path,
        )
    with pytest.raises(FileNotFoundError, match="swvl1"):
        discover_sources(
            "hourly", "swvl1", date(2024, 5, 1), date(2024, 5, 31),
            data_directory=tmp_path,
        )


def test_ambiguous_month_is_rejected(tmp_path):
    _raw_month(tmp_path, 5)
    folder = _folder(tmp_path, "raw")
    original = next(folder.glob("*.nc"))
    duplicated = folder / "2024_0501_0531_d2m_t2m_tp.nc"
    duplicated.write_bytes(original.read_bytes())
    with pytest.raises(ValueError, match="Ambiguous"):
        discover_sources(
            "hourly", "tp", date(2024, 5, 1), date(2024, 5, 31),
            data_directory=tmp_path,
        )


def test_daily_sources_preserve_local_day_identity(tmp_path):
    _daily_month(tmp_path, 5)
    _daily_month(tmp_path, 6)
    selected = discover_sources(
        "daily", "t2m,tp", date(2024, 5, 1), date(2024, 6, 30),
        data_directory=tmp_path,
    )
    assert len(selected.months) == 2
    validate_sources(selected)


def test_daily_missing_local_day_fails(tmp_path):
    _daily_month(tmp_path, 6, missing_day=True)
    selected = discover_sources(
        "daily", "tp", date(2024, 6, 1), date(2024, 6, 30),
        data_directory=tmp_path,
    )
    with pytest.raises(ValueError, match="incomplete"):
        validate_sources(selected)


def test_partial_month_still_requires_complete_monthly_checkpoint(tmp_path):
    _daily_month(tmp_path, 5)
    selected = discover_sources(
        "daily", "tp", date(2024, 5, 15), date(2024, 5, 20),
        data_directory=tmp_path,
    )
    validate_sources(selected)
    assert selected.start == date(2024, 5, 15)
    assert selected.end == date(2024, 5, 20)


def test_source_driven_discovery_skips_absent_months_and_keeps_actual_coverage(tmp_path):
    _daily_month(tmp_path, 5)
    _daily_month(tmp_path, 7)
    (plan,) = discover_available_sources(
        "daily", "tp,t2m", data_directory=tmp_path
    )
    assert [month.start.month for month in plan.months] == [5, 7]
    assert plan.start == date(2024, 5, 1)
    assert plan.end == date(2024, 7, 31)
    validate_sources(plan)


def test_source_driven_discovery_rejects_incompatible_variable_source(tmp_path):
    _raw_month(tmp_path, 5)
    with pytest.raises(ValueError, match="fewer variables"):
        discover_available_sources("hourly", "all", data_directory=tmp_path)


def test_source_driven_discovery_rejects_ambiguous_month(tmp_path):
    original = _daily_month(tmp_path, 5)
    other = original.with_name("2024_0501_0531_d2m_t2m_tp.parquet")
    other.write_bytes(original.read_bytes())
    with pytest.raises(ValueError, match="Ambiguous"):
        discover_available_sources("daily", "tp", data_directory=tmp_path)


def test_source_driven_discovery_requires_at_least_one_checkpoint(tmp_path):
    with pytest.raises(FileNotFoundError, match="No hourly checkpoint"):
        discover_available_sources("hourly", "tp", data_directory=tmp_path)
