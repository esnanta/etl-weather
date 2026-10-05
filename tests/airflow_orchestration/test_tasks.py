import logging
from types import SimpleNamespace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from orchestration.monitoring import resources as monitoring
from orchestration.scheduling.intervals import inclusive_local_dates
from orchestration.scheduling.requests import build_historical_request_specs
from orchestration.tasks.extract import ensure_raw_checkpoint_partition
from orchestration.tasks.load import load_partition
from orchestration.tasks.transform import ensure_transformed_checkpoint_partition


def test_build_historical_request_specs_requires_explicit_dates() -> None:
    with pytest.raises(ValueError, match="must be supplied explicitly"):
        build_historical_request_specs(None, None, current_local_date=date(2026, 9, 21))


def test_build_historical_request_specs_splits_ten_calendar_years() -> None:
    specs = build_historical_request_specs(
        "2016-01-01",
        "2025-12-31",
        current_local_date=date(2026, 9, 21),
    )

    assert len(specs) == 120
    assert specs[0]["start_date"] == "2016-01-01"
    assert specs[0]["end_date"] == "2016-01-31"
    assert specs[-1]["start_date"] == "2025-12-01"
    assert specs[-1]["end_date"] == "2025-12-31"


def test_build_historical_request_specs_enforces_availability_lag() -> None:
    with pytest.raises(ValueError, match="not eligible"):
        build_historical_request_specs(
            "2026-09-01",
            "2026-09-20",
            current_local_date=date(2026, 9, 21),
        )


def test_inclusive_local_dates_converts_utc_to_jakarta_midnight() -> None:
    start = datetime(2026, 8, 31, 17, tzinfo=UTC)
    end = datetime(2026, 9, 2, 17, tzinfo=UTC)

    assert inclusive_local_dates(start, end) == (
        date(2026, 9, 1),
        date(2026, 9, 2),
    )


def test_inclusive_local_dates_rejects_non_midnight_boundary() -> None:
    start = datetime(2026, 8, 31, 18, tzinfo=UTC)
    end = start + timedelta(days=1)

    with pytest.raises(ValueError, match="local-midnight"):
        inclusive_local_dates(start, end)


def test_inclusive_local_dates_rejects_empty_interval() -> None:
    boundary = datetime(2026, 8, 31, 17, tzinfo=UTC)

    with pytest.raises(ValueError, match="non-empty"):
        inclusive_local_dates(boundary, boundary)


def test_inclusive_local_dates_rejects_naive_boundaries() -> None:
    start = datetime(2026, 9, 1)
    end = datetime(2026, 9, 2)

    with pytest.raises(ValueError, match="timezone-aware"):
        inclusive_local_dates(start, end)


def test_ensure_transformed_checkpoint_partition_rejects_wrong_upstream_boundary() -> None:
    with pytest.raises(ValueError, match="Expected ensure_raw_checkpoint"):
        ensure_transformed_checkpoint_partition({"boundary": "load"})


def test_load_partition_rejects_missing_request_metadata() -> None:
    with pytest.raises(ValueError, match="missing a request"):
        load_partition({"boundary": "ensure_transformed_checkpoint"})


def test_partition_boundaries_log_peak_memory_without_changing_xcom(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from pipeline import runtime

    spec = build_historical_request_specs(
        "2024-01-01", "2024-01-31", current_local_date=date(2026, 9, 21)
    )[0]
    extract_result = {"boundary": "ensure_raw_checkpoint", "request": spec, "request_key": "key"}
    transform_result = {
        "boundary": "ensure_transformed_checkpoint", "request": spec, "request_key": "key"
    }
    load_result = {"boundary": "load", "request": spec, "request_key": "key"}
    monkeypatch.setattr(runtime, "ensure_raw_checkpoint_partition", lambda request: extract_result)
    monkeypatch.setattr(runtime, "ensure_transformed_checkpoint_partition", lambda request: transform_result)
    monkeypatch.setattr(runtime, "load_request_partition", lambda request: load_result)

    with caplog.at_level(logging.INFO, logger="orchestration.tasks.era5_land"):
        assert ensure_raw_checkpoint_partition(spec) is extract_result
        assert ensure_transformed_checkpoint_partition(extract_result) is transform_result
        assert load_partition(transform_result) is load_result

    memory_logs = [record.message for record in caplog.records if "[METRIC] task memory" in record.message]
    assert len(memory_logs) == 3
    for boundary, message in zip(("ensure_raw_checkpoint", "ensure_transformed_checkpoint", "load"), memory_logs):
        assert f"boundary={boundary}" in message
        assert "process_peak_rss_mib=" in message
        assert "peak_before_mib=" in message


def test_peak_memory_is_logged_when_partition_fails(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from pipeline import runtime

    spec = build_historical_request_specs(
        "2024-01-01", "2024-01-31", current_local_date=date(2026, 9, 21)
    )[0]

    def fail_extraction(request: object) -> None:
        raise RuntimeError("extract failed")

    monkeypatch.setattr(runtime, "ensure_raw_checkpoint_partition", fail_extraction)
    with caplog.at_level(logging.INFO, logger="orchestration.tasks.era5_land"):
        with pytest.raises(RuntimeError, match="extract failed"):
            ensure_raw_checkpoint_partition(spec)
    assert any(
        "[METRIC] task memory | boundary=ensure_raw_checkpoint" in record.message
        for record in caplog.records
    )


def test_monitoring_data_path_still_targets_repository_data() -> None:
    expected = Path(__file__).resolve().parents[2] / "data"
    resolved = Path(monitoring.__file__).resolve().parents[2] / "data"
    assert resolved == expected


def test_disk_warning_reports_low_space_without_failing_task(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        monitoring.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=4 * monitoring.GIB),
    )
    with caplog.at_level(logging.WARNING, logger="orchestration.tasks.era5_land"):
        monitoring.warn_low_disk_space("ensure_transformed_checkpoint")
    warnings = [record.message for record in caplog.records if "[DISK]" in record.message]
    assert len(warnings) >= 1
    assert all("boundary=ensure_transformed_checkpoint" in message for message in warnings)
    assert any("available_gib=4.00" in message for message in warnings)

    caplog.clear()
    monkeypatch.setattr(
        monitoring.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=6 * monitoring.GIB),
    )
    monitoring.warn_low_disk_space("ensure_transformed_checkpoint")
    assert not any("[DISK]" in record.message for record in caplog.records)


def test_disk_warning_reports_unavailable_measurement(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def unavailable(path: object) -> None:
        raise OSError("measurement failed")

    monkeypatch.setattr(monitoring.shutil, "disk_usage", unavailable)
    with caplog.at_level(logging.WARNING, logger="orchestration.tasks.era5_land"):
        monitoring.warn_low_disk_space("ensure_raw_checkpoint")
    assert any(
        "[DISK] free space check unavailable" in record.message
        and "boundary=ensure_raw_checkpoint" in record.message
        for record in caplog.records
    )
