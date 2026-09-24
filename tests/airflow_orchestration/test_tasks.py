from datetime import UTC, date, datetime, timedelta

import pytest

from orchestration.tasks.era5_land import (
    build_historical_request_specs,
    inclusive_local_dates,
    load_partition,
    transform_partition,
)


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


def test_transform_partition_rejects_wrong_upstream_boundary() -> None:
    with pytest.raises(ValueError, match="Expected extract_or_resume"):
        transform_partition({"boundary": "load"})


def test_load_partition_rejects_missing_request_metadata() -> None:
    with pytest.raises(ValueError, match="missing a request"):
        load_partition({"boundary": "transform_or_resume"})

