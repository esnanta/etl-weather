from __future__ import annotations

import logging
import os
import resource
import shutil
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

PIPELINE_TIMEZONE = ZoneInfo("Asia/Jakarta")
AVAILABILITY_LAG_DAYS = 6
DISK_WARNING_GIB = 5
GIB = 1024**3


def warn_low_disk_space(boundary: str) -> None:
    """Warn about low space on artifact and temporary-data filesystems."""
    data_directory = Path(__file__).resolve().parents[2] / "data"
    artifact_path = data_directory if data_directory.is_dir() else data_directory.parent
    locations = [("artifacts", artifact_path)]
    try:
        locations.append(("temporary", Path(tempfile.gettempdir())))
    except OSError as exc:
        logger.warning(
            "[DISK] temporary directory unavailable | boundary=%s | error=%s",
            boundary,
            exc,
        )
    checked_devices: set[int] = set()
    for location, path in locations:
        try:
            device = path.stat().st_dev
            if device in checked_devices:
                continue
            available_bytes = shutil.disk_usage(path).free
            checked_devices.add(device)
        except OSError as exc:
            logger.warning(
                "[DISK] free space check unavailable | boundary=%s | "
                "location=%s | path=%s | error=%s",
                boundary,
                location,
                path,
                exc,
            )
            continue

        if available_bytes < DISK_WARNING_GIB * GIB:
            logger.warning(
                "[DISK] low free space | boundary=%s | location=%s | "
                "path=%s | available_gib=%.2f | threshold_gib=%d",
                boundary,
                location,
                path,
                available_bytes / GIB,
                DISK_WARNING_GIB,
            )


@contextmanager
def measure_peak_memory(boundary: str) -> Iterator[None]:
    """Log the process RSS high-water mark even when a boundary fails."""
    before_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    try:
        yield
    finally:
        after_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info(
            "[METRIC] task memory | boundary=%s | pid=%d | "
            "process_peak_rss_mib=%.2f | peak_before_mib=%.2f",
            boundary,
            os.getpid(),
            after_kib / 1024,
            before_kib / 1024,
        )


def inclusive_local_dates(
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> tuple[date, date]:
    """Convert a half-open Airflow interval to inclusive Jakarta dates."""
    if data_interval_start.tzinfo is None or data_interval_end.tzinfo is None:
        raise ValueError("Airflow data interval boundaries must be timezone-aware.")

    start = data_interval_start.astimezone(PIPELINE_TIMEZONE)
    end = data_interval_end.astimezone(PIPELINE_TIMEZONE)
    if any((start.hour, start.minute, start.second, start.microsecond)) or any(
        (end.hour, end.minute, end.second, end.microsecond)
    ):
        raise ValueError("Airflow data interval must use local-midnight boundaries.")
    if end <= start:
        raise ValueError("Airflow data interval must be non-empty.")
    return start.date(), end.date() - timedelta(days=1)


def build_historical_request_specs(
    start_date: str | None,
    end_date: str | None,
    *,
    current_local_date: date | None = None,
) -> list[dict[str, object]]:
    """Validate an explicit historical range and return monthly request specs."""
    from pipeline.request import (
        split_request_by_month,
        validate_request,
        weather_request_to_spec,
    )
    from scripts.config import build_cds_request

    if not start_date or not end_date:
        raise ValueError(
            "Historical start_date and end_date must be supplied explicitly."
        )

    request = build_cds_request(
        start_date=start_date,
        end_date=end_date,
        require_api_key=False,
    )
    validate_request(request)
    today = current_local_date or datetime.now(PIPELINE_TIMEZONE).date()
    eligibility_boundary = today - timedelta(days=AVAILABILITY_LAG_DAYS)
    requested_end = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    if requested_end > eligibility_boundary:
        raise ValueError(
            f"Historical end_date {requested_end.isoformat()} is not eligible; "
            f"the current boundary is {eligibility_boundary.isoformat()} "
            f"({AVAILABILITY_LAG_DAYS} completed local days)."
        )

    monthly_requests = split_request_by_month(request)
    logger.info(
        "Prepared %d monthly ERA5-Land partitions for %s through %s; "
        "eligibility_boundary=%s",
        len(monthly_requests),
        request.start_date,
        request.end_date,
        eligibility_boundary.isoformat(),
    )
    return [weather_request_to_spec(monthly) for monthly in monthly_requests]


def extract_partition(request_spec: Mapping[str, Any]) -> dict[str, object]:
    """Delegate one mapped extraction task to the pipeline runtime."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import extract_request_partition

    request = weather_request_from_spec(request_spec)
    logger.info("Starting extraction for %s through %s", request.start_date, request.end_date)
    with measure_peak_memory("extract_or_resume"):
        warn_low_disk_space("extract_or_resume")
        return extract_request_partition(request)


def transform_partition(
    extract_result: Mapping[str, Any],
) -> dict[str, object]:
    """Delegate one mapped transform task using extraction control metadata."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import transform_request_partition

    request_spec = _request_spec_from_result(extract_result, "extract_or_resume")
    request = weather_request_from_spec(request_spec)
    logger.info("Starting transformation for request_key=%s", extract_result["request_key"])
    with measure_peak_memory("transform_or_resume"):
        warn_low_disk_space("transform_or_resume")
        return transform_request_partition(request)


def load_partition(transform_result: Mapping[str, Any]) -> dict[str, object]:
    """Delegate one mapped load task using transform control metadata."""
    from pipeline.request import weather_request_from_spec
    from pipeline.runtime import load_request_partition

    request_spec = _request_spec_from_result(transform_result, "transform_or_resume")
    request = weather_request_from_spec(request_spec)
    logger.info("Starting database load for request_key=%s", transform_result["request_key"])
    with measure_peak_memory("load"):
        warn_low_disk_space("load")
        return load_request_partition(request)


def _request_spec_from_result(
    result: Mapping[str, Any],
    expected_boundary: str,
) -> Mapping[str, Any]:
    if result.get("boundary") != expected_boundary:
        raise ValueError(
            f"Expected {expected_boundary} control metadata before downstream task."
        )
    request_spec = result.get("request")
    if not isinstance(request_spec, Mapping):
        raise ValueError("Upstream control metadata is missing a request specification.")
    return request_spec
