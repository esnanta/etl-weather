"""Convert Airflow data intervals to inclusive local calendar dates."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from orchestration.config import PIPELINE_TIMEZONE


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
