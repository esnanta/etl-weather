"""Coordinate CDS submission timestamps so separate workers obey one pacing clock.

The one-slot Airflow pool serializes extraction tasks but cannot enforce elapsed
time between provider calls, so a durable database row records each reservation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from orchestration.config import (
    DEFAULT_INTERVAL_SECONDS,
    INTERVAL_ENV,
    PACING_KEY,
    PACING_SCHEMA,
    PACING_TABLE,
)


@dataclass(frozen=True)
class SubmissionDecision:
    granted: bool
    checked_at: datetime
    next_allowed_at: datetime


def submission_interval_seconds() -> int:
    """Read the independent operational interval; reject malformed settings."""
    raw = os.getenv(INTERVAL_ENV, str(DEFAULT_INTERVAL_SECONDS))
    try:
        seconds = int(raw)
    except ValueError as exc:
        raise ValueError(f"{INTERVAL_ENV} must be a nonnegative integer.") from exc
    if seconds < 0:
        raise ValueError(f"{INTERVAL_ENV} must be a nonnegative integer.")
    return seconds


def create_cds_pacing_table(engine: Engine) -> None:
    """Create a separate operational clock without changing progress identity."""
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {PACING_SCHEMA}"))
        connection.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {PACING_SCHEMA}.{PACING_TABLE} (
                    pacing_key TEXT PRIMARY KEY,
                    last_submission_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""
            )
        )


def reserve_cds_submission(
    engine: Engine,
    *,
    interval_seconds: int | None = None,
) -> SubmissionDecision:
    """Atomically reserve a submission or return when the clock permits one.

    The row lock spans the read and update, and the database clock is read only
    after acquiring it. A crash after commit conservatively delays the next
    attempt even if the provider call did not start. Call this immediately before
    each actual retrieve, including context chunks and task retries.
    """
    seconds = submission_interval_seconds() if interval_seconds is None else interval_seconds
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 0:
        raise ValueError("interval_seconds must be a nonnegative integer.")

    table = f"{PACING_SCHEMA}.{PACING_TABLE}"
    with engine.begin() as connection:
        connection.execute(
            text(
                f"INSERT INTO {table} (pacing_key) VALUES (:key) "
                "ON CONFLICT (pacing_key) DO NOTHING"
            ),
            {"key": PACING_KEY},
        )
        last_submission = connection.execute(
            text(f"SELECT last_submission_at FROM {table} WHERE pacing_key = :key FOR UPDATE"),
            {"key": PACING_KEY},
        ).scalar_one()
        checked_at = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        next_allowed_at = (
            last_submission + timedelta(seconds=seconds)
            if last_submission is not None
            else checked_at
        )
        if checked_at < next_allowed_at:
            return SubmissionDecision(False, checked_at, next_allowed_at)
        connection.execute(
            text(
                f"UPDATE {table} SET last_submission_at = :submitted_at, "
                "updated_at = :submitted_at WHERE pacing_key = :key"
            ),
            {"key": PACING_KEY, "submitted_at": checked_at},
        )
        return SubmissionDecision(
            True, checked_at, checked_at + timedelta(seconds=seconds)
        )
