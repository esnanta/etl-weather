"""Persist one global provider-submission clock for orchestration workers.

The Airflow pool serializes extraction tasks but cannot enforce elapsed time
between provider calls, so this repository uses one durable singleton row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import SUBMISSION_INTERVAL_MINUTES
from pipeline.database.config import PACING_SCHEMA, PACING_TABLE


@dataclass(frozen=True)
class SubmissionDecision:
    """Describe whether one provider submission slot was reserved."""

    granted: bool
    checked_at: datetime
    next_allowed_at: datetime


def submission_interval_seconds() -> int:
    """Return the fixed provider pacing interval converted to seconds."""
    if (
        isinstance(SUBMISSION_INTERVAL_MINUTES, bool)
        or not isinstance(SUBMISSION_INTERVAL_MINUTES, int)
        or SUBMISSION_INTERVAL_MINUTES < 0
    ):
        raise ValueError("SUBMISSION_INTERVAL_MINUTES must be a nonnegative integer.")
    return SUBMISSION_INTERVAL_MINUTES * 60


def create_submission_pacing_table(engine: Engine) -> None:
    """Create the durable global submission clock without provider identity."""
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {PACING_SCHEMA}"))
        connection.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {PACING_SCHEMA}.{PACING_TABLE} (
                    singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
                    last_submission_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""
            )
        )


def reserve_submission(
    engine: Engine,
    *,
    interval_seconds: int | None = None,
) -> SubmissionDecision:
    """Atomically reserve the global submission clock or return its next slot."""
    seconds = submission_interval_seconds() if interval_seconds is None else interval_seconds
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 0:
        raise ValueError("interval_seconds must be a nonnegative integer.")

    table = f"{PACING_SCHEMA}.{PACING_TABLE}"
    with engine.begin() as connection:
        connection.execute(
            text(
                f"INSERT INTO {table} (singleton_id) VALUES (1) "
                "ON CONFLICT (singleton_id) DO NOTHING"
            )
        )
        last_submission = connection.execute(
            text(
                f"SELECT last_submission_at FROM {table} "
                "WHERE singleton_id = 1 FOR UPDATE"
            )
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
                "updated_at = :submitted_at WHERE singleton_id = 1"
            ),
            {"submitted_at": checked_at},
        )
        return SubmissionDecision(True, checked_at, checked_at + timedelta(seconds=seconds))
