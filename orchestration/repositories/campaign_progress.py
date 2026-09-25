from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from orchestration.campaign import CAMPAIGN_KEY, CampaignQuarter

CAMPAIGN_PROGRESS_SCHEMA = "climate"
CAMPAIGN_PROGRESS_TABLE = "quarterly_progress"
_ALLOWED_STATUSES = ("pending", "failed", "complete")


@dataclass(frozen=True)
class CampaignProgress:
    quarter: CampaignQuarter
    configuration_identity: str
    status: str
    failure_count: int
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.status not in _ALLOWED_STATUSES:
            raise ValueError(f"Unsupported campaign progress status: {self.status}")


def _table_name() -> str:
    return f"{CAMPAIGN_PROGRESS_SCHEMA}.{CAMPAIGN_PROGRESS_TABLE}"


def create_campaign_progress_table(engine: Engine) -> None:
    """Create quarterly campaign progress in the existing climate database."""
    table = _table_name()
    with engine.begin() as connection:
        connection.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {CAMPAIGN_PROGRESS_SCHEMA}")
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table}
                (
                    campaign_key TEXT NOT NULL,
                    year SMALLINT NOT NULL,
                    quarter SMALLINT NOT NULL,
                    configuration_identity TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_failure TEXT,
                    completed_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (campaign_key, year, quarter),
                    CONSTRAINT ck_{CAMPAIGN_PROGRESS_TABLE}_quarter
                        CHECK (quarter BETWEEN 1 AND 4),
                    CONSTRAINT ck_{CAMPAIGN_PROGRESS_TABLE}_status
                        CHECK (status IN ('pending', 'failed', 'complete'))
                )
                """
            )
        )


def seed_campaign_progress(
    engine: Engine,
    expected_identities: Mapping[CampaignQuarter, str],
) -> None:
    """Upsert expected quarter identities, invalidating stale completion safely."""
    table = _table_name()
    statement = text(
        f"""
        INSERT INTO {table} AS current_progress
            (campaign_key, year, quarter, configuration_identity)
        VALUES
            (:campaign_key, :year, :quarter, :configuration_identity)
        ON CONFLICT (campaign_key, year, quarter)
        DO UPDATE SET
            configuration_identity = EXCLUDED.configuration_identity,
            status = CASE
                WHEN current_progress.configuration_identity = EXCLUDED.configuration_identity
                    THEN current_progress.status
                ELSE 'pending'
            END,
            failure_count = CASE
                WHEN current_progress.configuration_identity = EXCLUDED.configuration_identity
                    THEN current_progress.failure_count
                ELSE 0
            END,
            last_failure = CASE
                WHEN current_progress.configuration_identity = EXCLUDED.configuration_identity
                    THEN current_progress.last_failure
                ELSE NULL
            END,
            completed_at = CASE
                WHEN current_progress.configuration_identity = EXCLUDED.configuration_identity
                    THEN current_progress.completed_at
                ELSE NULL
            END,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as connection:
        for quarter, identity in sorted(expected_identities.items()):
            connection.execute(
                statement,
                {
                    "campaign_key": CAMPAIGN_KEY,
                    "year": quarter.year,
                    "quarter": quarter.quarter,
                    "configuration_identity": identity,
                },
            )


def get_earliest_incomplete_quarter(engine: Engine) -> CampaignProgress | None:
    table = _table_name()
    statement = text(
        f"""
        SELECT year, quarter, configuration_identity, status,
               failure_count, completed_at
        FROM {table}
        WHERE campaign_key = :campaign_key
          AND status <> 'complete'
        ORDER BY year, quarter
        LIMIT 1
        """
    )
    with engine.connect() as connection:
        row = connection.execute(
            statement, {"campaign_key": CAMPAIGN_KEY}
        ).mappings().first()

    if row is None:
        return None
    return CampaignProgress(
        quarter=CampaignQuarter(int(row["year"]), int(row["quarter"])),
        configuration_identity=str(row["configuration_identity"]),
        status=str(row["status"]),
        failure_count=int(row["failure_count"]),
        completed_at=row["completed_at"],
    )


def mark_quarter_complete(
    engine: Engine,
    quarter: CampaignQuarter,
    configuration_identity: str,
) -> None:
    """Atomically complete only the currently expected quarter identity."""
    table = _table_name()
    statement = text(
        f"""
        UPDATE {table}
        SET status = 'complete',
            last_failure = NULL,
            completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE campaign_key = :campaign_key
          AND year = :year
          AND quarter = :quarter
          AND configuration_identity = :configuration_identity
        """
    )
    with engine.begin() as connection:
        result = connection.execute(
            statement,
            {
                "campaign_key": CAMPAIGN_KEY,
                "year": quarter.year,
                "quarter": quarter.quarter,
                "configuration_identity": configuration_identity,
            },
        )
        if result.rowcount != 1:
            raise ValueError(
                f"Cannot complete {quarter.label}: progress row or configuration "
                "identity does not match the current campaign."
            )


def record_quarter_failure(
    engine: Engine,
    quarter: CampaignQuarter,
    configuration_identity: str,
    reason: str,
) -> None:
    """Record a bounded diagnostic state without changing request identity."""
    if not reason.strip():
        raise ValueError("Failure reason must not be empty.")

    table = _table_name()
    statement = text(
        f"""
        UPDATE {table}
        SET status = 'failed',
            failure_count = failure_count + 1,
            last_failure = :reason,
            completed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE campaign_key = :campaign_key
          AND year = :year
          AND quarter = :quarter
          AND configuration_identity = :configuration_identity
        """
    )
    with engine.begin() as connection:
        result = connection.execute(
            statement,
            {
                "campaign_key": CAMPAIGN_KEY,
                "year": quarter.year,
                "quarter": quarter.quarter,
                "configuration_identity": configuration_identity,
                "reason": reason.strip(),
            },
        )
        if result.rowcount != 1:
            raise ValueError(
                f"Cannot record failure for {quarter.label}: progress row or "
                "configuration identity does not match the current campaign."
            )
