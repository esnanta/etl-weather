"""Print a read-only summary of the exact 2016-2025 campaign quarters.

Operators need to see missing and failed quarters without counting unrelated
manual progress rows or relying on large Airflow XCom payloads.
"""

from __future__ import annotations

from sqlalchemy import create_engine

from orchestration.reporting import format_campaign_report, read_campaign_report
from pipeline.database import DatabaseConfig


def main() -> None:
    engine = create_engine(DatabaseConfig.from_env().url, future=True)
    try:
        print(format_campaign_report(read_campaign_report(engine)))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
