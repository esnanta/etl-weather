"""Print a read-only summary of the exact 2016-2025 campaign quarters.

Operators need to see missing and failed quarters without counting unrelated
manual progress rows or relying on large Airflow XCom payloads.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine

from config import validate_and_log_app_environment
from orchestration.campaign.reporting import format_campaign_report, read_campaign_report
from database.config import DatabaseConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def main() -> None:
    try:
        validate_and_log_app_environment(logger)
    except ValueError:
        raise SystemExit(2) from None
    engine = create_engine(DatabaseConfig.from_env().url, future=True)
    try:
        print(format_campaign_report(read_campaign_report(engine)))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
