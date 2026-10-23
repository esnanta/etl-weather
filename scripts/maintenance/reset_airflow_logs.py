"""Guarded maintenance command for clearing generated Airflow logs."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from config import APP_ENV_DEV, resolve_app_environment
from scripts.helpers.reset_helpers import (
    clear_directory_contents,
    confirmation_for_target,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

AIRFLOW_LOG_DIRECTORY = project_root / "orchestration" / "logs"


def clear_airflow_logs(log_directory: Path = AIRFLOW_LOG_DIRECTORY) -> int:
    """Delete every child of the Airflow log directory while preserving it."""
    return clear_directory_contents(log_directory)


def _confirmation() -> bool:
    return confirmation_for_target(AIRFLOW_LOG_DIRECTORY)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect or clear generated Airflow log contents."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Allow the interactive deletion prompt when APP_ENV=dev.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        app_env = resolve_app_environment()
    except ValueError as error:
        logger.warning("Airflow log reset aborted | %s", error)
        return 2

    logger.info("Application environment | APP_ENV=%s", app_env)
    if app_env != APP_ENV_DEV:
        logger.warning("Airflow log reset unavailable | APP_ENV=%s.", app_env)
        return 3

    logger.info("Airflow log reset target | path=%s", AIRFLOW_LOG_DIRECTORY)
    if not args.execute:
        logger.info("Inspect-only mode | no Airflow logs were deleted.")
        return 0
    if not _confirmation():
        logger.info("Airflow log reset cancelled by operator.")
        return 0

    removed = clear_airflow_logs()
    logger.info("Airflow log reset complete | removed_entries=%d", removed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
