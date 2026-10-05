"""Guarded maintenance command for clearing generated local Airflow runtime state."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Callable

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config import APP_ENV_DEV, resolve_app_environment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

ORCHESTRATION_DIRECTORY = project_root / "orchestration"

# Tracked canonical Airflow configuration is intentionally preserved.
PRESERVED_AIRFLOW_CONFIG = ORCHESTRATION_DIRECTORY / "airflow.cfg"

# Generated local runtime files safe to remove during a development reset.
RUNTIME_FILENAMES = (
    "airflow.db",
    "airflow.db-shm",
    "airflow.db-wal",
    "airflow.db-journal",
    "simple_auth_manager_passwords.json.generated",
)


def clear_airflow_runtime_state(
    orchestration_directory: Path = ORCHESTRATION_DIRECTORY,
) -> list[Path]:
    """Remove generated SQLite/auth/PID state while preserving tracked config."""
    removed: list[Path] = []
    for name in RUNTIME_FILENAMES:
        path = orchestration_directory / name
        if path.exists() or path.is_symlink():
            path.unlink()
            removed.append(path)

    for path in orchestration_directory.glob("*.pid"):
        if path.is_file() or path.is_symlink():
            path.unlink()
            removed.append(path)

    return removed


def _confirmation(input_fn: Callable[[str], str] = input) -> bool:
    while True:
        answer = input_fn(
            "Delete generated Airflow SQLite/auth/PID runtime state while "
            "preserving orchestration/airflow.cfg? [y/n]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes/y or no/n.")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or clear generated local Airflow SQLite/auth/PID state. "
            "The tracked orchestration/airflow.cfg file is always preserved."
        )
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
        logger.warning("Airflow runtime reset aborted | %s", error)
        return 2

    logger.info("Application environment | APP_ENV=%s", app_env)
    if app_env != APP_ENV_DEV:
        logger.warning("Airflow runtime reset unavailable | APP_ENV=%s.", app_env)
        return 3

    logger.info("Airflow runtime reset target | directory=%s", ORCHESTRATION_DIRECTORY)
    logger.info("Preserved tracked config | path=%s", PRESERVED_AIRFLOW_CONFIG)
    if not args.execute:
        logger.info("Inspect-only mode | no Airflow runtime files were deleted.")
        return 0
    if not _confirmation():
        logger.info("Airflow runtime reset cancelled by operator.")
        return 0

    removed = clear_airflow_runtime_state()
    logger.info("Airflow runtime reset complete | removed_files=%d", len(removed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
