"""Guarded maintenance command for clearing generated pipeline data artifacts."""

from __future__ import annotations

import argparse
import logging
import shutil
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

DATA_DIRECTORY = project_root / "data"


def clear_data_contents(data_directory: Path = DATA_DIRECTORY) -> int:
    """Delete every child of the data directory while preserving the directory."""
    if not data_directory.exists():
        return 0
    removed = 0
    for child in data_directory.iterdir():
        if child.is_symlink() or child.is_file():
            child.unlink()
        else:
            shutil.rmtree(child)
        removed += 1
    return removed


def _confirmation(input_fn: Callable[[str], str] = input) -> bool:
    while True:
        answer = input_fn(
            f"Delete all contents inside {DATA_DIRECTORY}? [y/n]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes/y or no/n.")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect or clear generated contents inside the project data directory."
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
        logger.warning("Data reset aborted | %s", error)
        return 2

    logger.info("Application environment | APP_ENV=%s", app_env)
    if app_env != APP_ENV_DEV:
        logger.warning("Data reset unavailable | APP_ENV=%s.", app_env)
        return 3

    logger.info("Data reset target | path=%s", DATA_DIRECTORY)
    if not args.execute:
        logger.info("Inspect-only mode | no data files were deleted.")
        return 0
    if not _confirmation():
        logger.info("Data reset cancelled by operator.")
        return 0

    removed = clear_data_contents()
    logger.info("Data reset complete | removed_entries=%d", removed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
