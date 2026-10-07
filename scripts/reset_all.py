"""Compose all verified development reset operations behind one guarded command.

The command is inspect-only by default. It reuses the already-tested maintenance
functions for PostgreSQL recreation, project data cleanup, Airflow log cleanup,
and Airflow runtime cleanup rather than duplicating deletion logic.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Callable
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config import APP_ENV_DEV, resolve_app_environment
from pipeline.database import DatabaseConfig
from scripts import recreate_postgres_database
from scripts import reset_airflow_logs
from scripts import reset_airflow_runtime
from scripts import reset_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or execute the complete guarded development reset: "
            "PostgreSQL, project data, Airflow logs, and Airflow runtime state."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="After all preflight guards pass, allow one final reset confirmation.",
    )
    return parser.parse_args(argv)


def _display_targets(config: DatabaseConfig) -> None:
    logger.info("Reset-all targets")
    logger.info(
        "  PostgreSQL=%s:%d/%s | user=%s | schema=%s",
        config.host,
        config.port,
        config.database,
        config.user,
        config.schema,
    )
    logger.info("  data=%s", reset_data.DATA_DIRECTORY)
    logger.info("  airflow_logs=%s", reset_airflow_logs.AIRFLOW_LOG_DIRECTORY)
    logger.info(
        "  airflow_runtime=%s",
        reset_airflow_runtime.ORCHESTRATION_DIRECTORY,
    )
    logger.info(
        "  preserved_airflow_config=%s",
        reset_airflow_runtime.PRESERVED_AIRFLOW_CONFIG,
    )


def _run_preflight(config: DatabaseConfig) -> None:
    recreate_postgres_database._validate_static_guards(config)
    recreate_postgres_database._validate_runtime_guards(
        config,
        allow_residual_localexecutor_workers=True,
    )


def _confirmation(
    config: DatabaseConfig,
    input_fn: Callable[[str], str] = input,
) -> bool:
    while True:
        answer = input_fn(
            "Reset ALL development state listed above, including PostgreSQL "
            f"database {config.database!r}, data artifacts, Airflow logs, and "
            "Airflow SQLite/runtime state? [y/n]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes/y or no/n.")


def execute_reset(config: DatabaseConfig) -> dict[str, int]:
    """Execute verified reset components in failure-safe order."""
    stopped_workers = reset_airflow_runtime.stop_local_executor_workers()
    recreate_postgres_database._recreate_database(config)
    data_removed = reset_data.clear_data_contents()
    log_entries_removed = reset_airflow_logs.clear_airflow_logs()
    runtime_files_removed = len(reset_airflow_runtime.clear_airflow_runtime_state())
    return {
        "stopped_localexecutor_workers": len(stopped_workers),
        "data_entries_removed": data_removed,
        "airflow_log_entries_removed": log_entries_removed,
        "airflow_runtime_files_removed": runtime_files_removed,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        app_env = resolve_app_environment()
    except ValueError as error:
        logger.warning("Reset-all aborted | %s", error)
        return 2

    logger.info("Application environment | APP_ENV=%s", app_env)
    if app_env != APP_ENV_DEV:
        logger.warning("Reset-all unavailable | APP_ENV=%s.", app_env)
        return 3

    config = DatabaseConfig.from_env()
    _display_targets(config)

    try:
        _run_preflight(config)
    except (RuntimeError, ValueError) as error:
        logger.warning("Reset-all preflight failed | %s", error)
        return 4

    if not args.execute:
        logger.info(
            "Inspect-only preflight passed | no reset actions were executed. "
            "Use --execute only after reviewing every target above."
        )
        return 0

    if not _confirmation(config):
        logger.info("Reset-all cancelled by operator.")
        return 0

    logger.warning(
        "Executing complete development reset | database=%s | host=%s",
        config.database,
        config.host,
    )
    result = execute_reset(config)
    logger.info(
        "Reset-all complete | stopped_localexecutor_workers=%d | "
        "data_entries_removed=%d | airflow_log_entries_removed=%d | "
        "airflow_runtime_files_removed=%d",
        result["stopped_localexecutor_workers"],
        result["data_entries_removed"],
        result["airflow_log_entries_removed"],
        result["airflow_runtime_files_removed"],
    )
    logger.info(
        "Preserved tracked Airflow config | path=%s",
        reset_airflow_runtime.PRESERVED_AIRFLOW_CONFIG,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
