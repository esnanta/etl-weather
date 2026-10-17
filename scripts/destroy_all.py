"""Intentionally destroy all local development runtime and data state.

Unlike reset commands, destroy is a clean-slate operation. It is available only
when APP_ENV=dev, requires the exact DESTROY confirmation, stops recognized
Airflow runtime processes, recreates the configured PostgreSQL database, clears
generated data/log/runtime state, and verifies the resulting clean state.

Tracked project configuration, source code, .env, airflow.cfg, and the Airflow
JWT key are never deletion targets of this command.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from psycopg import connect

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config import APP_ENV_DEV, resolve_app_environment
from database.config import DatabaseConfig, APPLICATION_SCHEMA
from database.verification import CLIMATE_TABLES, GEOSPATIAL_TABLES
from scripts import recreate_postgres_database
from scripts import reset_airflow_logs
from scripts import reset_airflow_runtime
from scripts import reset_data
from scripts.airflow_processes import list_airflow_processes, terminate_processes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

def _section(title: str) -> None:
    """Make major phases recognizable in terminal output."""
    # Section headings are presentation, not timestamped log events.
    print(f"\n{'=' * 68}\n{title}", file=sys.stderr, flush=True)


EXPECTED_APPLICATION_TABLES = tuple(sorted(CLIMATE_TABLES | GEOSPATIAL_TABLES))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or intentionally destroy all local development runtime/data "
            "state and recreate the configured application database."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Allow the exact DESTROY confirmation and clean-slate operation.",
    )
    return parser.parse_args(argv)


def _display_targets(
    config: DatabaseConfig,
    processes: list[tuple[int, str]],
) -> None:
    _section("DESTROY ALL | TARGETS (review before confirmation)")
    logger.warning(
        "  PostgreSQL=%s:%d/%s | user=%s | schema=%s",
        config.host,
        config.port,
        config.database,
        config.user,
        config.schema,
    )
    logger.warning("  data=%s", reset_data.DATA_DIRECTORY)
    logger.warning("  airflow_logs=%s", reset_airflow_logs.AIRFLOW_LOG_DIRECTORY)
    logger.warning(
        "  airflow_runtime=%s",
        reset_airflow_runtime.ORCHESTRATION_DIRECTORY,
    )
    logger.warning(
        "  preserved_airflow_config=%s",
        reset_airflow_runtime.PRESERVED_AIRFLOW_CONFIG,
    )
    logger.warning("  recognized_airflow_processes=%d", len(processes))
    for pid, command in processes:
        logger.warning("    pid=%d | cmd=%s", pid, command)


def _confirmation(input_fn: Callable[[str], str] = input) -> bool:
    answer = input_fn(
        "Type DESTROY to delete all local development runtime and data state: "
    )
    return answer == "DESTROY"


def _database_sessions(config: DatabaseConfig) -> list[dict[str, object]]:
    """Return external sessions connected to the target application database."""
    with connect(
        host=config.host,
        port=config.port,
        dbname="postgres",
        user=config.user,
        password=config.password,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pid, usename, application_name, client_addr::text, state "
                "FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid() "
                "ORDER BY pid",
                (config.database,),
            )
            return [
                {
                    "pid": row[0],
                    "user": row[1],
                    "application_name": row[2],
                    "client_addr": row[3],
                    "state": row[4],
                }
                for row in cursor.fetchall()
            ]


def _require_no_database_sessions(config: DatabaseConfig) -> None:
    sessions = _database_sessions(config)
    if not sessions:
        logger.info(
            "PostgreSQL destroy preflight | database=%s | other_sessions=0",
            config.database,
        )
        return

    rendered = "\n".join(
        "  - pid={pid} user={user} app={application_name!r} "
        "client={client_addr!r} state={state!r}".format(**session)
        for session in sessions
    )
    raise RuntimeError(
        f"Cannot recreate database {config.database!r}: "
        f"{len(sessions)} external PostgreSQL connection(s) are still open.\n"
        f"{rendered}\n"
        "Action: disconnect the listed database sessions in PhpStorm/DataGrip "
        "or other clients, then rerun the command. "
        "Database recreation and file cleanup have NOT started."
    )


def _stop_all_airflow_processes() -> int:
    processes = list_airflow_processes()
    if not processes:
        logger.info("Airflow destroy stop | recognized_processes=0")
        return 0

    for pid, command in processes:
        logger.warning("Stopping Airflow process | pid=%d | cmd=%s", pid, command)

    stopped = terminate_processes(
        processes,
        remaining_processes_fn=list_airflow_processes,
    )
    remaining = list_airflow_processes()
    if remaining:
        rendered = "; ".join(
            f"pid={pid} cmd={command}" for pid, command in remaining
        )
        raise RuntimeError(
            "Recognized Airflow process(es) remain after termination: " + rendered
        )

    logger.info("Airflow destroy stop complete | stopped_processes=%d", len(stopped))
    return len(stopped)


def _runtime_files_remaining() -> list[Path]:
    directory = reset_airflow_runtime.ORCHESTRATION_DIRECTORY
    remaining = [
        directory / name
        for name in reset_airflow_runtime.RUNTIME_FILENAMES
        if (directory / name).exists() or (directory / name).is_symlink()
    ]
    remaining.extend(
        path
        for path in directory.glob("*.pid")
        if path.is_file() or path.is_symlink()
    )
    return remaining


def _directory_entry_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.iterdir())


def _database_tables_ready(config: DatabaseConfig) -> bool:
    with connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s",
                (config.schema,),
            )
            found = {row[0] for row in cursor.fetchall()}
            cursor.execute(
                "SELECT n.nspname FROM pg_extension e "
                "JOIN pg_namespace n ON n.oid=e.extnamespace "
                "WHERE e.extname='postgis'"
            )
            extension_row = cursor.fetchone()
    return (
        set(EXPECTED_APPLICATION_TABLES).issubset(found)
        and extension_row is not None
        and extension_row[0] == APPLICATION_SCHEMA
    )


def _verify_clean_state(config: DatabaseConfig) -> dict[str, int]:
    remaining_processes = list_airflow_processes()
    remaining_runtime_files = _runtime_files_remaining()
    data_entries = _directory_entry_count(reset_data.DATA_DIRECTORY)
    log_entries = _directory_entry_count(reset_airflow_logs.AIRFLOW_LOG_DIRECTORY)
    database_ready = _database_tables_ready(config)

    result = {
        "airflow_processes_remaining": len(remaining_processes),
        "airflow_runtime_files_remaining": len(remaining_runtime_files),
        "data_entries_remaining": data_entries,
        "airflow_log_entries_remaining": log_entries,
        "database_ready": int(database_ready),
    }

    if (
        result["airflow_processes_remaining"]
        or result["airflow_runtime_files_remaining"]
        or result["data_entries_remaining"]
        or result["airflow_log_entries_remaining"]
        or not database_ready
    ):
        raise RuntimeError(
            "Destroy final verification failed | "
            + " | ".join(f"{key}={value}" for key, value in result.items())
        )

    return result


def execute_destroy(config: DatabaseConfig) -> dict[str, int]:
    """Execute clean-slate destruction in failure-safe order."""
    _section("PREFLIGHT | Check PostgreSQL privileges, PostGIS and sessions")
    recreate_postgres_database._validate_bootstrap_prerequisites(config)
    _require_no_database_sessions(config)

    _section("STEP 1/6 | Stop Airflow processes")
    stopped_processes = _stop_all_airflow_processes()
    _section("STEP 2/6 | Check open PostgreSQL sessions")
    _require_no_database_sessions(config)

    _section("STEP 3/6 | DROP, CREATE and bootstrap database (8 tables + PostGIS)")
    recreate_postgres_database._recreate_database(config)
    _section("STEP 4/6 | Remove generated NetCDF/Parquet data")
    data_removed = reset_data.clear_data_contents()
    _section("STEP 5/6 | Remove Airflow logs")
    log_entries_removed = reset_airflow_logs.clear_airflow_logs()
    logger.info("DESTROY | Remove Airflow runtime state")
    runtime_files_removed = len(reset_airflow_runtime.clear_airflow_runtime_state())

    _section("STEP 6/6 | Verify database and clean runtime state")
    verification = _verify_clean_state(config)
    return {
        "stopped_airflow_processes": stopped_processes,
        "data_entries_removed": data_removed,
        "airflow_log_entries_removed": log_entries_removed,
        "airflow_runtime_files_removed": runtime_files_removed,
        **verification,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        app_env = resolve_app_environment()
    except ValueError as error:
        _section("DESTROY ALL | BLOCKED")
        logger.warning("Reason: %s", error)
        logger.warning("No destructive operation was started.")
        return 2

    _section("DESTROY ALL | PREFLIGHT")
    logger.info("Application environment | APP_ENV=%s", app_env)
    if app_env != APP_ENV_DEV:
        logger.warning("Destroy-all unavailable | APP_ENV=%s.", app_env)
        logger.warning("Reason: destructive operations require APP_ENV=dev. No changes made.")
        return 3

    config = DatabaseConfig.from_env()

    try:
        recreate_postgres_database._validate_static_guards(config)
        processes = list_airflow_processes()
    except RuntimeError as error:
        _section("DESTROY ALL | PREFLIGHT FAILED")
        logger.warning("Reason: %s", error)
        logger.warning("No destructive operation was started.")
        return 4

    _display_targets(config, processes)

    if not args.execute:
        _section("DESTROY ALL | INSPECTION COMPLETE")
        logger.info("No Airflow process, database, data, logs, or runtime state was changed.")
        return 0

    logger.info("DESTROY | Confirmation required: type exactly DESTROY")
    if not _confirmation():
        _section("DESTROY ALL | CANCELLED")
        logger.info("Exact DESTROY confirmation not supplied; no changes made.")
        return 0

    _section("DESTROY ALL | EXECUTION STARTED")
    logger.warning(
        "Executing intentional development destroy | database=%s | host=%s",
        config.database,
        config.host,
    )

    try:
        result = execute_destroy(config)
    except RuntimeError as error:
        _section("DESTROY ALL | FAILED")
        logger.error("Destroy-all failed | %s", error)
        logger.error("Review the failure above before retrying.")
        logger.error("Some earlier steps may already have completed; do not assume full rollback.")
        return 5

    _section("DESTROY ALL | SUCCESS")
    logger.info(
        "Destroy-all complete | stopped_airflow_processes=%d | "
        "data_entries_removed=%d | airflow_log_entries_removed=%d | "
        "airflow_runtime_files_removed=%d",
        result["stopped_airflow_processes"],
        result["data_entries_removed"],
        result["airflow_log_entries_removed"],
        result["airflow_runtime_files_removed"],
    )
    logger.info(
        "Destroy verification | airflow_processes_remaining=%d | "
        "airflow_runtime_files_remaining=%d | data_entries_remaining=%d | "
        "airflow_log_entries_remaining=%d | database_ready=%d",
        result["airflow_processes_remaining"],
        result["airflow_runtime_files_remaining"],
        result["data_entries_remaining"],
        result["airflow_log_entries_remaining"],
        result["database_ready"],
    )
    logger.info(
        "Preserved project configuration | airflow_cfg=%s",
        reset_airflow_runtime.PRESERVED_AIRFLOW_CONFIG,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
