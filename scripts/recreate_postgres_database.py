"""Guarded maintenance entrypoint for recreating the development PostgreSQL database.

The command is inspect-only by default. Destructive recreation is available only
when APP_ENV=dev, all preflight guards pass, --execute is supplied, and the
operator confirms the exact target interactively.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Callable

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from psycopg import connect, sql

from config import APP_ENV_DEV, resolve_app_environment
from database.bootstrap import bootstrap
from orchestration.scheduling.schedule import campaign_schedule
from database.config import DatabaseConfig
from scripts.airflow_processes import list_airflow_processes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

# PostgreSQL maintenance databases must never be application reset targets.
SYSTEM_DATABASES = frozenset({"postgres", "template0", "template1"})

# Airflow task states that can execute now or resume automatically later.
BLOCKING_TASK_STATES = (
    "queued",
    "running",
    "deferred",
    "scheduled",
    "up_for_retry",
    "up_for_reschedule",
    "restarting",
)

# Airflow DagRun states that may still produce or resume task work.
BLOCKING_DAGRUN_STATES = ("queued", "running")

# noinspection SqlNoDataSourceInspection,SqlResolve
SQLITE_TABLE_EXISTS_QUERY = (
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1"
)
# noinspection SqlNoDataSourceInspection,SqlResolve
SQLITE_TASK_INSTANCE_COUNT_QUERY = (
    "SELECT COUNT(*) FROM task_instance WHERE state = ?"
)
# noinspection SqlNoDataSourceInspection,SqlResolve
SQLITE_DAG_RUN_COUNT_QUERY = (
    "SELECT COUNT(*) FROM dag_run WHERE dag_id = ? AND state = ?"
)

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or recreate the configured development PostgreSQL database. "
            "Recreation is unavailable outside APP_ENV=dev."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="After all guards pass, allow the interactive recreation prompt.",
    )
    return parser.parse_args(argv)


def _airflow_metadata_path() -> Path:
    repository_root = Path(__file__).resolve().parents[1]
    airflow_home = Path(os.getenv("AIRFLOW_HOME", repository_root / "orchestration"))
    sqlalchemy_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    if sqlalchemy_url.startswith("sqlite:///"):
        return Path(sqlalchemy_url.removeprefix("sqlite:///"))
    return airflow_home / "airflow.db"


def _airflow_task_state_counts(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {state: 0 for state in BLOCKING_TASK_STATES}
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)) as conn:
            table_exists = conn.execute(
                SQLITE_TABLE_EXISTS_QUERY,
                ("task_instance",),
            ).fetchone()
            if table_exists is None:
                logger.info(
                    "Airflow metadata preflight | task_instance table not found at %s; "
                    "treating blocking task counts as zero.",
                    path,
                )
                return {state: 0 for state in BLOCKING_TASK_STATES}
            result: dict[str, int] = {}
            for state in BLOCKING_TASK_STATES:
                count_row: tuple[int] | None = conn.execute(
                    SQLITE_TASK_INSTANCE_COUNT_QUERY,
                    (state,),
                ).fetchone()
                if count_row is None:
                    raise RuntimeError(
                        f"Unexpected empty COUNT result for task_instance state={state!r}."
                    )
                result[state] = int(
                    count_row[0]
                )
            return result
    except sqlite3.Error as error:
        raise RuntimeError(
            f"Unable to inspect Airflow SQLite metadata at {path}: "
            f"{type(error).__name__}"
        ) from error


def _airflow_dag_run_state_counts(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {state: 0 for state in BLOCKING_DAGRUN_STATES}
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)) as conn:
            table_exists = conn.execute(
                SQLITE_TABLE_EXISTS_QUERY,
                ("dag_run",),
            ).fetchone()
            if table_exists is None:
                logger.info(
                    "Airflow metadata preflight | dag_run table not found at %s; "
                    "treating blocking DagRun counts as zero.",
                    path,
                )
                return {state: 0 for state in BLOCKING_DAGRUN_STATES}
            result: dict[str, int] = {}
            for state in BLOCKING_DAGRUN_STATES:
                count_row: tuple[int] | None = conn.execute(
                    SQLITE_DAG_RUN_COUNT_QUERY,
                    ("cds_era5_land_etl", state),
                ).fetchone()
                if count_row is None:
                    raise RuntimeError(
                        f"Unexpected empty COUNT result for dag_run state={state!r}."
                    )
                result[state] = int(
                    count_row[0]
                )
            return result
    except sqlite3.Error as error:
        raise RuntimeError(
            f"Unable to inspect Airflow DagRuns at {path}: "
            f"{type(error).__name__}"
        ) from error



def _database_session_count(config: DatabaseConfig) -> int:
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
                "SELECT COUNT(*) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (config.database,),
            )
            row: tuple[int] | None = cursor.fetchone()
            if row is None:
                return 0
            return int(row[0])


def _display_target(config: DatabaseConfig) -> None:
    logger.info("PostgreSQL maintenance target")
    logger.info("  host=%s", config.host)
    logger.info("  port=%d", config.port)
    logger.info("  database=%s", config.database)
    logger.info("  user=%s", config.user)
    logger.info("  schema=%s", config.schema)


def _validate_static_guards(config: DatabaseConfig) -> None:
    if config.database.lower() in SYSTEM_DATABASES:
        raise RuntimeError(
            f"Refusing to recreate PostgreSQL system database {config.database!r}."
        )


def _validate_bootstrap_prerequisites(config: DatabaseConfig) -> None:
    """Read-only privilege and PostGIS checks before a destructive database drop.

    Preflight cannot guarantee success of future DDL; it rejects known blockers.
    """
    _validate_static_guards(config)
    if config.schema != "climate_data":
        raise RuntimeError("Recreation requires the canonical climate_data schema.")
    with connect(
        host=config.host, port=config.port, dbname="postgres",
        user=config.user, password=config.password, autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_has_role(d.datdba, 'MEMBER'), "
                "(r.rolcreatedb OR r.rolsuper), r.rolsuper "
                "FROM pg_database d CROSS JOIN pg_roles r "
                "WHERE d.datname = %s AND r.rolname = current_user",
                (config.database,),
            )
            row: tuple[bool, bool, bool] | None = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    f"Database {config.database!r} does not exist; use database.setup."
                )
            owns_database, can_create, is_superuser = row
            if not owns_database or not can_create:
                raise RuntimeError(
                    "Database recreation requires owner membership and CREATEDB privileges."
                )
            cursor.execute(
                "SELECT default_version FROM pg_available_extensions WHERE name='postgis'"
            )
            if cursor.fetchone() is None:
                raise RuntimeError(
                    "PostGIS server package is unavailable; refusing database DROP."
                )
            if not is_superuser:
                raise RuntimeError(
                    "Recreation requires superuser permission for PostGIS activation "
                    "in the newly created database."
                )


def _validate_runtime_guards(config: DatabaseConfig) -> None:
    if campaign_schedule() is not None:
        raise RuntimeError(
            "ERA5 campaign scheduling is enabled. Set "
            "ERA5_CAMPAIGN_SCHEDULE_ENABLED=false before database recreation."
        )

    task_counts = _airflow_task_state_counts(_airflow_metadata_path())
    logger.info(
        "Airflow task preflight | %s",
        " | ".join(f"{state}={count}" for state, count in task_counts.items()),
    )
    blocking_tasks = {state: count for state, count in task_counts.items() if count}
    if blocking_tasks:
        raise RuntimeError(
            "Airflow task instances can still run or resume: "
            + ", ".join(f"{state}={count}" for state, count in blocking_tasks.items())
        )

    dag_run_counts = _airflow_dag_run_state_counts(_airflow_metadata_path())
    logger.info(
        "Airflow DagRun preflight | %s",
        " | ".join(f"{state}={count}" for state, count in dag_run_counts.items()),
    )
    blocking_runs = {state: count for state, count in dag_run_counts.items() if count}
    if blocking_runs:
        raise RuntimeError(
            "ERA5-Land DagRuns can still run or resume: "
            + ", ".join(f"{state}={count}" for state, count in blocking_runs.items())
        )

    processes = list_airflow_processes()
    logger.info("Airflow process preflight | blocking_processes=%d", len(processes))
    if processes:
        rendered = "; ".join(f"pid={pid} cmd={command}" for pid, command in processes)
        raise RuntimeError(
            "Airflow processes that can resume or schedule work are still running: "
            + rendered
        )

    session_count = _database_session_count(config)
    logger.info(
        "PostgreSQL session preflight | database=%s | other_sessions=%d",
        config.database,
        session_count,
    )
    if session_count:
        raise RuntimeError(
            f"Target database {config.database!r} still has "
            f"{session_count} other PostgreSQL session(s)."
        )


def _confirmation(
    config: DatabaseConfig,
    input_fn: Callable[[str], str] = input,
) -> bool:
    while True:
        answer = input_fn(
            f"Delete and recreate PostgreSQL database {config.database!r} "
            f"on {config.host}:{config.port}? [y/n]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes/y or no/n.")


def _recreate_database(config: DatabaseConfig) -> None:
    with connect(
        host=config.host,
        port=config.port,
        dbname="postgres",
        user=config.user,
        password=config.password,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            logger.info("DATABASE | DROP %s | starting", config.database)
            cursor.execute(
                sql.SQL("DROP DATABASE ") + sql.Identifier(config.database)
            )
            logger.info("DATABASE | DROP %s | complete", config.database)
            logger.info("DATABASE | CREATE %s | starting", config.database)
            cursor.execute(
                sql.SQL("CREATE DATABASE ")
                + sql.Identifier(config.database)
                + sql.SQL(" OWNER ")
                + sql.Identifier(config.user)
            )

    logger.info("DATABASE | CREATE %s | complete", config.database)
    logger.info("DATABASE | Bootstrap 8 tables + PostGIS | starting")
    # Canonical root bootstrap initializes all eight application tables.
    bootstrap(config)
    logger.info("DATABASE | Bootstrap 8 tables + PostGIS | complete")


def validate_static_guards(config: DatabaseConfig) -> None:
    """Public wrapper for static safety guards."""
    _validate_static_guards(config)


def validate_bootstrap_prerequisites(config: DatabaseConfig) -> None:
    """Public wrapper for read-only privilege and PostGIS preflight checks."""
    _validate_bootstrap_prerequisites(config)


def recreate_database(config: DatabaseConfig) -> None:
    """Public wrapper for guarded DROP/CREATE and canonical bootstrap."""
    _recreate_database(config)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        app_env = resolve_app_environment()
    except ValueError as error:
        logger.warning("Database maintenance aborted | %s", error)
        return 2

    logger.info("Application environment | APP_ENV=%s", app_env)
    if app_env != APP_ENV_DEV:
        logger.warning(
            "Database maintenance unavailable | database recreation is disabled "
            "when APP_ENV=%s.",
            app_env,
        )
        return 3

    config = DatabaseConfig.from_env()
    _display_target(config)

    try:
        _validate_static_guards(config)
    except RuntimeError as error:
        logger.warning("Database maintenance unavailable | %s", error)
        return 3

    try:
        _validate_runtime_guards(config)
        _validate_bootstrap_prerequisites(config)
    except (RuntimeError, ValueError) as error:
        logger.warning("Database recreation preflight failed | %s", error)
        return 4

    if not args.execute:
        logger.info(
            "Inspect-only preflight passed | no database changes requested. "
            "Use --execute only after reviewing the displayed target."
        )
        return 0

    if not _confirmation(config):
        logger.info("Database recreation cancelled by operator.")
        return 0

    logger.warning(
        "Recreating confirmed development database | database=%s | host=%s",
        config.database,
        config.host,
    )
    _validate_runtime_guards(config)
    _validate_bootstrap_prerequisites(config)
    _recreate_database(config)
    logger.info(
        "Database recreation complete | database=%s | schema=%s",
        config.database,
        config.schema,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
