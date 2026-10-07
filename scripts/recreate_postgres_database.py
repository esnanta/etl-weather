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
from orchestration.repositories.database import initialize_application_storage
from orchestration.scheduling.schedule import campaign_schedule
from pipeline.database import DatabaseConfig

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

# Airflow service/process markers that can schedule, resume, or execute task work.
AIRFLOW_PROCESS_MARKERS = (
    b"airflow api-server",
    b"airflow scheduler",
    b"airflow triggerer",
    b"airflow dag-processor",
    b"airflow worker",
    b"airflow celery worker",
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
    project_root = Path(__file__).resolve().parents[1]
    airflow_home = Path(os.getenv("AIRFLOW_HOME", project_root / "orchestration"))
    sqlalchemy_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    if sqlalchemy_url.startswith("sqlite:///"):
        return Path(sqlalchemy_url.removeprefix("sqlite:///"))
    return airflow_home / "airflow.db"


def _airflow_task_state_counts(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {state: 0 for state in BLOCKING_TASK_STATES}
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)) as conn:
            result = {}
            for state in BLOCKING_TASK_STATES:
                result[state] = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM task_instance WHERE state = ?",
                        (state,),
                    ).fetchone()[0]
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
            result = {}
            for state in BLOCKING_DAGRUN_STATES:
                result[state] = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM dag_run "
                        "WHERE dag_id = ? AND state = ?",
                        ("cds_era5_land_etl", state),
                    ).fetchone()[0]
                )
            return result
    except sqlite3.Error as error:
        raise RuntimeError(
            f"Unable to inspect Airflow DagRuns at {path}: "
            f"{type(error).__name__}"
        ) from error


def _airflow_processes(proc_root: Path = Path("/proc")) -> list[tuple[int, str]]:
    processes: list[tuple[int, str]] = []
    if not proc_root.is_dir():
        raise RuntimeError(f"Unable to inspect process table at {proc_root}.")
    for entry in proc_root.iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            raw = (entry / "cmdline").read_bytes().replace(b"\0", b" ").strip()
        except OSError:
            continue
        lowered = raw.lower()
        if any(marker in lowered for marker in AIRFLOW_PROCESS_MARKERS):
            processes.append((int(entry.name), raw.decode(errors="replace")))
    return processes


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
            row = cursor.fetchone()
            return int(row[0]) if row else 0


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


def _validate_runtime_guards(
    config: DatabaseConfig,
    *,
    allow_residual_localexecutor_workers: bool = False,
    allow_deferred_task_instances: bool = False,
    allow_running_dagrun_with_only_deferred_tasks: bool = False,
) -> None:
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
    blocking_tasks = {
        state: count
        for state, count in task_counts.items()
        if count and not (allow_deferred_task_instances and state == "deferred")
    }
    if blocking_tasks:
        raise RuntimeError(
            "Airflow task instances can still run or resume: "
            + ", ".join(f"{state}={count}" for state, count in blocking_tasks.items())
        )

    processes = _airflow_processes()
    if allow_residual_localexecutor_workers:
        localexecutor_marker = b"airflow worker -- localexecutor"
        blocking_processes = [
            (pid, command)
            for pid, command in processes
            if localexecutor_marker not in command.lower().encode()
        ]
        residual_workers = len(processes) - len(blocking_processes)
    else:
        blocking_processes = processes
        residual_workers = 0

    logger.info(
        "Airflow process preflight | blocking_processes=%d | "
        "residual_localexecutor_workers=%d",
        len(blocking_processes),
        residual_workers,
    )
    if blocking_processes:
        rendered = "; ".join(
            f"pid={pid} cmd={command}" for pid, command in blocking_processes
        )
        raise RuntimeError(
            "Airflow processes that can resume or schedule work are still running: "
            + rendered
        )

    dag_run_counts = _airflow_dag_run_state_counts(_airflow_metadata_path())
    logger.info(
        "Airflow DagRun preflight | %s",
        " | ".join(f"{state}={count}" for state, count in dag_run_counts.items()),
    )
    blocking_runs = {state: count for state, count in dag_run_counts.items() if count}
    if (
        allow_running_dagrun_with_only_deferred_tasks
        and not blocking_tasks
        and task_counts.get("deferred", 0) > 0
        and blocking_runs
        and set(blocking_runs) == {"running"}
    ):
        blocking_runs = {}
    if blocking_runs:
        raise RuntimeError(
            "ERA5-Land DagRuns can still run or resume: "
            + ", ".join(f"{state}={count}" for state, count in blocking_runs.items())
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
            cursor.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(config.database))
            )
            cursor.execute(
                sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(config.database),
                    sql.Identifier(config.user),
                )
            )

    from sqlalchemy import create_engine

    engine = create_engine(config.url, future=True)
    try:
        initialize_application_storage(engine, config)
    finally:
        engine.dispose()


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
    _recreate_database(config)
    logger.info(
        "Database recreation complete | database=%s | schema=%s",
        config.database,
        config.schema,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
