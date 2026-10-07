"""Guarded maintenance command for clearing generated local Airflow runtime state.

The reset may terminate residual LocalExecutor worker processes after the main
Airflow services have stopped. Scheduler, API server, triggerer, and DAG
processor processes are never terminated automatically.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Callable

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config import APP_ENV_DEV, resolve_app_environment
from scripts.airflow_processes import (
    list_airflow_services,
    list_localexecutor_workers,
    terminate_processes,
)

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

# Reset policy remains local to this command: main Airflow services are blockers,
# while residual LocalExecutor workers may be stopped after services are absent.


def blocking_airflow_services(
    proc_root: Path = Path("/proc"),
) -> list[tuple[int, str]]:
    """Return main Airflow services that block guarded runtime reset."""
    return list_airflow_services(proc_root=proc_root)


def local_executor_workers(
    proc_root: Path = Path("/proc"),
) -> list[tuple[int, str]]:
    """Return residual LocalExecutor workers eligible for runtime cleanup."""
    return list_localexecutor_workers(proc_root=proc_root)


def stop_local_executor_workers(
    *,
    proc_root: Path = Path("/proc"),
    grace_seconds: float = 2.0,
    sleep_fn: Callable[[float], None] | None = None,
    kill_fn: Callable[[int, int], None] | None = None,
) -> list[int]:
    """Stop residual LocalExecutor workers only when main services are absent."""
    blocking = blocking_airflow_services(proc_root)
    if blocking:
        rendered = "; ".join(
            f"pid={pid} cmd={command}" for pid, command in blocking
        )
        raise RuntimeError(
            "Refusing to terminate LocalExecutor workers while main Airflow "
            "services are still running: " + rendered
        )

    workers = local_executor_workers(proc_root)
    kwargs: dict[str, object] = {
        "remaining_processes_fn": lambda: local_executor_workers(proc_root),
        "grace_seconds": grace_seconds,
    }
    if sleep_fn is not None:
        kwargs["sleep_fn"] = sleep_fn
    if kill_fn is not None:
        kwargs["kill_fn"] = kill_fn

    return terminate_processes(workers, **kwargs)  # type: ignore[arg-type]


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


def reset_airflow_runtime_state() -> dict[str, int]:
    """Stop residual LocalExecutor workers and clear generated runtime files."""
    stopped_workers = stop_local_executor_workers()
    removed_files = clear_airflow_runtime_state()
    return {
        "stopped_local_executor_workers": len(stopped_workers),
        "removed_files": len(removed_files),
    }


def _confirmation(input_fn: Callable[[str], str] = input) -> bool:
    while True:
        answer = input_fn(
            "Stop residual LocalExecutor workers and delete generated Airflow "
            "SQLite/auth/PID runtime state while preserving "
            "orchestration/airflow.cfg? [y/n]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes/y or no/n.")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or clear generated local Airflow runtime state. Residual "
            "LocalExecutor workers are stopped only after main Airflow services "
            "are confirmed absent. The tracked orchestration/airflow.cfg file "
            "is always preserved."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Allow the interactive reset prompt when APP_ENV=dev.",
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

    try:
        blocking = blocking_airflow_services()
        workers = local_executor_workers()
    except RuntimeError as error:
        logger.warning("Airflow runtime reset preflight failed | %s", error)
        return 4

    logger.info(
        "Airflow runtime process preflight | blocking_services=%d | "
        "localexecutor_workers=%d",
        len(blocking),
        len(workers),
    )
    if blocking:
        rendered = "; ".join(
            f"pid={pid} cmd={command}" for pid, command in blocking
        )
        logger.warning(
            "Airflow runtime reset blocked | stop main Airflow services first: %s",
            rendered,
        )
        return 4

    if not args.execute:
        logger.info(
            "Inspect-only mode | no LocalExecutor workers were stopped and no "
            "Airflow runtime files were deleted."
        )
        return 0
    if not _confirmation():
        logger.info("Airflow runtime reset cancelled by operator.")
        return 0

    try:
        result = reset_airflow_runtime_state()
    except RuntimeError as error:
        logger.warning("Airflow runtime reset failed | %s", error)
        return 4

    logger.info(
        "Airflow runtime reset complete | stopped_localexecutor_workers=%d | "
        "removed_files=%d",
        result["stopped_local_executor_workers"],
        result["removed_files"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
