"""Process-table primitives shared by Airflow maintenance commands.

This module owns only process discovery and signal mechanics. It intentionally
does not decide whether a reset, destroy, or other command is allowed to stop a
given Airflow process; that policy belongs to the calling command.
"""

from __future__ import annotations

import os
import signal
import time
from collections.abc import Callable, Iterable
from pathlib import Path

ProcessInfo = tuple[int, str]

AIRFLOW_SERVICE_MARKERS = (
    b"airflow api-server",
    b"airflow scheduler",
    b"airflow triggerer",
    b"airflow dag-processor",
)

LOCAL_EXECUTOR_WORKER_MARKER = b"airflow worker -- localexecutor"

AIRFLOW_PROCESS_MARKERS = (
    *AIRFLOW_SERVICE_MARKERS,
    LOCAL_EXECUTOR_WORKER_MARKER,
    b"airflow celery worker",
)


def list_matching_processes(
    markers: tuple[bytes, ...],
    *,
    proc_root: Path = Path("/proc"),
    exclude_pids: Iterable[int] = (),
) -> list[ProcessInfo]:
    """Return processes whose command line contains one of the byte markers."""
    if not proc_root.is_dir():
        raise RuntimeError(f"Unable to inspect process table at {proc_root}.")

    excluded = set(exclude_pids)
    excluded.add(os.getpid())
    matches: list[ProcessInfo] = []

    for entry in proc_root.iterdir():
        if not entry.name.isdecimal():
            continue
        pid = int(entry.name)
        if pid in excluded:
            continue
        try:
            raw = (entry / "cmdline").read_bytes().replace(b"\0", b" ").strip()
        except OSError:
            continue
        lowered = raw.lower()
        if any(marker in lowered for marker in markers):
            matches.append((pid, raw.decode(errors="replace")))

    return sorted(matches, key=lambda item: item[0])


def list_airflow_services(
    *,
    proc_root: Path = Path("/proc"),
) -> list[ProcessInfo]:
    """Return Airflow scheduler/API/triggerer/DAG-processor processes."""
    return list_matching_processes(AIRFLOW_SERVICE_MARKERS, proc_root=proc_root)


def list_localexecutor_workers(
    *,
    proc_root: Path = Path("/proc"),
) -> list[ProcessInfo]:
    """Return Airflow LocalExecutor worker processes."""
    return list_matching_processes(
        (LOCAL_EXECUTOR_WORKER_MARKER,),
        proc_root=proc_root,
    )


def list_airflow_processes(
    *,
    proc_root: Path = Path("/proc"),
) -> list[ProcessInfo]:
    """Return all recognized Airflow service and worker processes."""
    return list_matching_processes(AIRFLOW_PROCESS_MARKERS, proc_root=proc_root)


def terminate_processes(
    processes: Iterable[ProcessInfo],
    *,
    remaining_processes_fn: Callable[[], list[ProcessInfo]],
    grace_seconds: float = 2.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    kill_fn: Callable[[int, int], None] = os.kill,
) -> list[int]:
    """Terminate explicitly selected PIDs, escalating only those still present."""
    selected = {pid: command for pid, command in processes}
    if not selected:
        return []

    for pid in selected:
        try:
            kill_fn(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    if grace_seconds > 0:
        sleep_fn(grace_seconds)

    remaining = {
        pid
        for pid, _ in remaining_processes_fn()
        if pid in selected
    }
    for pid in sorted(remaining):
        try:
            kill_fn(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    still_remaining = {
        pid
        for pid, _ in remaining_processes_fn()
        if pid in selected
    }
    if still_remaining:
        raise RuntimeError(
            "Unable to stop selected Airflow process(es): "
            + ", ".join(str(pid) for pid in sorted(still_remaining))
        )

    return sorted(selected)
