"""Verify shared Airflow process discovery and signal mechanics."""

from __future__ import annotations

from pathlib import Path
import signal

import pytest

from scripts.helpers import airflow_processes


def _write_process(proc_root: Path, pid: int, command: str) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(command.replace(" ", "\0").encode())


def _remove_process(proc_root: Path, pid: int) -> None:
    process = proc_root / str(pid)
    cmdline = process / "cmdline"
    if cmdline.exists():
        cmdline.unlink()
    if process.exists():
        process.rmdir()


def test_list_airflow_processes_detects_services_and_workers(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 101, "airflow scheduler")
    _write_process(proc_root, 102, "airflow triggerer")
    _write_process(proc_root, 103, "airflow worker -- LocalExecutor: <idle>")
    _write_process(proc_root, 104, "python unrelated.py")

    assert airflow_processes.list_airflow_processes(proc_root=proc_root) == [
        (101, "airflow scheduler"),
        (102, "airflow triggerer"),
        (103, "airflow worker -- LocalExecutor: <idle>"),
    ]


def test_process_views_keep_services_and_localexecutor_workers_separate(
    tmp_path: Path,
) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 201, "airflow api-server")
    _write_process(proc_root, 202, "airflow dag-processor")
    _write_process(proc_root, 203, "airflow worker -- LocalExecutor: <idle>")

    assert airflow_processes.list_airflow_services(proc_root=proc_root) == [
        (201, "airflow api-server"),
        (202, "airflow dag-processor"),
    ]
    assert airflow_processes.list_localexecutor_workers(proc_root=proc_root) == [
        (203, "airflow worker -- LocalExecutor: <idle>"),
    ]


def test_process_discovery_rejects_missing_proc_root(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Unable to inspect process table"):
        airflow_processes.list_airflow_processes(
            proc_root=tmp_path / "missing-proc"
        )


def test_terminate_processes_uses_sigterm_only_when_process_exits() -> None:
    remaining = [(301, "airflow scheduler")]
    calls: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        if sig == signal.SIGTERM:
            remaining.clear()

    stopped = airflow_processes.terminate_processes(
        [(301, "airflow scheduler")],
        remaining_processes_fn=lambda: list(remaining),
        grace_seconds=0,
        kill_fn=kill,
    )

    assert stopped == [301]
    assert calls == [(301, signal.SIGTERM)]


def test_terminate_processes_escalates_only_selected_remaining_pids() -> None:
    remaining = [
        (401, "airflow scheduler"),
        (402, "airflow triggerer"),
        (999, "unrelated process"),
    ]
    calls: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        if sig == signal.SIGKILL:
            remaining[:] = [item for item in remaining if item[0] != pid]

    stopped = airflow_processes.terminate_processes(
        [
            (401, "airflow scheduler"),
            (402, "airflow triggerer"),
        ],
        remaining_processes_fn=lambda: list(remaining),
        grace_seconds=0,
        kill_fn=kill,
    )

    assert stopped == [401, 402]
    assert calls == [
        (401, signal.SIGTERM),
        (402, signal.SIGTERM),
        (401, signal.SIGKILL),
        (402, signal.SIGKILL),
    ]
    assert remaining == [(999, "unrelated process")]


def test_terminate_processes_fails_if_selected_pid_survives_sigkill() -> None:
    remaining = [(501, "airflow scheduler")]

    with pytest.raises(RuntimeError, match="501"):
        airflow_processes.terminate_processes(
            remaining,
            remaining_processes_fn=lambda: list(remaining),
            grace_seconds=0,
            kill_fn=lambda pid, sig: None,
        )


def test_terminate_processes_tolerates_disappearing_pid() -> None:
    calls: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        raise ProcessLookupError

    stopped = airflow_processes.terminate_processes(
        [(601, "airflow scheduler")],
        remaining_processes_fn=lambda: [],
        grace_seconds=0,
        kill_fn=kill,
    )

    assert stopped == [601]
    assert calls == [(601, signal.SIGTERM)]
