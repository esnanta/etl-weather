"""Verify guarded cleanup of generated Airflow runtime state.

The tests cover development-only reset behavior, preservation of tracked config,
and safe handling of residual LocalExecutor workers. Process-table interaction
is isolated with temporary /proc fixtures and injected signal functions.
"""

from pathlib import Path
import signal

from scripts import reset_airflow_runtime


def _write_process(proc_root: Path, pid: int, command: str) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(command.replace(" ", "\0").encode())


def test_clear_airflow_runtime_preserves_tracked_config_and_structure(
    tmp_path: Path,
) -> None:
    orchestration = tmp_path / "orchestration"
    orchestration.mkdir()
    config = orchestration / "airflow.cfg"
    config.write_text("[core]\n")
    dags = orchestration / "dags"
    dags.mkdir()
    (dags / "dag.py").write_text("dag")
    for name in reset_airflow_runtime.RUNTIME_FILENAMES:
        (orchestration / name).write_text("generated")
    (orchestration / "scheduler.pid").write_text("123")
    (orchestration / "triggerer.pid").write_text("456")

    removed = reset_airflow_runtime.clear_airflow_runtime_state(orchestration)

    assert len(removed) == len(reset_airflow_runtime.RUNTIME_FILENAMES) + 2
    assert config.read_text() == "[core]\n"
    assert (dags / "dag.py").read_text() == "dag"
    for name in reset_airflow_runtime.RUNTIME_FILENAMES:
        assert not (orchestration / name).exists()
    assert not (orchestration / "scheduler.pid").exists()
    assert not (orchestration / "triggerer.pid").exists()


def test_runtime_process_detection_separates_services_and_local_workers(
    tmp_path: Path,
) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 101, "airflow scheduler")
    _write_process(proc_root, 102, "airflow worker -- LocalExecutor: <idle>")

    assert reset_airflow_runtime.blocking_airflow_services(proc_root) == [
        (101, "airflow scheduler")
    ]
    assert reset_airflow_runtime.local_executor_workers(proc_root) == [
        (102, "airflow worker -- LocalExecutor: <idle>")
    ]


def test_local_executor_cleanup_refuses_while_main_service_is_running(
    tmp_path: Path,
) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 101, "airflow triggerer")
    _write_process(proc_root, 102, "airflow worker -- LocalExecutor: <idle>")

    calls: list[tuple[int, int]] = []
    try:
        reset_airflow_runtime.stop_local_executor_workers(
            proc_root=proc_root,
            grace_seconds=0,
            kill_fn=lambda pid, sig: calls.append((pid, sig)),
        )
    except RuntimeError as error:
        assert "main Airflow services" in str(error)
    else:
        raise AssertionError("Expected cleanup to refuse while triggerer is running.")

    assert calls == []


def test_local_executor_cleanup_terms_then_kills_remaining_worker(
    tmp_path: Path,
) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 201, "airflow worker -- LocalExecutor: <idle>")
    calls: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        if sig == signal.SIGKILL:
            process = proc_root / str(pid)
            (process / "cmdline").unlink()
            process.rmdir()

    stopped = reset_airflow_runtime.stop_local_executor_workers(
        proc_root=proc_root,
        grace_seconds=0,
        kill_fn=kill,
    )

    assert stopped == [201]
    assert calls == [(201, signal.SIGTERM), (201, signal.SIGKILL)]


def test_local_executor_cleanup_stops_after_sigterm_when_worker_exits(
    tmp_path: Path,
) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 301, "airflow worker -- LocalExecutor: <idle>")
    calls: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        if sig == signal.SIGTERM:
            process = proc_root / str(pid)
            (process / "cmdline").unlink()
            process.rmdir()

    stopped = reset_airflow_runtime.stop_local_executor_workers(
        proc_root=proc_root,
        grace_seconds=0,
        kill_fn=kill,
    )

    assert stopped == [301]
    assert calls == [(301, signal.SIGTERM)]


def test_airflow_runtime_reset_prod_is_hard_denied(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.resolve_app_environment",
        lambda: "prod",
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.reset_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(AssertionError("must not reset")),
    )

    assert reset_airflow_runtime.main(["--execute"]) == 3


def test_airflow_runtime_reset_inspect_never_mutates(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.blocking_airflow_services",
        lambda: [],
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.local_executor_workers",
        lambda: [(10, "airflow worker -- LocalExecutor: <idle>")],
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.reset_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(AssertionError("inspect must not reset")),
    )

    assert reset_airflow_runtime.main([]) == 0


def test_airflow_runtime_reset_no_confirmation_never_mutates(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.blocking_airflow_services",
        lambda: [],
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.local_executor_workers",
        lambda: [],
    )
    monkeypatch.setattr("scripts.reset_airflow_runtime._confirmation", lambda: False)
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.reset_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(AssertionError("no must not reset")),
    )

    assert reset_airflow_runtime.main(["--execute"]) == 0


def test_airflow_runtime_reset_blocks_when_main_service_is_running(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.blocking_airflow_services",
        lambda: [(77, "airflow scheduler")],
    )
    monkeypatch.setattr(
        "scripts.reset_airflow_runtime.local_executor_workers",
        lambda: [(88, "airflow worker -- LocalExecutor: <idle>")],
    )

    assert reset_airflow_runtime.main(["--execute"]) == 4
