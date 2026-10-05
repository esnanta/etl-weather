"""Verify guarded deletion of generated Airflow log contents."""

from pathlib import Path

from scripts import reset_airflow_logs


def test_clear_airflow_logs_preserves_root_and_removes_children(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    nested = logs / "dag_id=x"
    nested.mkdir(parents=True)
    (nested / "task.log").write_text("log")
    (logs / "scheduler.log").write_text("scheduler")

    removed = reset_airflow_logs.clear_airflow_logs(logs)

    assert removed == 2
    assert logs.is_dir()
    assert list(logs.iterdir()) == []


def test_airflow_log_reset_prod_is_hard_denied(monkeypatch) -> None:
    monkeypatch.setattr(reset_airflow_logs, "resolve_app_environment", lambda: "prod")
    monkeypatch.setattr(
        reset_airflow_logs,
        "clear_airflow_logs",
        lambda: (_ for _ in ()).throw(AssertionError("must not delete")),
    )

    assert reset_airflow_logs.main(["--execute"]) == 3


def test_airflow_log_reset_inspect_never_deletes(monkeypatch) -> None:
    monkeypatch.setattr(reset_airflow_logs, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(
        reset_airflow_logs,
        "clear_airflow_logs",
        lambda: (_ for _ in ()).throw(AssertionError("inspect must not delete")),
    )

    assert reset_airflow_logs.main([]) == 0
