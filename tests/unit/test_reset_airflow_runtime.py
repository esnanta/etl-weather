"""Verify guarded deletion of generated local Airflow runtime state."""

from pathlib import Path

from scripts import reset_airflow_runtime


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


def test_airflow_runtime_reset_prod_is_hard_denied(monkeypatch) -> None:
    monkeypatch.setattr(reset_airflow_runtime, "resolve_app_environment", lambda: "prod")
    monkeypatch.setattr(
        reset_airflow_runtime,
        "clear_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(AssertionError("must not delete")),
    )

    assert reset_airflow_runtime.main(["--execute"]) == 3


def test_airflow_runtime_reset_inspect_never_deletes(monkeypatch) -> None:
    monkeypatch.setattr(reset_airflow_runtime, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(
        reset_airflow_runtime,
        "clear_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(AssertionError("inspect must not delete")),
    )

    assert reset_airflow_runtime.main([]) == 0


def test_airflow_runtime_reset_no_confirmation_never_deletes(monkeypatch) -> None:
    monkeypatch.setattr(reset_airflow_runtime, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(reset_airflow_runtime, "_confirmation", lambda: False)
    monkeypatch.setattr(
        reset_airflow_runtime,
        "clear_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(AssertionError("no must not delete")),
    )

    assert reset_airflow_runtime.main(["--execute"]) == 0
