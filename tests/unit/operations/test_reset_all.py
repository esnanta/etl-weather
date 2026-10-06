"""Verify reset_all composes only the tested maintenance functions."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ..._paths import REPOSITORY_ROOT

from scripts import reset_all


def _config():
    return SimpleNamespace(
        host="localhost",
        port=5432,
        database="weather",
        user="postgres",
        password="secret",
        schema="climate_data",
    )


def test_prod_denies_before_database_config_access(monkeypatch) -> None:
    monkeypatch.setattr(reset_all, "resolve_app_environment", lambda: "prod")

    def unexpected_config():
        raise AssertionError("Database config must not be read in prod mode.")

    monkeypatch.setattr(reset_all.DatabaseConfig, "from_env", unexpected_config)

    assert reset_all.main([]) == 3


def test_missing_app_env_aborts_before_database_config(monkeypatch) -> None:
    monkeypatch.setattr(
        reset_all,
        "resolve_app_environment",
        lambda: (_ for _ in ()).throw(
            ValueError(
                "APP_ENV is required. Set APP_ENV=dev or APP_ENV=prod "
                "before starting the application."
            )
        ),
    )

    def unexpected_config():
        raise AssertionError("Database config must not be read without APP_ENV.")

    monkeypatch.setattr(reset_all.DatabaseConfig, "from_env", unexpected_config)

    assert reset_all.main([]) == 2


def test_inspect_mode_runs_preflight_without_reset(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(reset_all, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(reset_all.DatabaseConfig, "from_env", lambda: _config())
    monkeypatch.setattr(
        reset_all,
        "_run_preflight",
        lambda config: calls.append("preflight"),
    )
    monkeypatch.setattr(
        reset_all,
        "execute_reset",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Inspect mode must not execute reset.")
        ),
    )

    assert reset_all.main([]) == 0
    assert calls == ["preflight"]


def test_failed_preflight_blocks_confirmation_and_reset(monkeypatch) -> None:
    monkeypatch.setattr(reset_all, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(reset_all.DatabaseConfig, "from_env", lambda: _config())
    monkeypatch.setattr(
        reset_all,
        "_run_preflight",
        lambda config: (_ for _ in ()).throw(RuntimeError("blocked")),
    )
    monkeypatch.setattr(
        reset_all,
        "_confirmation",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Prompt must not appear after failed preflight.")
        ),
    )
    monkeypatch.setattr(
        reset_all,
        "execute_reset",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Failed preflight must block reset.")
        ),
    )

    assert reset_all.main(["--execute"]) == 4


def test_no_confirmation_blocks_reset(monkeypatch) -> None:
    monkeypatch.setattr(reset_all, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(reset_all.DatabaseConfig, "from_env", lambda: _config())
    monkeypatch.setattr(reset_all, "_run_preflight", lambda config: None)
    monkeypatch.setattr(reset_all, "_confirmation", lambda config: False)
    monkeypatch.setattr(
        reset_all,
        "execute_reset",
        lambda config: (_ for _ in ()).throw(
            AssertionError("No confirmation must not execute reset.")
        ),
    )

    assert reset_all.main(["--execute"]) == 0


@pytest.mark.parametrize("answer", ["y", "Y", "yes", "YES"])
def test_confirmation_accepts_yes_variants(answer: str) -> None:
    assert reset_all._confirmation(_config(), input_fn=lambda prompt: answer) is True


@pytest.mark.parametrize("answer", ["n", "N", "no", "NO"])
def test_confirmation_accepts_no_variants(answer: str) -> None:
    assert reset_all._confirmation(_config(), input_fn=lambda prompt: answer) is False


def test_confirmation_reprompts_invalid_answer() -> None:
    answers = iter(["maybe", "yes"])

    assert reset_all._confirmation(
        _config(),
        input_fn=lambda prompt: next(answers),
    ) is True


def test_execute_reset_uses_verified_components_in_safe_order(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        reset_all.recreate_postgres_database,
        "_recreate_database",
        lambda config: calls.append("database"),
    )
    monkeypatch.setattr(
        reset_all.reset_data,
        "clear_data_contents",
        lambda: calls.append("data") or 2,
    )
    monkeypatch.setattr(
        reset_all.reset_airflow_logs,
        "clear_airflow_logs",
        lambda: calls.append("logs") or 3,
    )
    monkeypatch.setattr(
        reset_all.reset_airflow_runtime,
        "clear_airflow_runtime_state",
        lambda: calls.append("runtime") or [Path("a"), Path("b")],
    )

    result = reset_all.execute_reset(_config())

    assert calls == ["database", "data", "logs", "runtime"]
    assert result == {
        "data_entries_removed": 2,
        "airflow_log_entries_removed": 3,
        "airflow_runtime_files_removed": 2,
    }


def test_database_failure_prevents_local_cleanup(monkeypatch) -> None:
    monkeypatch.setattr(
        reset_all.recreate_postgres_database,
        "_recreate_database",
        lambda config: (_ for _ in ()).throw(RuntimeError("database failed")),
    )
    monkeypatch.setattr(
        reset_all.reset_data,
        "clear_data_contents",
        lambda: (_ for _ in ()).throw(
            AssertionError("Data must remain after database failure.")
        ),
    )
    monkeypatch.setattr(
        reset_all.reset_airflow_logs,
        "clear_airflow_logs",
        lambda: (_ for _ in ()).throw(
            AssertionError("Logs must remain after database failure.")
        ),
    )
    monkeypatch.setattr(
        reset_all.reset_airflow_runtime,
        "clear_airflow_runtime_state",
        lambda: (_ for _ in ()).throw(
            AssertionError("Runtime state must remain after database failure.")
        ),
    )

    with pytest.raises(RuntimeError, match="database failed"):
        reset_all.execute_reset(_config())


def test_execute_yes_runs_preflight_then_single_reset(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(reset_all, "resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(reset_all.DatabaseConfig, "from_env", lambda: _config())
    monkeypatch.setattr(
        reset_all,
        "_run_preflight",
        lambda config: calls.append("preflight"),
    )
    monkeypatch.setattr(
        reset_all,
        "_confirmation",
        lambda config: calls.append("confirm") or True,
    )
    monkeypatch.setattr(
        reset_all,
        "execute_reset",
        lambda config: calls.append("reset") or {
            "data_entries_removed": 0,
            "airflow_log_entries_removed": 0,
            "airflow_runtime_files_removed": 0,
        },
    )

    assert reset_all.main(["--execute"]) == 0
    assert calls == ["preflight", "confirm", "reset"]


def test_direct_script_execution_hard_denies_prod_without_import_shadowing() -> None:
    project_root = REPOSITORY_ROOT
    env = os.environ.copy()
    env["APP_ENV"] = "prod"

    result = subprocess.run(
        [sys.executable, str(project_root / "scripts" / "reset_all.py")],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 3
    assert "Reset-all unavailable | APP_ENV=prod" in result.stderr
    assert "ImportError" not in result.stderr
