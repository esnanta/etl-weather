"""Protect the guarded development PostgreSQL recreation command."""

from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from pipeline.database import DatabaseConfig
from scripts import recreate_postgres_database as maintenance
from ..._paths import REPOSITORY_ROOT


def _config(database: str = "weather_dev"):
    return SimpleNamespace(
        host="localhost",
        port=5432,
        database=database,
        user="weather",
        password="secret",
        schema="climate_data",
        url="postgresql+psycopg://weather:secret@localhost:5432/weather_dev",
    )


def _database_config(database: str = "weather_dev") -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database=database,
        user="weather",
        password="weather",
        schema="climate_data",
    )


def test_prod_denies_before_database_config_access(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.resolve_app_environment",
        lambda: "prod",
    )

    def unexpected_config():
        raise AssertionError("Database config must not be read in prod mode.")

    monkeypatch.setattr(
        maintenance.DatabaseConfig,
        "from_env",
        unexpected_config,
    )

    assert maintenance.main([]) == 3


def test_missing_app_env_aborts_before_database_config(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.resolve_app_environment",
        lambda: (_ for _ in ()).throw(
            ValueError(
                "APP_ENV is required. Set APP_ENV=dev or APP_ENV=prod "
                "before starting the application."
            )
        ),
    )

    def unexpected_config():
        raise AssertionError("Database config must not be read without APP_ENV.")

    monkeypatch.setattr(
        maintenance.DatabaseConfig,
        "from_env",
        unexpected_config,
    )

    assert maintenance.main([]) == 2


def test_dev_inspect_mode_runs_preflight_but_never_recreates(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        maintenance.DatabaseConfig, "from_env", lambda: _database_config()
    )
    monkeypatch.setattr(
        maintenance,
        "_validate_runtime_guards",
        lambda config: calls.append("preflight"),
    )
    monkeypatch.setattr(
        maintenance,
        "_recreate_database",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Inspect mode must never recreate the database.")
        ),
    )

    assert maintenance.main([]) == 0
    assert calls == ["preflight"]


@pytest.mark.parametrize("database", ["postgres", "template0", "template1"])
def test_system_databases_are_rejected(database: str) -> None:
    with pytest.raises(RuntimeError, match="system database"):
        maintenance._validate_static_guards(_database_config(database))


@pytest.mark.parametrize("answer", ["y", "Y", "yes", "YES"])
def test_confirmation_accepts_yes_variants(answer: str) -> None:
    assert (
        maintenance._confirmation(
            _database_config(),
            input_fn=lambda prompt: answer,
        )
        is True
    )


@pytest.mark.parametrize("answer", ["n", "N", "no", "NO"])
def test_confirmation_accepts_no_variants(answer: str) -> None:
    assert (
        maintenance._confirmation(
            _database_config(),
            input_fn=lambda prompt: answer,
        )
        is False
    )


def test_confirmation_reprompts_invalid_answer() -> None:
    answers = iter(["maybe", "y"])

    assert maintenance._confirmation(
        _database_config(),
        input_fn=lambda prompt: next(answers),
    ) is True


def test_execute_no_confirmation_never_recreates(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        maintenance.DatabaseConfig, "from_env", lambda: _database_config()
    )
    monkeypatch.setattr(maintenance, "_validate_runtime_guards", lambda config: None)
    monkeypatch.setattr(maintenance, "_confirmation", lambda config: False)
    monkeypatch.setattr(
        maintenance,
        "_recreate_database",
        lambda config: (_ for _ in ()).throw(
            AssertionError("No confirmation must never recreate the database.")
        ),
    )

    assert maintenance.main(["--execute"]) == 0


def test_execute_yes_recreates_only_after_preflight(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        maintenance.DatabaseConfig, "from_env", lambda: _database_config()
    )
    monkeypatch.setattr(
        maintenance,
        "_validate_runtime_guards",
        lambda config: calls.append("preflight"),
    )
    monkeypatch.setattr(
        maintenance,
        "_confirmation",
        lambda config: calls.append("confirm") or True,
    )
    monkeypatch.setattr(
        maintenance,
        "_recreate_database",
        lambda config: calls.append("recreate"),
    )

    assert maintenance.main(["--execute"]) == 0
    assert calls == ["preflight", "confirm", "recreate"]


def test_failed_preflight_blocks_prompt_and_recreate(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        maintenance.DatabaseConfig, "from_env", lambda: _database_config()
    )
    monkeypatch.setattr(
        maintenance,
        "_validate_runtime_guards",
        lambda config: (_ for _ in ()).throw(RuntimeError("blocked")),
    )
    monkeypatch.setattr(
        maintenance,
        "_confirmation",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Prompt must not be shown after failed preflight.")
        ),
    )
    monkeypatch.setattr(
        maintenance,
        "_recreate_database",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Failed preflight must block recreation.")
        ),
    )

    assert maintenance.main(["--execute"]) == 4



def test_runtime_guard_rejects_enabled_campaign_schedule(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.recreate_postgres_database.campaign_schedule",
        lambda: "0 */2 * * *",
    )

    with pytest.raises(RuntimeError, match="scheduling is enabled"):
        maintenance._validate_runtime_guards(_database_config())



def test_direct_script_execution_resolves_root_config_without_shadowing() -> None:
    project_root = REPOSITORY_ROOT
    env = os.environ.copy()
    env["APP_ENV"] = "prod"

    result = subprocess.run(
        [sys.executable, str(project_root / "scripts" / "recreate_postgres_database.py")],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 3
    assert "database recreation is disabled when APP_ENV=prod" in result.stderr
    assert "ImportError" not in result.stderr
