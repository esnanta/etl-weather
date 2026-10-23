"""Verify production destroy-all guards, execution order, and operator logs.

These tests invoke the real destroy_all functions while isolating destructive OS,
PostgreSQL, and Airflow boundaries. They do not substitute for a live reset.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from database.config import DatabaseConfig
from scripts.maintenance import destroy_all
from ..._paths import REPOSITORY_ROOT


@pytest.fixture(autouse=True)
def _mock_destructive_postgis_preflight(monkeypatch):
    """Unit tests never open a live PostgreSQL maintenance connection."""
    monkeypatch.setattr(
        destroy_all.recreate_postgres_database,
        "_validate_bootstrap_prerequisites",
        lambda config: None,
    )


def _config(database: str = "weather") -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database=database,
        user="postgres",
        password="secret",
        schema="climate_data",
    )


def test_prod_denies_before_database_config_access(monkeypatch) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "prod")

    def unexpected_config():
        raise AssertionError("Database config must not be read in prod mode.")

    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", unexpected_config)

    assert destroy_all.main([]) == 3


def test_missing_app_env_aborts_before_database_config(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.resolve_app_environment",
        lambda: (_ for _ in ()).throw(ValueError("APP_ENV is required")),
    )

    def unexpected_config():
        raise AssertionError("Database config must not be read without APP_ENV.")

    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", unexpected_config)

    assert destroy_all.main([]) == 2


def test_inspect_only_displays_targets_without_mutation(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "dev")
    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", lambda: _config())
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._validate_static_guards",
        lambda config: calls.append("static"),
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.list_airflow_processes",
        lambda: [(101, "airflow scheduler")],
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.execute_destroy",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Inspect-only mode must not destroy anything.")
        ),
    )

    assert destroy_all.main([]) == 0
    assert calls == ["static"]


@pytest.mark.parametrize("answer", ["DESTROY"])
def test_confirmation_requires_exact_destroy(answer: str) -> None:
    assert destroy_all._confirmation(input_fn=lambda prompt: answer) is True


@pytest.mark.parametrize("answer", ["destroy", "Destroy", "YES", "y", "", " DESTROY "])
def test_confirmation_rejects_every_non_exact_value(answer: str) -> None:
    assert destroy_all._confirmation(input_fn=lambda prompt: answer) is False


def test_system_database_is_rejected_before_confirmation(monkeypatch) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "dev")
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.DatabaseConfig.from_env",
        lambda: _config("postgres"),
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._confirmation",
        lambda: (_ for _ in ()).throw(
            AssertionError("System database must fail before confirmation.")
        ),
    )

    assert destroy_all.main(["--execute"]) == 4


def test_stop_all_airflow_processes_uses_shared_termination(monkeypatch) -> None:
    current = [
        (101, "airflow scheduler"),
        (102, "airflow triggerer"),
        (103, "airflow worker -- LocalExecutor: <idle>"),
    ]
    selected: list[tuple[int, str]] = []

    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.list_airflow_processes",
        lambda: list(current),
    )

    def terminate(processes, *, remaining_processes_fn):
        selected.extend(processes)
        current.clear()
        assert remaining_processes_fn() == []
        return [101, 102, 103]

    monkeypatch.setattr("scripts.maintenance.destroy_all.terminate_processes", terminate)

    assert destroy_all._stop_all_airflow_processes() == 3
    assert selected == [
        (101, "airflow scheduler"),
        (102, "airflow triggerer"),
        (103, "airflow worker -- LocalExecutor: <idle>"),
    ]


def test_airflow_stop_failure_prevents_database_recreation(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._stop_all_airflow_processes",
        lambda: (_ for _ in ()).throw(RuntimeError("cannot stop airflow")),
    )
    monkeypatch.setattr(destroy_all, "_require_no_database_sessions", lambda config: None)
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._recreate_database",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Database must remain untouched.")
        ),
    )

    with pytest.raises(RuntimeError, match="cannot stop airflow"):
        destroy_all.execute_destroy(_config())


def test_external_database_session_prevents_recreation(monkeypatch) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all._stop_all_airflow_processes", lambda: 2)
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._database_sessions",
        lambda config: [
            {
                "pid": 701,
                "user": "postgres",
                "application_name": "DataGrip",
                "client_addr": "127.0.0.1",
                "state": "idle",
            }
        ],
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._recreate_database",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Database must not be recreated with external sessions.")
        ),
    )

    with pytest.raises(RuntimeError, match="pid=701"):
        destroy_all.execute_destroy(_config())


def test_execute_destroy_uses_failure_safe_order(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._stop_all_airflow_processes",
        lambda: calls.append("airflow") or 4,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._require_no_database_sessions",
        lambda config: calls.append("sessions"),
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._recreate_database",
        lambda config: calls.append("database"),
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.reset_data.clear_data_contents",
        lambda: calls.append("data") or 2,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.reset_airflow_logs.clear_airflow_logs",
        lambda: calls.append("logs") or 3,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.reset_airflow_runtime.clear_airflow_runtime_state",
        lambda: calls.append("runtime") or [Path("airflow.db")],
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._verify_clean_state",
        lambda config: calls.append("verify") or {
            "airflow_processes_remaining": 0,
            "airflow_runtime_files_remaining": 0,
            "data_entries_remaining": 0,
            "airflow_log_entries_remaining": 0,
            "database_ready": 1,
        },
    )

    result = destroy_all.execute_destroy(_config())

    assert calls == [
        "sessions",
        "airflow",
        "sessions",
        "database",
        "data",
        "logs",
        "runtime",
        "verify",
    ]
    assert result["stopped_airflow_processes"] == 4
    assert result["data_entries_removed"] == 2
    assert result["airflow_log_entries_removed"] == 3
    assert result["airflow_runtime_files_removed"] == 1


def test_final_verification_rejects_remaining_runtime_state(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.list_airflow_processes",
        lambda: [(801, "airflow scheduler")],
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._runtime_files_remaining",
        lambda: [],
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._directory_entry_count",
        lambda path: 0,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._database_tables_ready",
        lambda config: True,
    )

    with pytest.raises(RuntimeError, match="airflow_processes_remaining=1"):
        destroy_all._verify_clean_state(_config())


def test_final_verification_accepts_clean_state(monkeypatch) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all.list_airflow_processes", lambda: [])
    monkeypatch.setattr("scripts.maintenance.destroy_all._runtime_files_remaining", lambda: [])
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._directory_entry_count",
        lambda path: 0,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._database_tables_ready",
        lambda config: True,
    )

    assert destroy_all._verify_clean_state(_config()) == {
        "airflow_processes_remaining": 0,
        "airflow_runtime_files_remaining": 0,
        "data_entries_remaining": 0,
        "airflow_log_entries_remaining": 0,
        "database_ready": 1,
    }


def test_execute_main_requires_confirmation_before_destroy(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "dev")
    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", lambda: _config())
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._validate_static_guards",
        lambda config: None,
    )
    monkeypatch.setattr("scripts.maintenance.destroy_all.list_airflow_processes", lambda: [])
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._confirmation",
        lambda: calls.append("confirm") or False,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.execute_destroy",
        lambda config: (_ for _ in ()).throw(
            AssertionError("Destroy must not run without exact confirmation.")
        ),
    )

    assert destroy_all.main(["--execute"]) == 0
    assert calls == ["confirm"]


def test_execute_main_returns_failure_when_destroy_fails(monkeypatch) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "dev")
    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", lambda: _config())
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._validate_static_guards",
        lambda config: None,
    )
    monkeypatch.setattr("scripts.maintenance.destroy_all.list_airflow_processes", lambda: [])
    monkeypatch.setattr("scripts.maintenance.destroy_all._confirmation", lambda: True)
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.execute_destroy",
        lambda config: (_ for _ in ()).throw(RuntimeError("verification failed")),
    )

    assert destroy_all.main(["--execute"]) == 5


def test_direct_script_execution_hard_denies_prod() -> None:
    env = os.environ.copy()
    env["APP_ENV"] = "prod"

    result = subprocess.run(
        [sys.executable, str(REPOSITORY_ROOT / "scripts" / "maintenance" / "destroy_all.py")],
        cwd=REPOSITORY_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 3
    assert "Destroy-all unavailable | APP_ENV=prod" in result.stderr
    assert "ImportError" not in result.stderr


def test_open_database_sessions_log_actionable_failure(monkeypatch, caplog, capsys) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "dev")
    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", lambda: _config())
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._validate_static_guards",
        lambda config: None,
    )
    monkeypatch.setattr("scripts.maintenance.destroy_all.list_airflow_processes", lambda: [])
    monkeypatch.setattr("scripts.maintenance.destroy_all._confirmation", lambda: True)
    monkeypatch.setattr("scripts.maintenance.destroy_all._stop_all_airflow_processes", lambda: 0)
    monkeypatch.setattr("scripts.maintenance.destroy_all._database_sessions", lambda config: [
        {"pid": 15381, "user": "postgres", "application_name": "PyCharm",
         "client_addr": "127.0.0.1", "state": "idle"}
    ])
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._recreate_database",
        lambda config: pytest.fail("Database recreation must not start."),
    )

    with caplog.at_level("INFO"):
        assert destroy_all.main(["--execute"]) == 5
    stderr = capsys.readouterr().err
    assert "PREFLIGHT | Check PostgreSQL privileges, PostGIS and sessions" in stderr
    assert "DESTROY ALL | FAILED" in stderr
    assert "INFO | \n" not in stderr
    assert "pid=15381" in caplog.text
    assert "PyCharm" in caplog.text
    assert "disconnect the listed database sessions" in caplog.text
    assert "file cleanup have NOT started" in caplog.text


def test_success_logs_phases_and_final_state(monkeypatch, caplog, capsys) -> None:
    monkeypatch.setattr("scripts.maintenance.destroy_all.resolve_app_environment", lambda: "dev")
    monkeypatch.setattr("scripts.maintenance.destroy_all.DatabaseConfig.from_env", lambda: _config())
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._validate_static_guards",
        lambda config: None,
    )
    monkeypatch.setattr("scripts.maintenance.destroy_all.list_airflow_processes", lambda: [])
    monkeypatch.setattr("scripts.maintenance.destroy_all._confirmation", lambda: True)
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._stop_all_airflow_processes", lambda: 0,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._require_no_database_sessions", lambda config: None,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.recreate_postgres_database._recreate_database",
        lambda config: None,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.reset_data.clear_data_contents", lambda: 0,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.reset_airflow_logs.clear_airflow_logs", lambda: 0,
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all.reset_airflow_runtime.clear_airflow_runtime_state",
        lambda: [],
    )
    monkeypatch.setattr(
        "scripts.maintenance.destroy_all._verify_clean_state", lambda config: {
            "airflow_processes_remaining": 0,
            "airflow_runtime_files_remaining": 0,
            "data_entries_remaining": 0,
            "airflow_log_entries_remaining": 0,
            "database_ready": 1,
        },
    )
    with caplog.at_level("INFO"):
        assert destroy_all.main(["--execute"]) == 0
    stderr = capsys.readouterr().err
    for step in range(1, 7):
        assert f"STEP {step}/6" in stderr
    assert "DESTROY ALL | SUCCESS" in stderr
    assert "DROP, CREATE and bootstrap database (8 tables + PostGIS)" in stderr
    assert "Remove generated NetCDF/Parquet data" in stderr
    assert "Remove Airflow logs" in stderr
    assert "Verify database and clean runtime state" in stderr
    assert "DESTROY | Remove Airflow runtime state" in caplog.text
    assert "INFO | \n" not in stderr
    assert "database_ready=1" in caplog.text


def test_destroy_contract_requires_all_eight_canonical_tables() -> None:
    from database.verification import CLIMATE_TABLES, GEOSPATIAL_TABLES

    assert set(destroy_all.EXPECTED_APPLICATION_TABLES) == (
        CLIMATE_TABLES | GEOSPATIAL_TABLES
    )
    assert len(destroy_all.EXPECTED_APPLICATION_TABLES) == 8


def test_session_preflight_blocks_airflow_termination(monkeypatch) -> None:
    """A blocked PostgreSQL preflight must not stop even one Airflow process."""
    monkeypatch.setattr(
        destroy_all, "_require_no_database_sessions",
        lambda config: (_ for _ in ()).throw(RuntimeError("sessions blocked")),
    )
    monkeypatch.setattr(
        destroy_all, "_stop_all_airflow_processes",
        lambda: pytest.fail("Airflow termination must not begin"),
    )
    with pytest.raises(RuntimeError, match="sessions blocked"):
        destroy_all.execute_destroy(_config())


def test_postgis_preflight_blocks_airflow_termination(monkeypatch) -> None:
    monkeypatch.setattr(
        destroy_all.recreate_postgres_database,
        "_validate_bootstrap_prerequisites",
        lambda config: (_ for _ in ()).throw(RuntimeError("PostGIS unavailable")),
    )
    monkeypatch.setattr(
        destroy_all, "_stop_all_airflow_processes",
        lambda: pytest.fail("Airflow termination must not begin"),
    )
    with pytest.raises(RuntimeError, match="PostGIS unavailable"):
        destroy_all.execute_destroy(_config())
