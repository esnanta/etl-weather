"""Guardrails for explicit two-database development initialization."""
from __future__ import annotations

import os

import pytest

from database.config import DatabaseConfig, resolve_test_database_config
from database import setup


def _config(name: str) -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost", port=5432, database=name,
        user="app", password="redacted",
    )


def test_resolve_targets_requires_independent_config_families(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    for prefix, name in (("POSTGRES_", "weather"), ("TEST_POSTGRES_", "weather_test")):
        for key, value in (
            ("HOST", "localhost"), ("PORT", "5432"), ("DATABASE", name),
            ("USER", "app"), ("PASSWORD", "redacted"),
        ):
            monkeypatch.setenv(prefix + key, value)
    application, testing = setup.resolve_targets()
    assert (application.database, testing.database) == ("weather", "weather_test")


def test_resolve_targets_rejects_same_database(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    for prefix in ("POSTGRES_", "TEST_POSTGRES_"):
        for key, value in (
            ("HOST", "localhost"), ("PORT", "5432"),
            ("DATABASE", "weather_test"), ("USER", "app"), ("PASSWORD", "redacted"),
        ):
            monkeypatch.setenv(prefix + key, value)
    with pytest.raises(ValueError, match="distinct"):
        setup.resolve_targets()


def test_setup_preflights_both_targets_before_writes(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    calls = []

    def inspect(config):
        calls.append(config.database)
        if config.database == "weather_test":
            raise RuntimeError("PostGIS unavailable")

    monkeypatch.setattr(setup, "_inspect_target", inspect)
    monkeypatch.setattr(
        setup, "bootstrap",
        lambda config: pytest.fail("No initialization before both preflights pass"),
    )
    with pytest.raises(RuntimeError, match="PostGIS unavailable"):
        setup.initialize_development_databases(_config("weather"), _config("weather_test"))
    assert calls == ["weather", "weather_test"]


def test_missing_test_configuration_never_uses_application_settings(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    for key, value in (
        ("HOST", "localhost"), ("PORT", "5432"), ("DATABASE", "weather"),
        ("USER", "app"), ("PASSWORD", "redacted"),
    ):
        monkeypatch.setenv("POSTGRES_" + key, value)
        monkeypatch.delenv("TEST_POSTGRES_" + key, raising=False)
    # Supply a deliberately incomplete environment mapping to avoid loading .env.
    supplied = {
        key: value for key, value in os.environ.items()
        if not key.startswith("TEST_POSTGRES_")
    }
    with pytest.raises(ValueError, match="TEST_POSTGRES_"):
        resolve_test_database_config(supplied)


def test_direct_setup_rejects_production_environment(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setattr(
        setup, "_inspect_target",
        lambda config: pytest.fail("No connection outside development"),
    )
    with pytest.raises(ValueError, match="APP_ENV=dev"):
        setup.initialize_development_databases(_config("weather"), _config("weather_test"))


def test_direct_setup_rejects_same_target_before_preflight(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setattr(
        setup, "_inspect_target",
        lambda config: pytest.fail("No connection to an ambiguous target"),
    )
    with pytest.raises(ValueError, match="distinct"):
        setup.initialize_development_databases(_config("weather_test"), _config("weather_test"))


def test_direct_setup_rejects_unmarked_test_database(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setattr(
        setup, "_inspect_target",
        lambda config: pytest.fail("No connection to an unmarked test target"),
    )
    with pytest.raises(ValueError, match="test target"):
        setup.initialize_development_databases(_config("weather"), _config("archive"))


def test_bootstrap_initializes_both_paths_without_provisioning(monkeypatch) -> None:
    from database import bootstrap as bootstrap_module
    from database import schema

    calls = []

    class FakeEngine:
        def dispose(self):
            calls.append("dispose")

        def connect(self):
            from contextlib import contextmanager

            @contextmanager
            def context():
                class Connection:
                    def execute(self, statement):
                        class Result:
                            def scalar_one(self):
                                return "3.4.2"
                        return Result()
                yield Connection()

            return context()

    monkeypatch.setattr(bootstrap_module, "create_database_engine", lambda config: FakeEngine())
    monkeypatch.setattr(
        schema, "initialize_climate_schema",
        lambda engine, config: calls.append("climate"),
    )
    monkeypatch.setattr(
        schema, "initialize_geospatial_schema",
        lambda engine, config: calls.append("geospatial"),
    )
    assert bootstrap_module.bootstrap(_config("weather")) == "3.4.2"
    assert calls == ["climate", "geospatial", "dispose"]
