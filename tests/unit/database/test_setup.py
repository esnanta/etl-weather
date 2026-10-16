"""Guardrails for explicit two-database development initialization."""
from __future__ import annotations

import pytest

from database.config import DatabaseConfig
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
    with pytest.raises(ValueError, match="TEST_POSTGRES_"):
        setup.resolve_targets()
