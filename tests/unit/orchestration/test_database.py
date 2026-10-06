"""Verify orchestration database lifecycle and bootstrap coordination."""

from __future__ import annotations

from collections.abc import Generator
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.repositories import database


@pytest.fixture
def dummy_engine() -> Generator[Engine, None, None]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def test_runtime_engine_disposes_without_database_creation(monkeypatch) -> None:
    disposed = []
    engine = SimpleNamespace(dispose=lambda: disposed.append(True))
    monkeypatch.setattr(
        database.DatabaseConfig,
        "from_env",
        lambda: SimpleNamespace(url="postgresql+psycopg://runtime"),
    )
    monkeypatch.setattr(
        "orchestration.repositories.database.create_engine",
        lambda url, future: engine,
    )

    with database.runtime_database_engine() as received:
        assert received is engine

    assert disposed == [True]


def test_progress_bootstrap_preserves_dependency_order(
    monkeypatch, dummy_engine: Engine
) -> None:
    calls = []
    for name in (
        "create_schema",
        "create_daily_climate_table",
        "create_daily_climate_detail_table",
        "create_pipeline_progress_tables",
    ):
        monkeypatch.setattr(
            database,
            name,
            lambda engine, config=None, name=name: calls.append(name),
        )

    database.initialize_progress_storage(dummy_engine)

    assert calls == [
        "create_schema",
        "create_daily_climate_table",
        "create_daily_climate_detail_table",
        "create_pipeline_progress_tables",
    ]


def test_full_bootstrap_adds_pacing_after_progress_storage(
    monkeypatch, dummy_engine: Engine
) -> None:
    calls = []
    monkeypatch.setattr(
        database,
        "initialize_progress_storage",
        lambda engine, config=None: calls.append("progress"),
    )
    monkeypatch.setattr(
        database,
        "initialize_pacing_storage",
        lambda engine: calls.append("pacing"),
    )

    database.initialize_application_storage(dummy_engine)

    assert calls == ["progress", "pacing"]
