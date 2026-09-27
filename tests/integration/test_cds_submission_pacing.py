"""Verify that PostgreSQL serializes CDS submission reservations across workers.

The same database clock must survive separate connections, concurrent claims,
and task process termination without admitting early requests.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from orchestration.repositories import cds_pacing
from pipeline.database import DatabaseConfig

pytestmark = pytest.mark.integration


def test_reservations_are_durable_and_atomic(monkeypatch: pytest.MonkeyPatch) -> None:
    load_dotenv()
    names = (
        "TEST_POSTGRES_HOST",
        "TEST_POSTGRES_PORT",
        "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER",
        "TEST_POSTGRES_PASSWORD",
    )
    if any(not os.getenv(name) for name in names):
        pytest.skip("Isolated TEST_POSTGRES_* connection is required.")
    if "test" not in os.environ["TEST_POSTGRES_DATABASE"].lower():
        pytest.fail("TEST_POSTGRES_DATABASE must name an isolated test database.")

    config = DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=os.environ["TEST_POSTGRES_DATABASE"],
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema="climate_data",
    )
    engine = create_engine(config.url, future=True)
    key = f"pacing-test-{uuid4().hex}"
    monkeypatch.setattr(cds_pacing, "PACING_KEY", key)
    table_ready = False
    try:
        cds_pacing.create_cds_pacing_table(engine)
        table_ready = True
        with ThreadPoolExecutor(max_workers=2) as executor:
            decisions = list(
                executor.map(
                    lambda _: cds_pacing.reserve_cds_submission(
                        engine, interval_seconds=1800
                    ),
                    range(2),
                )
            )
        assert [decision.granted for decision in decisions].count(True) == 1
        assert [decision.granted for decision in decisions].count(False) == 1
        blocked = next(decision for decision in decisions if not decision.granted)
        assert blocked.next_allowed_at > blocked.checked_at

        after_restart = cds_pacing.reserve_cds_submission(
            engine, interval_seconds=1800
        )
        assert not after_restart.granted
        assert after_restart.next_allowed_at == blocked.next_allowed_at

        immediate = cds_pacing.reserve_cds_submission(engine, interval_seconds=0)
        assert immediate.granted
        assert immediate.next_allowed_at == immediate.checked_at
    finally:
        if table_ready:
            with engine.begin() as connection:
                connection.execute(
                    text(
                        f"DELETE FROM {cds_pacing.PACING_SCHEMA}.{cds_pacing.PACING_TABLE} "
                        "WHERE pacing_key = :key"
                    ),
                    {"key": key},
                )
        engine.dispose()
