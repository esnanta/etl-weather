"""Verify fresh and repeated orchestration storage bootstrap on isolated PostgreSQL."""

from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from pipeline.database.config import (
    PACING_SCHEMA,
    PACING_TABLE,
    PIPELINE_PROGRESS_DETAIL_TABLE,
    PIPELINE_PROGRESS_SCHEMA,
    PIPELINE_PROGRESS_TABLE,
)
from orchestration.repositories.database import initialize_application_storage
from pipeline.database import (
    DAILY_CLIMATE_DETAIL_TABLE,
    DAILY_CLIMATE_TABLE,
    DatabaseConfig,
    SCHEMA_NAME,
)

pytestmark = pytest.mark.integration


def _test_database_config() -> DatabaseConfig:
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
    return DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=os.environ["TEST_POSTGRES_DATABASE"],
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema=SCHEMA_NAME,
    )


def test_fresh_and_repeated_application_bootstrap() -> None:
    config = _test_database_config()
    engine = create_engine(config.url, future=True)
    expected_tables = (
        f"{SCHEMA_NAME}.{DAILY_CLIMATE_TABLE}",
        f"{SCHEMA_NAME}.{DAILY_CLIMATE_DETAIL_TABLE}",
        f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE}",
        f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}",
        f"{PACING_SCHEMA}.{PACING_TABLE}",
    )
    try:
        with engine.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE"))

        initialize_application_storage(engine, config)
        initialize_application_storage(engine, config)

        with engine.connect() as connection:
            for table in expected_tables:
                assert connection.execute(
                    text("SELECT to_regclass(:table_name)"),
                    {"table_name": table},
                ).scalar_one() == table
    finally:
        engine.dispose()
