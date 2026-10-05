"""Own orchestration database engine lifecycle and storage bootstrap coordination.

Normal orchestration runtime connects only to the configured project database.
Database creation or recreation remains an explicit maintenance operation outside
these helpers.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.repositories.submission_pacing import create_submission_pacing_table
from orchestration.repositories.pipeline_progress import create_pipeline_progress_tables
from pipeline.database import (
    DatabaseConfig,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)


@contextmanager
def runtime_database_engine() -> Iterator[Engine]:
    """Yield one runtime engine without creating a missing database."""
    engine = create_engine(DatabaseConfig.from_env().url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def initialize_progress_storage(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Ensure weather and progress tables required during job preparation exist."""
    create_schema(engine, config)
    create_daily_climate_table(engine, config)
    create_daily_climate_detail_table(engine, config)
    create_pipeline_progress_tables(engine)


def initialize_pacing_storage(engine: Engine) -> None:
    """Ensure the durable submission clock exists at submission-gate time."""
    create_submission_pacing_table(engine)


def initialize_application_storage(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Idempotently bootstrap all five application tables in dependency order."""
    initialize_progress_storage(engine, config)
    initialize_pacing_storage(engine)
