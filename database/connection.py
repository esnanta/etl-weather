"""Connections and read-only database existence checks.

Database creation belongs to explicit, guarded maintenance/bootstrap commands.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from database.config import DatabaseConfig


def create_database_engine(config: DatabaseConfig) -> Engine:
    """Create an engine without checking or creating the target database."""
    return create_engine(config.sqlalchemy_url, future=True)


@contextmanager
def managed_database_engine(config: DatabaseConfig) -> Iterator[Engine]:
    engine = create_database_engine(config)
    try:
        yield engine
    finally:
        engine.dispose()


def database_exists(config: DatabaseConfig) -> bool:
    """Check existence through the maintenance DB; do not mutate server state."""
    maintenance_engine = create_engine(config.maintenance_url, future=True)
    try:
        with maintenance_engine.connect() as connection:
            return bool(
                connection.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :database"),
                    {"database": config.database},
                ).scalar_one_or_none()
            )
    finally:
        maintenance_engine.dispose()
