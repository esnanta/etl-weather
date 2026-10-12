"""Initialize spatial database capabilities without running Airflow.

This module does not create, drop, or recreate databases or application tables.
GeoJSON importers must call ensure_postgis_extension before their writes.
"""
from __future__ import annotations

import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pipeline.database.config import DatabaseConfig, resolve_database_config

logger = logging.getLogger(__name__)


def ensure_postgis_extension(engine: Engine, *, expected_database: str) -> str:
    """Enable PostGIS if absent in the intended database and return its version."""
    with engine.begin() as connection:
        actual = connection.execute(text("SELECT current_database()")).scalar_one()
        if actual != expected_database:
            raise RuntimeError(
                f"Unexpected database: expected {expected_database!r}, got {actual!r}"
            )
        version = connection.execute(
            text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
        ).scalar_one_or_none()
        if version is None:
            available = connection.execute(
                text("SELECT default_version FROM pg_available_extensions WHERE name = 'postgis'")
            ).scalar_one_or_none()
            if available is None:
                raise RuntimeError("PostGIS server package is not available.")
            try:
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            except Exception as error:
                raise RuntimeError(
                    "PostGIS activation failed; check CREATE EXTENSION privileges."
                ) from error
            version = connection.execute(
                text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
            ).scalar_one_or_none()
        if version is None:
            raise RuntimeError("PostGIS extension verification failed.")
    logger.info("PostGIS ready | database=%s | version=%s", expected_database, version)
    return str(version)


def bootstrap(config: DatabaseConfig | None = None) -> str:
    """Bootstrap the existing application database without creating it."""
    resolved = resolve_database_config(config)
    engine = create_engine(resolved.url, future=True)
    try:
        return ensure_postgis_extension(engine, expected_database=resolved.database)
    finally:
        engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    version = bootstrap()
    print(f"PostGIS ready (version {version}).")


if __name__ == "__main__":
    main()
