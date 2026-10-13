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

POSTGIS_SCHEMA = "public"


def ensure_postgis_extension(engine: Engine, *, expected_database: str) -> str:
    """Enable PostGIS if absent in the intended database and return its version."""
    with engine.begin() as connection:
        actual = connection.execute(text("SELECT current_database()")).scalar_one()
        if actual != expected_database:
            raise RuntimeError(
                f"Unexpected database: expected {expected_database!r}, got {actual!r}"
            )
        row = connection.execute(text(
            "SELECT e.extversion, n.nspname "
            "FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace "
            "WHERE e.extname='postgis'"
        )).one_or_none()
        version = row[0] if row else None
        if row and row[1] != POSTGIS_SCHEMA:
            raise RuntimeError(
                f"PostGIS must be in {POSTGIS_SCHEMA!r}, found {row[1]!r}; "
                "refusing automatic relocation of an existing extension."
            )
        if version is None:
            available = connection.execute(
                text("SELECT default_version FROM pg_available_extensions WHERE name = 'postgis'")
            ).scalar_one_or_none()
            if available is None:
                raise RuntimeError("PostGIS server package is not available.")
            try:
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public"))
            except Exception as error:
                raise RuntimeError(
                    "PostGIS activation failed; check CREATE EXTENSION privileges."
                ) from error
            version = connection.execute(
                text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
            ).scalar_one_or_none()
        schema = connection.execute(text(
            "SELECT n.nspname FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid=e.extnamespace "
            "WHERE e.extname='postgis'"
        )).scalar_one_or_none()
        if version is None or schema != POSTGIS_SCHEMA:
            raise RuntimeError("PostGIS extension/schema verification failed.")
    logger.info("PostGIS ready | database=%s | schema=public | version=%s", expected_database, version)
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
