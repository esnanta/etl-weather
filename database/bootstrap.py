"""Initialize application schema paths independently, without Airflow.

The combined CLI never creates or drops databases; PostGIS is isolated to the
spatial initializer, and importers can check it independently.
"""
from __future__ import annotations

import logging
from sqlalchemy import text
from database.connection import create_database_engine
from sqlalchemy.engine import Engine
from database.config import APPLICATION_SCHEMA, DatabaseConfig, resolve_database_config

logger = logging.getLogger(__name__)

POSTGIS_SCHEMA = APPLICATION_SCHEMA


def ensure_postgis_extension(engine: Engine, *, expected_database: str) -> str:
    """Enable PostGIS if absent in the intended database and return its version."""
    with engine.begin() as connection:
        actual = connection.execute(text("SELECT current_database()")).scalar_one()
        if actual != expected_database:
            raise RuntimeError(
                f"Unexpected database: expected {expected_database!r}, got {actual!r}"
            )
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS climate_data"))
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
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA climate_data"))
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
    logger.info("PostGIS ready | database=%s | schema=climate_data | version=%s", expected_database, version)
    return str(version)


def bootstrap(config: DatabaseConfig | None = None) -> str:
    """Initialize climate and spatial objects in the existing configured database."""
    from database.schema import initialize_climate_schema, initialize_geospatial_schema

    resolved = resolve_database_config(config)
    engine = create_database_engine(resolved)
    try:
        initialize_climate_schema(engine, resolved)
        initialize_geospatial_schema(engine, resolved)
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
            ).scalar_one()
        return str(version)
    finally:
        engine.dispose()


def main() -> None:
    """Preview by default; explicitly initialize both independent schema paths."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Initialize climate and geospatial tables in the existing database."
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Create seven climate/operational and three spatial tables, including PostGIS.",
    )
    args = parser.parse_args()
    config = resolve_database_config()
    print(f"Database bootstrap target: database={config.database}, schema={config.schema}")
    print("Climate/operational tables: 7; geospatial tables: 3; regional views: 2 (PostGIS required).")
    if not args.execute:
        print("Dry run: database unchanged. Pass --execute to initialize both schema paths.")
        return

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    version = bootstrap(config)
    print(f"Climate and geospatial tables ready; PostGIS version {version}.")


if __name__ == "__main__":
    main()
