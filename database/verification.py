"""Read-only database schema checks; never bootstrap or mutate targets."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import (
    APPLICATION_SCHEMA, DAILY_CLIMATE_TABLE, DAILY_CLIMATE_DETAIL_TABLE,
    PIPELINE_PROGRESS_TABLE, PIPELINE_PROGRESS_DETAIL_TABLE, PACING_TABLE,
    PROVINCE_TABLE, DISTRICT_TABLE, GRID_REGION_TABLE,
)

CLIMATE_TABLES = frozenset({
    DAILY_CLIMATE_TABLE, DAILY_CLIMATE_DETAIL_TABLE,
    PIPELINE_PROGRESS_TABLE, PIPELINE_PROGRESS_DETAIL_TABLE, PACING_TABLE,
})
GEOSPATIAL_TABLES = frozenset({PROVINCE_TABLE, DISTRICT_TABLE, GRID_REGION_TABLE})


def inspect_database(engine: Engine) -> dict[str, object]:
    """Return database, PostGIS, and table identities without any DDL."""
    with engine.connect() as connection:
        name = connection.execute(text("SELECT current_database()")).scalar_one()
        schema_exists = bool(connection.execute(
            text("SELECT 1 FROM pg_namespace WHERE nspname = :schema"),
            {"schema": APPLICATION_SCHEMA},
        ).scalar_one_or_none())
        postgis_schema = connection.execute(text(
            "SELECT n.nspname FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid=e.extnamespace "
            "WHERE e.extname = 'postgis'"
        )).scalar_one_or_none()
        tables = set(connection.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_type = 'BASE TABLE'"
        ), {"schema": APPLICATION_SCHEMA}).scalars())
    return {
        "database": name,
        "schema_exists": schema_exists,
        "postgis_schema": postgis_schema,
        "climate_tables_missing": tuple(sorted(CLIMATE_TABLES - tables)),
        "geospatial_tables_missing": tuple(sorted(GEOSPATIAL_TABLES - tables)),
    }


def require_schema_ready(engine: Engine, *, geospatial: bool = False) -> None:
    """Fail with a database-qualified message when required objects are missing."""
    result = inspect_database(engine)
    missing = result["geospatial_tables_missing"] if geospatial else result["climate_tables_missing"]
    if missing:
        raise RuntimeError(
            f"Database {result['database']!r}: missing {'geospatial' if geospatial else 'climate'} "
            f"tables in {APPLICATION_SCHEMA}: {', '.join(missing)}"
        )
    if geospatial and result["postgis_schema"] != APPLICATION_SCHEMA:
        raise RuntimeError(
            f"Database {result['database']!r}: PostGIS must be installed in {APPLICATION_SCHEMA}, "
            f"found {result['postgis_schema']!r}."
        )
