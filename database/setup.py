"""Guarded development setup for separate application and test PostgreSQL databases.

The default command only inspects targets. --execute is explicit, limited to
APP_ENV=dev, and never drops databases, copies data, or touches Airflow SQLite.
"""
from __future__ import annotations

import argparse
import logging

from psycopg import connect, sql
from sqlalchemy import text

from config import APP_ENV_DEV, resolve_app_environment
from database.bootstrap import bootstrap
from database.config import DatabaseConfig, resolve_test_database_config
from database.connection import managed_database_engine
from database.verification import require_schema_ready

logger = logging.getLogger(__name__)
SYSTEM_DATABASES = frozenset({"postgres", "template0", "template1"})


def resolve_targets() -> tuple[DatabaseConfig, DatabaseConfig]:
    """Require both independent env families before any setup operation."""
    application = DatabaseConfig.from_env(prefix="POSTGRES_")
    testing = resolve_test_database_config()
    if resolve_app_environment() != APP_ENV_DEV:
        raise ValueError("Database setup is available only with APP_ENV=dev.")
    for target in (application, testing):
        if target.database.lower() in SYSTEM_DATABASES:
            raise ValueError("Refusing PostgreSQL maintenance database as a setup target.")
        if target.schema != "climate_data":
            raise ValueError("Setup requires the canonical climate_data schema.")
    if (
        application.host.lower(), application.port, application.database.lower()
    ) == (
        testing.host.lower(), testing.port, testing.database.lower()
    ):
        raise ValueError("Application and test databases must be distinct targets.")
    return application, testing


def _maintenance_connect(config: DatabaseConfig):
    return connect(
        host=config.host,
        port=config.port,
        dbname="postgres",
        user=config.user,
        password=config.password,
        connect_timeout=5,
        autocommit=True,
    )


def _inspect_target(config: DatabaseConfig) -> bool:
    """Validate maintenance access, server PostGIS package and existence, read-only."""
    with _maintenance_connect(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'postgis'"
            )
            if cursor.fetchone() is None:
                raise RuntimeError(
                    f"PostGIS server package unavailable for {config.database!r}."
                )
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (config.database,),
            )
            exists = cursor.fetchone() is not None
    if exists:
        with managed_database_engine(config) as engine:
            with engine.connect() as connection:
                actual = connection.execute(text("SELECT current_database()")).scalar_one()
                if actual != config.database:
                    raise RuntimeError("Connected database identity mismatch.")
                extension_schema = connection.execute(text(
                    "SELECT n.nspname FROM pg_extension e "
                    "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    "WHERE e.extname = 'postgis'"
                )).scalar_one_or_none()
                if extension_schema not in (None, "climate_data"):
                    raise RuntimeError(
                        f"PostGIS in {config.database!r} is installed in "
                        f"{extension_schema!r}, expected 'climate_data'; "
                        "refusing automatic relocation."
                    )
    return exists


def initialize_development_databases(
    application: DatabaseConfig, testing: DatabaseConfig
) -> None:
    """Create only missing target databases, then initialize canonical schemas.

    Both targets are checked before the first write. Database creation and DDL
    are not cross-database atomic: failures require operator inspection.
    """
    targets = (application, testing)
    exists = [_inspect_target(target) for target in targets]
    for target, present in zip(targets, exists):
        if not present:
            logger.info("Creating missing PostgreSQL database %s", target.database)
            with _maintenance_connect(target) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(target.database)
                        )
                    )
        logger.info("Initializing database %s", target.database)
        bootstrap(target)
        with managed_database_engine(target) as engine:
            require_schema_ready(engine)
            require_schema_ready(engine, geospatial=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect or prepare separate development and test PostgreSQL databases."
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Explicitly create missing databases and initialize all eight tables in each.",
    )
    args = parser.parse_args()
    application, testing = resolve_targets()
    for label, target in (("application", application), ("test", testing)):
        print(
            f"{label}: host={target.host}, port={target.port}, "
            f"database={target.database}, schema={target.schema}"
        )
    if not args.execute:
        print("Dry run: no database connections or changes. Pass --execute to initialize.")
        return
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    initialize_development_databases(application, testing)
    print("Application and isolated test database schemas verified.")


if __name__ == "__main__":
    main()
