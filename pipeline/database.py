from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Mapping

from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from pipeline.logging import log_metric

logger = logging.getLogger(__name__)

SCHEMA_NAME = "climate_data"
DATASET_TABLE = "weather_dataset"
DATASET_TABLE_DETAIL = "weather_dataset_detail"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = SCHEMA_NAME
    dataset_table: str = DATASET_TABLE
    dataset_table_detail: str = DATASET_TABLE_DETAIL

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "DatabaseConfig":
        load_dotenv()
        env_vars = env or os.environ

        required_keys = (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DATABASE",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
        )
        missing_keys = [key for key in required_keys if not env_vars.get(key)]
        if missing_keys:
            missing = ", ".join(missing_keys)
            raise ValueError(
                f"Missing required database environment variables: {missing}"
            )

        return cls(
            host=env_vars["POSTGRES_HOST"],
            port=int(env_vars["POSTGRES_PORT"]),
            database=env_vars["POSTGRES_DATABASE"],
            user=env_vars["POSTGRES_USER"],
            password=env_vars["POSTGRES_PASSWORD"],
        )

    @property
    def url(self) -> str:
        return (
            "postgresql+psycopg://"
            f"{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    @property
    def url_maintenance(self) -> str:
        return (
            "postgresql+psycopg://"
            f"{self.user}:{self.password}@{self.host}:{self.port}/postgres"
        )


def resolve_database_config(
    config: DatabaseConfig | None = None,
) -> DatabaseConfig:
    return config or DatabaseConfig.from_env()


def ensure_database_exists(config: DatabaseConfig | None = None) -> None:
    resolved_config = resolve_database_config(config)

    try:
        engine = create_engine(resolved_config.url)
        conn = engine.connect()
        conn.close()
        logger.info("Database '%s' already exists.", resolved_config.database)
        return
    except OperationalError as error:
        if f'database "{resolved_config.database}" does not exist' not in str(error):
            raise
        logger.info(
            "Database '%s' does not exist. Creating...",
            resolved_config.database,
        )

    maintenance_engine = create_engine(resolved_config.url_maintenance)
    with maintenance_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE DATABASE "{resolved_config.database}"'))

    logger.info("Database '%s' created.", resolved_config.database)


def create_database_engine(
    config: DatabaseConfig | None = None,
) -> Engine:
    resolved_config = resolve_database_config(config)
    ensure_database_exists(resolved_config)
    logger.info(
        "Creating PostgreSQL engine for database '%s'.",
        resolved_config.database,
    )
    return create_engine(resolved_config.url, future=True)


def create_schema(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    resolved_config = resolve_database_config(config)

    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE SCHEMA IF NOT EXISTS {resolved_config.schema}"
            )
        )

    log_metric(
        "Schema status",
        {
            "schema": resolved_config.schema,
            "status": "ready",
        },
    )


def create_dataset_table(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    resolved_config = resolve_database_config(config)

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                DO $$ BEGIN
                    CREATE TYPE {resolved_config.schema}.data_source_enum
                    AS ENUM ('arco', 'cds', 'other');
                EXCEPTION
                    WHEN duplicate_object THEN null;
                END $$;
                """
            )
        )

        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS
                {resolved_config.schema}.{resolved_config.dataset_table}
                (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    source {resolved_config.schema}.data_source_enum NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    transformed_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        )

    log_metric(
        "Table status",
        {
            "table": resolved_config.dataset_table,
            "status": "ready",
        },
    )


def create_dataset_table_detail(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Create or minimally migrate the weather detail table."""
    resolved_config = resolve_database_config(config)
    table = f"{resolved_config.schema}.{resolved_config.dataset_table_detail}"

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table}
                (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_id BIGINT NOT NULL,

                    valid_time TIMESTAMP NOT NULL,
                    valid_time_local TIMESTAMP,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,

                    tp DOUBLE PRECISION,
                    ssrd DOUBLE PRECISION,
                    t2m DOUBLE PRECISION,
                    d2m DOUBLE PRECISION,
                    swvl1 DOUBLE PRECISION,
                    u10 DOUBLE PRECISION,
                    v10 DOUBLE PRECISION,
                    swh DOUBLE PRECISION,
                    msl DOUBLE PRECISION,

                    CONSTRAINT fk_detail_dataset
                    FOREIGN KEY (dataset_id)
                    REFERENCES {resolved_config.schema}.{resolved_config.dataset_table}(id)
                    ON DELETE CASCADE
                )
                """
            )
        )

        # Backward-compatible migration for databases created by the earlier
        # pipeline version that used the generic column name `time`.
        connection.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = '{resolved_config.schema}'
                          AND table_name = '{resolved_config.dataset_table_detail}'
                          AND column_name = 'time'
                    ) AND NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = '{resolved_config.schema}'
                          AND table_name = '{resolved_config.dataset_table_detail}'
                          AND column_name = 'valid_time'
                    ) THEN
                        ALTER TABLE {table} RENAME COLUMN time TO valid_time;
                    END IF;
                END $$;
                """
            )
        )

        # Existing historical rows predate explicit local-time tracking, so
        # the migration intentionally leaves valid_time_local NULL for them.
        connection.execute(
            text(
                f"ALTER TABLE {table} "
                "ADD COLUMN IF NOT EXISTS valid_time_local TIMESTAMP"
            )
        )

        for column_name in ("d2m", "swvl1"):
            connection.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN IF NOT EXISTS {column_name} DOUBLE PRECISION"
                )
            )

    log_metric(
        "Table status",
        {
            "table": resolved_config.dataset_table_detail,
            "status": "ready",
        },
    )
