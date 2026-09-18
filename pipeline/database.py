from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Mapping

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from pipeline.logging import log_metric

logger = logging.getLogger(__name__)

SCHEMA_NAME = "climate_data"
DAILY_CLIMATE_TABLE = "daily_climate"
DAILY_CLIMATE_DETAIL_TABLE = "daily_climate_detail"

DAILY_CLIMATE_COLUMNS = (
    "tp_daily_total_mm",
    "t2m_daily_mean_c",
    "d2m_daily_mean_c",
    "swvl1_daily_mean_m3_m3",
    "ssrd_daily_total_j_m2",
)


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = SCHEMA_NAME
    daily_climate_table: str = DAILY_CLIMATE_TABLE
    daily_climate_detail_table: str = DAILY_CLIMATE_DETAIL_TABLE

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
            raise ValueError(
                "Missing required database environment variables: "
                + ", ".join(missing_keys)
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
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {resolved_config.schema}"))
    log_metric("Schema status", {"schema": resolved_config.schema, "status": "ready"})


def create_daily_climate_table(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Create metadata for one logical daily-climate partition."""
    resolved_config = resolve_database_config(config)
    table = f"{resolved_config.schema}.{resolved_config.daily_climate_table}"
    index_name = f"uq_{resolved_config.daily_climate_table}_request_key"

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                DO $$ BEGIN
                    CREATE TYPE {resolved_config.schema}.data_source_enum
                    AS ENUM ('cds', 'other');
                EXCEPTION WHEN duplicate_object THEN null;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table}
                (
                    id BIGSERIAL PRIMARY KEY,
                    request_key TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    source {resolved_config.schema}.data_source_enum NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    variable_signature TEXT NOT NULL,
                    north_lat DOUBLE PRECISION NOT NULL,
                    south_lat DOUBLE PRECISION NOT NULL,
                    east_lon DOUBLE PRECISION NOT NULL,
                    west_lon DOUBLE PRECISION NOT NULL,
                    downloaded_at TIMESTAMPTZ NOT NULL,
                    transformed_artifact_path TEXT NOT NULL,
                    transformed_sha256 TEXT NOT NULL,
                    transformed_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                f"ON {table} (request_key)"
            )
        )

    log_metric(
        "Table status",
        {
            "table": resolved_config.daily_climate_table,
            "status": "ready",
            "idempotency_key": "request_key",
            "provenance": (
                "downloaded_at + transformed_artifact_path + transformed_sha256"
            ),
        },
    )


def create_daily_climate_detail_table(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Create canonical daily climate observations."""
    resolved_config = resolve_database_config(config)
    table = f"{resolved_config.schema}.{resolved_config.daily_climate_detail_table}"
    index_name = f"uq_{resolved_config.daily_climate_detail_table}_grid_time"

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table}
                (
                    id BIGSERIAL PRIMARY KEY,
                    daily_climate_id BIGINT NOT NULL,
                    valid_time TIMESTAMP NOT NULL,
                    valid_time_local TIMESTAMP NOT NULL,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,
                    tp_daily_total_mm DOUBLE PRECISION,
                    t2m_daily_mean_c DOUBLE PRECISION,
                    d2m_daily_mean_c DOUBLE PRECISION,
                    swvl1_daily_mean_m3_m3 DOUBLE PRECISION,
                    ssrd_daily_total_j_m2 DOUBLE PRECISION,
                    CONSTRAINT fk_daily_climate_detail_daily_climate
                    FOREIGN KEY (daily_climate_id)
                    REFERENCES {resolved_config.schema}.{resolved_config.daily_climate_table}(id)
                    ON DELETE CASCADE
                )
                """
            )
        )
        connection.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                f"ON {table} (daily_climate_id, valid_time, latitude, longitude)"
            )
        )

    log_metric(
        "Table status",
        {
            "table": resolved_config.daily_climate_detail_table,
            "status": "ready",
            "weather_column_contract": "provider-based daily statistics",
            "idempotency_key": (
                "daily_climate_id + valid_time + latitude + longitude"
            ),
        },
    )
