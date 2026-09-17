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
DATASET_TABLE = "weather_dataset"
DATASET_TABLE_DETAIL = "weather_dataset_detail"

DAILY_WEATHER_COLUMNS = (
    "tp_daily_total_mm",
    "t2m_daily_mean_c",
    "d2m_daily_mean_c",
    "swvl1_daily_mean_m3_m3",
    "ssrd_daily_total_j_m2",
)

LEGACY_TO_DAILY_COLUMNS = {
    "tp": "tp_daily_total_mm",
    "total_precipitation": "tp_daily_total_mm",
    "t2m": "t2m_daily_mean_c",
    "2m_temperature": "t2m_daily_mean_c",
    "d2m": "d2m_daily_mean_c",
    "2m_dewpoint_temperature": "d2m_daily_mean_c",
    "swvl1": "swvl1_daily_mean_m3_m3",
    "volumetric_soil_water_layer_1": "swvl1_daily_mean_m3_m3",
    "ssrd": "ssrd_daily_total_j_m2",
    "surface_solar_radiation_downwards": "ssrd_daily_total_j_m2",
}


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


def create_dataset_table(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Create dataset metadata with a stable logical request identity."""
    resolved_config = resolve_database_config(config)
    table = f"{resolved_config.schema}.{resolved_config.dataset_table}"
    index_name = f"uq_{resolved_config.dataset_table}_request_key"

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                DO $$ BEGIN
                    CREATE TYPE {resolved_config.schema}.data_source_enum
                    AS ENUM ('arco', 'cds', 'other');
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
                    request_key TEXT,
                    dataset_name TEXT NOT NULL,
                    source {resolved_config.schema}.data_source_enum NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    variable_signature TEXT,
                    north_lat DOUBLE PRECISION,
                    south_lat DOUBLE PRECISION,
                    east_lon DOUBLE PRECISION,
                    west_lon DOUBLE PRECISION,
                    transformed_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        )

        for column_definition in (
            "request_key TEXT",
            "variable_signature TEXT",
            "north_lat DOUBLE PRECISION",
            "south_lat DOUBLE PRECISION",
            "east_lon DOUBLE PRECISION",
            "west_lon DOUBLE PRECISION",
        ):
            connection.execute(
                text(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                    f"{column_definition}"
                )
            )

        connection.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                f"ON {table} (request_key) WHERE request_key IS NOT NULL"
            )
        )

    log_metric(
        "Table status",
        {
            "table": resolved_config.dataset_table,
            "status": "ready",
            "idempotency_key": "request_key",
        },
    )


def _migrate_legacy_weather_columns(
    connection,
    *,
    schema: str,
    table_name: str,
    table: str,
) -> None:
    for legacy_name, daily_name in LEGACY_TO_DAILY_COLUMNS.items():
        connection.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                          AND table_name = '{table_name}'
                          AND column_name = '{legacy_name}'
                    ) THEN
                        EXECUTE 'UPDATE {table} SET {daily_name} = COALESCE({daily_name}, "{legacy_name}")';
                        EXECUTE 'ALTER TABLE {table} DROP COLUMN "{legacy_name}"';
                    END IF;
                END $$;
                """
            )
        )


def create_dataset_table_detail(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Create observations with a retry-safe natural uniqueness constraint."""
    resolved_config = resolve_database_config(config)
    table = f"{resolved_config.schema}.{resolved_config.dataset_table_detail}"
    index_name = f"uq_{resolved_config.dataset_table_detail}_grid_time"

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
                    tp_daily_total_mm DOUBLE PRECISION,
                    t2m_daily_mean_c DOUBLE PRECISION,
                    d2m_daily_mean_c DOUBLE PRECISION,
                    swvl1_daily_mean_m3_m3 DOUBLE PRECISION,
                    ssrd_daily_total_j_m2 DOUBLE PRECISION,
                    CONSTRAINT fk_detail_dataset
                    FOREIGN KEY (dataset_id)
                    REFERENCES {resolved_config.schema}.{resolved_config.dataset_table}(id)
                    ON DELETE CASCADE
                )
                """
            )
        )

        connection.execute(
            text(
                f"ALTER TABLE {table} "
                "ADD COLUMN IF NOT EXISTS valid_time_local TIMESTAMP"
            )
        )
        for column_name in DAILY_WEATHER_COLUMNS:
            connection.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN IF NOT EXISTS {column_name} DOUBLE PRECISION"
                )
            )

        _migrate_legacy_weather_columns(
            connection,
            schema=resolved_config.schema,
            table_name=resolved_config.dataset_table_detail,
            table=table,
        )

        # Older versions could append duplicate detail rows. Keep one row per
        # dataset/time/grid before enforcing the retry-safe uniqueness index.
        connection.execute(
            text(
                f"""
                DELETE FROM {table} duplicate
                USING {table} keeper
                WHERE duplicate.id > keeper.id
                  AND duplicate.dataset_id = keeper.dataset_id
                  AND duplicate.valid_time = keeper.valid_time
                  AND duplicate.latitude = keeper.latitude
                  AND duplicate.longitude = keeper.longitude
                """
            )
        )
        connection.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                f"ON {table} (dataset_id, valid_time, latitude, longitude)"
            )
        )

    log_metric(
        "Table status",
        {
            "table": resolved_config.dataset_table_detail,
            "status": "ready",
            "weather_column_contract": "provider-based daily statistics",
            "idempotency_key": "dataset_id + valid_time + latitude + longitude",
        },
    )
