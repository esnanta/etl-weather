from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from pipeline.logging import log_step

logger = logging.getLogger(__name__)

load_dotenv()

SCHEMA_NAME = "climate_data"
DATASET_TABLE = "weather_dataset"
DATASET_TABLE_DETAIL = "weather_dataset_detail"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = os.environ["POSTGRES_HOST"]
    port: int = int(os.environ["POSTGRES_PORT"])
    database: str = os.environ["POSTGRES_DATABASE"]
    user: str = os.environ["POSTGRES_USER"]
    password: str = os.environ["POSTGRES_PASSWORD"]

    schema: str = SCHEMA_NAME
    dataset_table: str = DATASET_TABLE
    dataset_table_detail: str = DATASET_TABLE_DETAIL

    @property
    def url(self) -> str:
        return (
            "postgresql+psycopg://"
            f"{self.user}:"
            f"{self.password}@"
            f"{self.host}:"
            f"{self.port}/"
            f"{self.database}"
        )

    @property
    def url_maintenance(self) -> str:
        """
        Return a connection string to the maintenance
        database (e.g. 'postgres')
        which can be used to create the actual database.
        """
        return (
            "postgresql+psycopg://"
            f"{self.user}:"
            f"{self.password}@"
            f"{self.host}:"
            f"{self.port}/"
            "postgres"
        )


def ensure_database_exists(config: DatabaseConfig = DatabaseConfig()) -> None:
    """
    Create the database if it does not exist.
    """
    try:
        engine = create_engine(config.url)
        conn = engine.connect()
        conn.close()
        logger.info("Database '%s' already exists.", config.database)
        return
    except OperationalError as e:
        if f'database "{config.database}" does not exist' not in str(e):
            raise
        logger.info("Database '%s' does not exist. Creating...", config.database)

    maintenance_engine = create_engine(config.url_maintenance)
    with maintenance_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE DATABASE "{config.database}"'))

    logger.info("Database '%s' created.", config.database)


@log_step
def create_database_engine(
        config: DatabaseConfig = DatabaseConfig(),
) -> Engine:
    """
    Create SQLAlchemy engine.
    """
    ensure_database_exists(config)
    logger.info("Creating PostgreSQL engine for database '%s'.", config.database)

    return create_engine(
        config.url,
        future=True,
    )


@log_step
def create_schema(
        engine: Engine,
        config: DatabaseConfig = DatabaseConfig(),
) -> None:
    """
    Create schema.
    """

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE SCHEMA IF NOT EXISTS
                {config.schema}
                """
            )
        )

    logger.info(
        {
            "schema": config.schema,
            "status": "ready",
        }
    )


@log_step
def create_dataset_table(
        engine: Engine,
        config: DatabaseConfig = DatabaseConfig(),
) -> None:
    """
    Create dataset table with ENUM for data source.
    """

    with engine.begin() as connection:
        # Create ENUM type safely (idempotent)
        connection.execute(
            text(
                f"""
                DO $$ BEGIN
                    CREATE TYPE {config.schema}.data_source_enum AS ENUM ('arco', 'cds', 'other');
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
                {config.schema}.{config.dataset_table}
                (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    source {config.schema}.data_source_enum NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    transformed_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        )

    logger.info(
        {
            "table": config.dataset_table,
            "status": "ready",
        }
    )


@log_step
def create_dataset_table_detail(
        engine: Engine,
        config: DatabaseConfig = DatabaseConfig(),
) -> None:
    """
    Create dataset detail table (weather data rows).
    """

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS
                {config.schema}.{config.dataset_table_detail}
                (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_id BIGINT NOT NULL,

                    time TIMESTAMP NOT NULL,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,

                    tp DOUBLE PRECISION,
                    ssrd DOUBLE PRECISION,
                    t2m DOUBLE PRECISION,
                    u10 DOUBLE PRECISION,
                    v10 DOUBLE PRECISION,
                    swh DOUBLE PRECISION,
                    msl DOUBLE PRECISION,

                    CONSTRAINT
                    fk_detail_dataset
                    FOREIGN KEY
                    (
                        dataset_id
                    )
                    REFERENCES
                    {config.schema}.{config.dataset_table}
                    (
                        id
                    )
                    ON DELETE CASCADE
                )
                """
            )
        )

    logger.info(
        {
            "table": config.dataset_table_detail,
            "status": "ready",
        }
    )