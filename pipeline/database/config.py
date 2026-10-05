from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Mapping

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from config import APPLICATION_SCHEMA

logger = logging.getLogger(__name__)

# Public schema name retained for existing database callers.
SCHEMA_NAME = APPLICATION_SCHEMA
DAILY_CLIMATE_TABLE = "daily_climate"
DAILY_CLIMATE_DETAIL_TABLE = "daily_climate_detail"


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
