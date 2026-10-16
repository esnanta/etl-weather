"""Canonical PostgreSQL configuration and schema identifiers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from dotenv import load_dotenv
from sqlalchemy.engine import URL

APPLICATION_SCHEMA = "climate_data"
SCHEMA_NAME = APPLICATION_SCHEMA
TABLE_PREFIX = "t_"
VIEW_PREFIX = "vw_"


def table_name(base_name: str) -> str:
    return f"{TABLE_PREFIX}{base_name}"


def view_name(base_name: str) -> str:
    return f"{VIEW_PREFIX}{base_name}"


DAILY_CLIMATE_TABLE = table_name("daily_climate")
DAILY_CLIMATE_DETAIL_TABLE = table_name("daily_climate_detail")
MONTHLY_CLIMATE_TABLE = table_name("monthly_climate")
MONTHLY_CLIMATE_DETAIL_TABLE = table_name("monthly_climate_detail")
PROVINCE_TABLE = table_name("province")
DISTRICT_TABLE = table_name("district")
GRID_REGION_TABLE = table_name("grid_region")
DAILY_CLIMATE_REGION_VIEW = view_name("daily_climate_region")
MONTHLY_CLIMATE_REGION_VIEW = view_name("monthly_climate_region")
PIPELINE_PROGRESS_SCHEMA = APPLICATION_SCHEMA
PACING_SCHEMA = APPLICATION_SCHEMA
PIPELINE_PROGRESS_TABLE = table_name("pipeline_progress")
PIPELINE_PROGRESS_DETAIL_TABLE = table_name("pipeline_progress_detail")
PACING_TABLE = table_name("submission_pacing")


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = APPLICATION_SCHEMA
    daily_climate_table: str = DAILY_CLIMATE_TABLE
    daily_climate_detail_table: str = DAILY_CLIMATE_DETAIL_TABLE

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        prefix: str = "POSTGRES_",
    ) -> "DatabaseConfig":
        """Resolve one explicit environment family; never fall back to another."""
        if prefix not in {"POSTGRES_", "TEST_POSTGRES_"}:
            raise ValueError("Unsupported PostgreSQL configuration prefix.")
        if env is None:
            load_dotenv()
        source = os.environ if env is None else env
        names = ("HOST", "PORT", "DATABASE", "USER", "PASSWORD")
        missing = [prefix + name for name in names if not source.get(prefix + name)]
        if missing:
            raise ValueError("Missing required database environment variables: " + ", ".join(missing))
        port = int(source[prefix + "PORT"])
        if not 1 <= port <= 65535:
            raise ValueError("PostgreSQL port must be between 1 and 65535.")
        return cls(
            host=source[prefix + "HOST"],
            port=port,
            database=source[prefix + "DATABASE"],
            user=source[prefix + "USER"],
            password=source[prefix + "PASSWORD"],
        )

    @property
    def sqlalchemy_url(self) -> URL:
        """Keep credentials as URL components, including special characters."""
        return URL.create(
            "postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    @property
    def maintenance_url(self) -> URL:
        return self.sqlalchemy_url.set(database="postgres")

    @property
    def url(self) -> str:
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    @property
    def url_maintenance(self) -> str:
        return self.maintenance_url.render_as_string(hide_password=False)


def resolve_database_config(config: DatabaseConfig | None = None) -> DatabaseConfig:
    return config if config is not None else DatabaseConfig.from_env()


def resolve_test_database_config(env: Mapping[str, str] | None = None) -> DatabaseConfig:
    """Require the test-only variables and refuse non-test database identities."""
    config = DatabaseConfig.from_env(env, prefix="TEST_POSTGRES_")
    if "test" not in config.database.lower() or config.database.lower() in {
        "postgres", "template0", "template1", "weather"
    }:
        raise ValueError("Test database name must be clearly identified as a test database.")
    return config
