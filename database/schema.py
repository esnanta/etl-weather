"""Two independent schema initialization paths sharing a single DDL owner.

Climate DDL does not enable PostGIS or create spatial tables.
Geospatial DDL does not create operational or climate tables.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import (
    APPLICATION_SCHEMA, PIPELINE_PROGRESS_TABLE, PIPELINE_PROGRESS_DETAIL_TABLE,
    PACING_TABLE, DatabaseConfig,
)
from database.climate import (
    create_schema, create_daily_climate_table, create_daily_climate_detail_table,
)


def _parent_table() -> str:
    return f"{APPLICATION_SCHEMA}.{PIPELINE_PROGRESS_TABLE}"


def _detail_table() -> str:
    return f"{APPLICATION_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}"


def create_pipeline_progress_tables(engine: Engine) -> None:
    """Create cadence-neutral operational progress tables in PostgreSQL."""
    parent = _parent_table()
    detail = _detail_table()
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {APPLICATION_SCHEMA}"))
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {parent}
                (
                    id BIGSERIAL PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    configuration_identity TEXT NOT NULL,
                    expected_partition_count INTEGER NOT NULL,
                    completed_partition_count INTEGER NOT NULL DEFAULT 0,
                    failed_partition_count INTEGER NOT NULL DEFAULT 0,
                    execution_status TEXT NOT NULL DEFAULT 'pending',
                    validation_status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    selection_owner TEXT,
                    selection_claimed_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_{PIPELINE_PROGRESS_TABLE}_identity
                        UNIQUE (
                            pipeline_name,
                            source,
                            dataset_name,
                            start_date,
                            end_date,
                            configuration_identity
                        ),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_TABLE}_period
                        CHECK (start_date <= end_date),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_TABLE}_execution_status
                        CHECK (execution_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_TABLE}_validation_status
                        CHECK (validation_status IN ('pending', 'failed', 'valid'))
                )
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {detail}
                (
                    id BIGSERIAL PRIMARY KEY,
                    pipeline_progress_id BIGINT NOT NULL,
                    request_key TEXT NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    raw_artifact_path TEXT,
                    raw_sha256 TEXT,
                    raw_file_size_bytes BIGINT,
                    transformed_artifact_path TEXT,
                    transformed_sha256 TEXT,
                    transformed_file_size_bytes BIGINT,
                    temporal_start TIMESTAMP,
                    temporal_end TIMESTAMP,
                    temporal_count INTEGER,
                    latitude_min DOUBLE PRECISION,
                    latitude_max DOUBLE PRECISION,
                    latitude_count INTEGER,
                    longitude_min DOUBLE PRECISION,
                    longitude_max DOUBLE PRECISION,
                    longitude_count INTEGER,
                    grid_count INTEGER,
                    row_count INTEGER,
                    extract_status TEXT NOT NULL DEFAULT 'pending',
                    transform_status TEXT NOT NULL DEFAULT 'pending',
                    load_status TEXT NOT NULL DEFAULT 'pending',
                    validation_status TEXT NOT NULL DEFAULT 'pending',
                    validation_message TEXT,
                    last_error TEXT,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    metrics JSONB,
                    CONSTRAINT fk_{PIPELINE_PROGRESS_DETAIL_TABLE}_parent
                        FOREIGN KEY (pipeline_progress_id)
                        REFERENCES {parent}(id)
                        ON DELETE CASCADE,
                    CONSTRAINT uq_{PIPELINE_PROGRESS_DETAIL_TABLE}_request
                        UNIQUE (pipeline_progress_id, request_key),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_period
                        CHECK (start_date <= end_date),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_extract_status
                        CHECK (extract_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_transform_status
                        CHECK (transform_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_load_status
                        CHECK (load_status IN ('pending', 'running', 'failed', 'complete')),
                    CONSTRAINT ck_{PIPELINE_PROGRESS_DETAIL_TABLE}_validation_status
                        CHECK (validation_status IN ('pending', 'failed', 'valid'))
                )
                """
            )
        )
        connection.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS ix_{PIPELINE_PROGRESS_TABLE}_selection "
                f"ON {parent} (pipeline_name, source, dataset_name, start_date, execution_status)"
            )
        )



def create_submission_pacing_table(engine: Engine) -> None:
    """Create the durable global submission clock without provider identity."""
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {APPLICATION_SCHEMA}"))
        connection.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {APPLICATION_SCHEMA}.{PACING_TABLE} (
                    singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
                    last_submission_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""
            )
        )




def initialize_climate_schema(engine: Engine, config: DatabaseConfig | None = None) -> None:
    """Initialize climate and operational tables in FK order."""
    create_schema(engine, config)
    create_daily_climate_table(engine, config)
    create_daily_climate_detail_table(engine, config)
    from database.monthly_climate import create_monthly_climate_tables

    create_monthly_climate_tables(engine, config)
    create_pipeline_progress_tables(engine)
    create_submission_pacing_table(engine)


def initialize_geospatial_schema(engine: Engine, config: DatabaseConfig | None = None) -> None:
    """Initialize spatial tables and the dependent daily region view."""
    from database.geospatial import create_spatial_tables
    from database.vw_daily_climate_region import create_daily_region_view
    from database.vw_monthly_climate_region import create_monthly_region_view

    create_spatial_tables(engine, config)
    create_daily_region_view(engine)
    create_monthly_region_view(engine)
