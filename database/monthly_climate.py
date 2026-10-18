"""Prospective monthly climate storage; no monthly ETL or aggregation is implemented."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import (
    APPLICATION_SCHEMA, MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE,
    DatabaseConfig, resolve_database_config,
)


def create_monthly_climate_tables(engine: Engine, config: DatabaseConfig | None = None) -> None:
    """Create empty, idempotent monthly parent/detail tables without transforming data."""
    resolved = resolve_database_config(config)
    if resolved.schema != APPLICATION_SCHEMA:
        raise ValueError("Monthly tables require climate_data application schema.")
    parent = f"{resolved.schema}.{MONTHLY_CLIMATE_TABLE}"
    detail = f"{resolved.schema}.{MONTHLY_CLIMATE_DETAIL_TABLE}"
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {parent} (
                id BIGSERIAL PRIMARY KEY,
                request_key TEXT NOT NULL UNIQUE,
                dataset_name TEXT NOT NULL,
                source {resolved.schema}.data_source_enum NOT NULL,
                month_start DATE NOT NULL,
                month_end DATE NOT NULL,
                variable_signature TEXT NOT NULL,
                north_lat DOUBLE PRECISION NOT NULL,
                south_lat DOUBLE PRECISION NOT NULL,
                east_lon DOUBLE PRECISION NOT NULL,
                west_lon DOUBLE PRECISION NOT NULL,
                CONSTRAINT ck_{MONTHLY_CLIMATE_TABLE}_month CHECK (
                    EXTRACT(DAY FROM month_start) = 1
                    AND month_end = (month_start + INTERVAL '1 month' - INTERVAL '1 day')::date
                )
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {detail} (
                id BIGSERIAL PRIMARY KEY,
                monthly_climate_id BIGINT NOT NULL
                    REFERENCES {parent}(id) ON DELETE CASCADE,
                month_start DATE NOT NULL CHECK (EXTRACT(DAY FROM month_start) = 1),
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                tp_monthly_total_mm DOUBLE PRECISION,
                t2m_monthly_mean_c DOUBLE PRECISION,
                d2m_monthly_mean_c DOUBLE PRECISION,
                swvl1_monthly_mean_m3_m3 DOUBLE PRECISION,
                ssrd_monthly_total_j_m2 DOUBLE PRECISION,
                CONSTRAINT uq_{MONTHLY_CLIMATE_DETAIL_TABLE}_grid_month
                    UNIQUE (monthly_climate_id, month_start, latitude, longitude)
            )
        """))
