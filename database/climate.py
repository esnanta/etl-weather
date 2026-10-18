from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import DatabaseConfig, resolve_database_config
from pipeline.observability.logging import log_metric

DAILY_CLIMATE_COLUMNS = (
    "tp_daily_total_mm",
    "t2m_daily_mean_c",
    "d2m_daily_mean_c",
    "swvl1_daily_mean_m3_m3",
    "ssrd_daily_total_j_m2",
)


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


def main() -> None:
    """Preview climate tables by default; apply DDL only with --execute."""
    import argparse
    from database.connection import create_database_engine
    from database.config import (
        DAILY_CLIMATE_TABLE, DAILY_CLIMATE_DETAIL_TABLE,
        MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE,
        PIPELINE_PROGRESS_TABLE, PIPELINE_PROGRESS_DETAIL_TABLE, PACING_TABLE,
    )
    from database.schema import initialize_climate_schema

    parser = argparse.ArgumentParser(
        description="Initialize climate and operational tables without PostGIS."
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Create the seven missing climate/operational tables.",
    )
    args = parser.parse_args()
    config = resolve_database_config()
    print(f"Climate schema target: database={config.database}, schema={config.schema}")
    print("Tables: " + ", ".join((
        DAILY_CLIMATE_TABLE, DAILY_CLIMATE_DETAIL_TABLE,
        MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE,
        PIPELINE_PROGRESS_TABLE, PIPELINE_PROGRESS_DETAIL_TABLE, PACING_TABLE,
    )))
    if not args.execute:
        print("Dry run: no database changes. Pass --execute to create missing objects.")
        return
    engine = create_database_engine(config)
    try:
        initialize_climate_schema(engine, config)
        print("Climate and operational tables ready.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
