"""Verify Plan C schema convergence on isolated real PostgreSQL.

The tests exercise production DDL against a temporary schema within the
TEST_POSTGRES_* database. No application or production tables are modified.
"""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from database.climate import (
    DAILY_CLIMATE_COLUMNS,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)
from database.config import resolve_test_database_config

pytestmark = pytest.mark.integration

ADDITIONAL_COLUMNS = DAILY_CLIMATE_COLUMNS[5:]


@pytest.fixture
def isolated_schema():
    base = resolve_test_database_config()
    config = replace(base, schema=f"climate_test_plan_c_{uuid4().hex}")
    engine = create_engine(config.url, future=True)
    try:
        create_schema(engine, config)
        create_daily_climate_table(engine, config)
        yield engine, config
    finally:
        with engine.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {config.schema} CASCADE"))
        engine.dispose()


def _columns(connection, config):
    return {
        row["column_name"]: (row["data_type"], row["is_nullable"])
        for row in connection.execute(
            text(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table"
            ),
            {"schema": config.schema, "table": config.daily_climate_detail_table},
        ).mappings()
    }


def _old_detail_table(connection, config, *, wrong_wind_type=False):
    parent = f"{config.schema}.{config.daily_climate_table}"
    detail = f"{config.schema}.{config.daily_climate_detail_table}"
    wind = "wind_direction_daily_deg TEXT," if wrong_wind_type else ""
    connection.execute(text(f"""
        CREATE TABLE {detail} (
            id BIGSERIAL PRIMARY KEY,
            daily_climate_id BIGINT NOT NULL REFERENCES {parent}(id) ON DELETE CASCADE,
            valid_time TIMESTAMP NOT NULL,
            valid_time_local TIMESTAMP NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            tp_daily_total_mm DOUBLE PRECISION,
            t2m_daily_mean_c DOUBLE PRECISION,
            d2m_daily_mean_c DOUBLE PRECISION,
            swvl1_daily_mean_m3_m3 DOUBLE PRECISION,
            ssrd_daily_total_j_m2 DOUBLE PRECISION,
            {wind}
            CONSTRAINT uq_plan_c_grid UNIQUE (
                daily_climate_id, valid_time, latitude, longitude
            )
        )
    """))


def test_fresh_detail_schema_contains_all_nullable_observations(isolated_schema):
    engine, config = isolated_schema
    create_daily_climate_detail_table(engine, config)
    with engine.connect() as connection:
        columns = _columns(connection, config)
    for name in DAILY_CLIMATE_COLUMNS:
        assert columns[name] == ("double precision", "YES")
    for name in ("id", "daily_climate_id", "valid_time", "valid_time_local"):
        assert columns[name][1] == "NO"


def test_old_detail_converges_without_changing_historical_row(isolated_schema):
    engine, config = isolated_schema
    parent = f"{config.schema}.{config.daily_climate_table}"
    detail = f"{config.schema}.{config.daily_climate_detail_table}"
    with engine.begin() as connection:
        _old_detail_table(connection, config)
        parent_id = connection.execute(text(f"""
            INSERT INTO {parent} (
                request_key, dataset_name, source, start_date, end_date,
                variable_signature, north_lat, south_lat, east_lon, west_lon,
                downloaded_at, transformed_artifact_path, transformed_sha256,
                transformed_at
            ) VALUES (
                'plan_c_old', 'reanalysis-era5-land', 'cds', '2024-01-01',
                '2024-01-01', 'd2m_ssrd_swvl1_t2m_tp', 6.1, 1.9, 98.3,
                94.9, CURRENT_TIMESTAMP, 'legacy.parquet', 'legacy-sha',
                CURRENT_TIMESTAMP
            ) RETURNING id
        """)).scalar_one()
        detail_id = connection.execute(text(f"""
            INSERT INTO {detail} (
                daily_climate_id, valid_time, valid_time_local, latitude,
                longitude, tp_daily_total_mm
            ) VALUES (
                :parent_id, '2023-12-31 17:00:00', '2024-01-01 00:00:00',
                4.5, 96.8, 12.5
            ) RETURNING id
        """), {"parent_id": parent_id}).scalar_one()

    create_daily_climate_detail_table(engine, config)
    create_daily_climate_detail_table(engine, config)

    with engine.connect() as connection:
        columns = _columns(connection, config)
        row = connection.execute(text(
            f"SELECT * FROM {detail} WHERE id = :id"
        ), {"id": detail_id}).mappings().one()
        duplicate_index = connection.execute(text("""
            SELECT count(*) FROM pg_indexes
            WHERE schemaname = :schema AND indexname = :index_name
        """), {
            "schema": config.schema,
            "index_name": f"uq_{config.daily_climate_detail_table}_grid_time",
        }).scalar_one()
        fk_count = connection.execute(text("""
            SELECT COUNT(*) FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = :schema AND t.relname = :table
              AND c.contype = 'f'
        """), {"schema": config.schema, "table": config.daily_climate_detail_table}).scalar_one()

    assert row["tp_daily_total_mm"] == 12.5
    assert row["daily_climate_id"] == parent_id
    assert all(columns[name] == ("double precision", "YES") for name in ADDITIONAL_COLUMNS)
    assert all(row[name] is None for name in ADDITIONAL_COLUMNS)
    assert duplicate_index == 1
    assert fk_count == 1


def test_incompatible_existing_observation_type_fails_without_partial_migration(
    isolated_schema,
):
    engine, config = isolated_schema
    with engine.begin() as connection:
        _old_detail_table(connection, config, wrong_wind_type=True)

    with pytest.raises(RuntimeError, match="Incompatible.*wind_direction_daily_deg"):
        create_daily_climate_detail_table(engine, config)

    with engine.connect() as connection:
        columns = _columns(connection, config)
    assert columns["wind_direction_daily_deg"] == ("text", "YES")
    assert "swvl2_daily_mean_m3_m3" not in columns
