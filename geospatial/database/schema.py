"""Idempotent DDL for administrative references and grid-to-region mapping.

Schema bootstrap is independent from both climate processing and Airflow.
It creates only missing spatial objects and never drops existing objects.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from geospatial.database.bootstrap import ensure_postgis_extension
from pipeline.database.config import (
    APPLICATION_SCHEMA,
    DISTRICT_TABLE,
    GRID_REGION_TABLE,
    PROVINCE_TABLE,
    DatabaseConfig,
    resolve_database_config,
)


def create_spatial_tables(
    engine: Engine,
    config: DatabaseConfig | None = None,
) -> None:
    """Create three spatial tables and their constraints after PostGIS bootstrap."""
    resolved = resolve_database_config(config)
    if resolved.schema != APPLICATION_SCHEMA:
        raise ValueError("Spatial tables must use the shared climate_data schema.")
    ensure_postgis_extension(engine, expected_database=resolved.database)
    schema = APPLICATION_SCHEMA
    province = f"{schema}.{PROVINCE_TABLE}"
    district = f"{schema}.{DISTRICT_TABLE}"
    grid = f"{schema}.{GRID_REGION_TABLE}"

    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {province} (
                id BIGSERIAL PRIMARY KEY,
                country_code VARCHAR(2) NOT NULL
                    CHECK (country_code ~ '^[a-z]{{2}}$'),
                province_name VARCHAR(255) NOT NULL,
                source_dataset VARCHAR(128) NOT NULL,
                source_version VARCHAR(128) NOT NULL,
                source_feature_ref VARCHAR(128),
                boundary_geometry public.geometry(MultiPolygonZ, 4326) NOT NULL,
                CONSTRAINT ck_province_geom_valid CHECK (public.ST_IsValid(boundary_geometry)),
                CONSTRAINT ck_province_geom_nonempty CHECK (NOT public.ST_IsEmpty(boundary_geometry)),
                CONSTRAINT uq_province_country_name UNIQUE (country_code, province_name),
                CONSTRAINT uq_province_source_feature UNIQUE
                    (source_dataset, source_version, source_feature_ref)
            )
        """))
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {district} (
                id BIGSERIAL PRIMARY KEY,
                province_id BIGINT NOT NULL
                    REFERENCES {province}(id) ON DELETE RESTRICT,
                district_name VARCHAR(255) NOT NULL,
                source_dataset VARCHAR(128) NOT NULL,
                source_version VARCHAR(128) NOT NULL,
                source_feature_ref VARCHAR(128) NOT NULL,
                boundary_geometry geometry(MultiPolygonZ, 4326) NOT NULL,
                CONSTRAINT ck_district_geom_valid CHECK (ST_IsValid(boundary_geometry)),
                CONSTRAINT ck_district_geom_nonempty CHECK (NOT ST_IsEmpty(boundary_geometry)),
                CONSTRAINT uq_district_province_name UNIQUE (province_id, district_name),
                CONSTRAINT uq_district_source_feature UNIQUE
                    (source_dataset, source_version, source_feature_ref),
                CONSTRAINT uq_district_id_province UNIQUE (id, province_id)
            )
        """))
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {grid} (
                id BIGSERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL
                    CHECK (latitude BETWEEN -90 AND 90),
                longitude DOUBLE PRECISION NOT NULL
                    CHECK (longitude BETWEEN -180 AND 180),
                province_id BIGINT REFERENCES {province}(id) ON DELETE RESTRICT,
                district_id BIGINT,
                match_status VARCHAR(16) NOT NULL,
                match_count INTEGER NOT NULL CHECK (match_count >= 0),
                mapped_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_grid_region_coordinates UNIQUE (latitude, longitude),
                CONSTRAINT ck_grid_region_status CHECK (
                    match_status IN ('matched', 'unmatched', 'ambiguous')
                ),
                CONSTRAINT ck_grid_region_match_consistency CHECK (
                    (match_status = 'matched'
                        AND match_count = 1
                        AND province_id IS NOT NULL
                        AND district_id IS NOT NULL)
                    OR (match_status = 'unmatched'
                        AND match_count = 0
                        AND district_id IS NULL
                        AND province_id IS NULL)
                    OR (match_status = 'ambiguous'
                        AND match_count > 1
                        AND district_id IS NULL
                        AND province_id IS NULL)
                ),
                CONSTRAINT fk_grid_region_district_province
                    FOREIGN KEY (district_id, province_id)
                    REFERENCES {district}(id, province_id) ON DELETE RESTRICT
            )
        """))
        for name, table in (
            ("ix_t_province_geom", province),
            ("ix_t_district_geom", district),
        ):
            connection.execute(text(
                f"CREATE INDEX IF NOT EXISTS {name} ON {table} USING GIST (boundary_geometry)"
            ))
        connection.execute(text(
            f"CREATE INDEX IF NOT EXISTS ix_t_district_province_id "
            f"ON {district} (province_id)"
        ))
        connection.execute(text(
            f"CREATE INDEX IF NOT EXISTS ix_t_grid_region_district_id "
            f"ON {grid} (district_id)"
        ))
        connection.execute(text(
            f"CREATE INDEX IF NOT EXISTS ix_t_grid_region_province_id "
            f"ON {grid} (province_id)"
        ))


def main() -> None:
    """Apply the spatial DDL only after explicit CLI confirmation."""
    import argparse
    from sqlalchemy import create_engine

    parser = argparse.ArgumentParser(description="Create non-destructive spatial tables.")
    parser.add_argument(
        "--execute", action="store_true",
        help="Apply CREATE TABLE/INDEX to the configured PostgreSQL database.",
    )
    args = parser.parse_args()
    config = resolve_database_config()
    print(f"Spatial schema target: database={config.database}, schema={config.schema}")
    print(f"Tables: {PROVINCE_TABLE}, {DISTRICT_TABLE}, {GRID_REGION_TABLE}")
    if not args.execute:
        print("Dry run: no database changes. Pass --execute to create missing objects.")
        return

    engine = create_engine(config.url, future=True)
    try:
        create_spatial_tables(engine, config)
        print("Spatial tables ready; no GeoJSON imported.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
