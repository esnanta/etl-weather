"""PostgreSQL integration: daily regional view preserves observation membership.

Requires an explicitly configured, dedicated empty TEST_POSTGRES_* database.
The test never uses the application 'weather' database, never drops schemas,
and cleans up only its own fixture records after execution.
"""
from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from database.config import resolve_test_database_config
from database.schema import initialize_climate_schema, initialize_geospatial_schema

pytestmark = pytest.mark.integration

_SOURCE = "region_view_integration_fixture"


@pytest.fixture
def region_view_db():
    load_dotenv()
    keys = ("HOST", "PORT", "DATABASE", "USER", "PASSWORD")
    if not all(os.getenv("TEST_POSTGRES_" + key) for key in keys):
        pytest.skip("Requires isolated TEST_POSTGRES_* settings")

    config = resolve_test_database_config()
    engine = create_engine(config.sqlalchemy_url, future=True)
    cleanup_ids = None
    try:
        with engine.connect() as conn:
            assert conn.execute(text("SELECT current_database()")).scalar_one() == config.database
            postgis_schema = conn.execute(text("""
                SELECT n.nspname
                FROM pg_extension e
                JOIN pg_namespace n ON n.oid = e.extnamespace
                WHERE e.extname = 'postgis'
            """)).scalar_one_or_none()
            if postgis_schema not in (None, "climate_data"):
                pytest.fail("PostGIS extension is in an incompatible schema; refusing relocation.")

        initialize_climate_schema(engine, config)
        initialize_geospatial_schema(engine, config)

        with engine.connect() as conn:
            total = conn.execute(text("""
                SELECT (SELECT COUNT(*) FROM climate_data.t_daily_climate)
                     + (SELECT COUNT(*) FROM climate_data.t_daily_climate_detail)
                     + (SELECT COUNT(*) FROM climate_data.t_province)
                     + (SELECT COUNT(*) FROM climate_data.t_district)
                     + (SELECT COUNT(*) FROM climate_data.t_grid_region)
            """)).scalar_one()
        if total:
            pytest.fail("Regional view integration requires empty isolated test tables.")

        with engine.begin() as conn:
            province_id = conn.execute(text("""
                INSERT INTO climate_data.t_province (
                    country_code, province_name, source_dataset, source_version,
                    boundary_geometry
                ) VALUES (
                    'id', 'Integration Province', :source, 'v1',
                    climate_data.ST_GeomFromText(
                        'MULTIPOLYGON Z (((95 2 0, 96 2 0, 96 3 0, 95 3 0, 95 2 0)))', 4326
                    )
                ) RETURNING id
            """), {"source": _SOURCE}).scalar_one()

            district_id = conn.execute(text("""
                INSERT INTO climate_data.t_district (
                    province_id, district_name, source_dataset, source_version,
                    source_feature_ref, boundary_geometry
                ) VALUES (
                    :province, 'Integration District', :source, 'v1', 'fixture-1',
                    climate_data.ST_GeomFromText(
                        'MULTIPOLYGON Z (((95 2 0, 96 2 0, 96 3 0, 95 3 0, 95 2 0)))', 4326
                    )
                ) RETURNING id
            """), {"province": province_id, "source": _SOURCE}).scalar_one()

            parent_id = conn.execute(text("""
                INSERT INTO climate_data.t_daily_climate (
                    request_key, dataset_name, source, start_date, end_date,
                    variable_signature, north_lat, south_lat, east_lon, west_lon,
                    downloaded_at, transformed_artifact_path, transformed_sha256,
                    transformed_at
                ) VALUES (
                    :request_key, 'fixture', 'cds', '2024-05-01', '2024-05-01',
                    'tp', 6, 1, 100, 94, NOW(), 'fixture', 'fixture', NOW()
                ) RETURNING id
            """), {"request_key": _SOURCE}).scalar_one()

            for lon in (95.5, 97.5, 98.5):
                conn.execute(text("""
                    INSERT INTO climate_data.t_daily_climate_detail (
                        daily_climate_id, valid_time, valid_time_local,
                        latitude, longitude, tp_daily_total_mm
                    ) VALUES (
                        :parent, '2024-05-01 00:00:00',
                        '2024-05-01 07:00:00', 2.5, :longitude, 12.0
                    )
                """), {"parent": parent_id, "longitude": lon})

            conn.execute(text("""
                INSERT INTO climate_data.t_grid_region (
                    latitude, longitude, province_id, district_id,
                    match_status, match_count
                ) VALUES
                    (2.5, 95.5, :province, :district, 'matched', 1),
                    (2.5, 97.5, NULL, NULL, 'unmatched', 0)
            """), {"province": province_id, "district": district_id})
        cleanup_ids = (parent_id, province_id)
        yield engine, config, parent_id
    finally:
        if cleanup_ids is not None:
            parent_id, province_id = cleanup_ids
            with engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM climate_data.t_grid_region
                    WHERE latitude = 2.5 AND longitude IN (95.5, 97.5)
                """))
                conn.execute(text("""
                    DELETE FROM climate_data.t_daily_climate
                    WHERE id = :id AND request_key = :source
                """), {"id": parent_id, "source": _SOURCE})
                conn.execute(text("""
                    DELETE FROM climate_data.t_district
                    WHERE province_id = :province AND source_dataset = :source
                """), {"province": province_id, "source": _SOURCE})
                conn.execute(text("""
                    DELETE FROM climate_data.t_province
                    WHERE id = :province AND source_dataset = :source
                """), {"province": province_id, "source": _SOURCE})
        engine.dispose()


def _snapshot(engine, parent_id):
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT v.daily_climate_detail_id, v.longitude, v.grid_region_id,
                   v.match_status, v.match_count, v.province_id,
                   v.province_name, v.district_id, v.district_name
            FROM climate_data.vw_daily_climate_region AS v
            WHERE v.daily_climate_id = :parent
            ORDER BY v.longitude
        """), {"parent": parent_id}).all()


def test_matched_unmatched_unmapped_and_repeated_schema_bootstrap(region_view_db):
    engine, config, parent_id = region_view_db
    first = _snapshot(engine, parent_id)
    assert len(first) == 3
    assert len({row.daily_climate_detail_id for row in first}) == 3

    matched, unmatched, unmapped = first
    assert matched.longitude == 95.5
    assert matched.match_status == "matched" and matched.match_count == 1
    assert matched.grid_region_id is not None
    assert matched.province_name == "Integration Province"
    assert matched.district_name == "Integration District"
    assert matched.province_id is not None and matched.district_id is not None

    assert unmatched.longitude == 97.5
    assert unmatched.match_status == "unmatched" and unmatched.match_count == 0
    assert unmatched.grid_region_id is not None
    assert unmatched.province_id is None and unmatched.district_id is None
    assert unmatched.province_name is None and unmatched.district_name is None

    assert unmapped.longitude == 98.5
    assert unmapped.grid_region_id is None and unmapped.match_status is None
    assert unmapped.province_id is None and unmapped.district_id is None
    assert unmapped.province_name is None and unmapped.district_name is None

    with engine.connect() as conn:
        source_count = conn.execute(text("""
            SELECT COUNT(*) FROM climate_data.t_daily_climate_detail
            WHERE daily_climate_id = :parent
        """), {"parent": parent_id}).scalar_one()
        map_count = conn.execute(text("""
            SELECT COUNT(*) FROM climate_data.t_grid_region
        """)).scalar_one()
    assert source_count == len(first) == 3
    assert map_count == 2

    initialize_geospatial_schema(engine, config)
    initialize_geospatial_schema(engine, config)
    assert _snapshot(engine, parent_id) == first

    with engine.connect() as conn:
        assert conn.execute(text("""
            SELECT COUNT(*) FROM climate_data.t_daily_climate_detail
            WHERE daily_climate_id = :parent
        """), {"parent": parent_id}).scalar_one() == 3
        assert conn.execute(text(
            "SELECT COUNT(*) FROM climate_data.t_grid_region"
        )).scalar_one() == 2
        assert conn.execute(text("""
            SELECT COUNT(*) FROM pg_views
            WHERE schemaname = 'climate_data'
              AND viewname = 'vw_daily_climate_region'
        """)).scalar_one() == 1
