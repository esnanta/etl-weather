"""PostGIS fixture for 0/1/multiple ST_Covers matches and repeated upserts.

Runs only against an explicitly configured, empty TEST_POSTGRES_* database.
"""
from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from database.config import resolve_test_database_config
from database.geospatial import create_spatial_tables
from database.schema import initialize_climate_schema
from geospatial.mapping import map_grid_regions

pytestmark = pytest.mark.integration


@pytest.fixture
def mapping_db():
    load_dotenv()
    if not all(os.getenv("TEST_POSTGRES_" + k) for k in
               ("HOST", "PORT", "DATABASE", "USER", "PASSWORD")):
        pytest.skip("Requires isolated TEST_POSTGRES_* settings")
    config = resolve_test_database_config()
    engine = create_engine(config.sqlalchemy_url, future=True)
    created = False
    try:
        initialize_climate_schema(engine, config)
        create_spatial_tables(engine, config)
        with engine.connect() as connection:
            counts = connection.execute(text("""
                SELECT (SELECT COUNT(*) FROM climate_data.t_daily_climate)
                     + (SELECT COUNT(*) FROM climate_data.t_daily_climate_detail)
                     + (SELECT COUNT(*) FROM climate_data.t_province)
                     + (SELECT COUNT(*) FROM climate_data.t_district)
                     + (SELECT COUNT(*) FROM climate_data.t_grid_region)
            """)).scalar_one()
        if counts:
            pytest.fail("Mapping integration requires empty isolated test tables.")
        created = True
        yield engine, config
    finally:
        if created:
            with engine.begin() as connection:
                connection.execute(text("DELETE FROM climate_data.t_grid_region"))
                connection.execute(text("DELETE FROM climate_data.t_daily_climate_detail"))
                connection.execute(text("DELETE FROM climate_data.t_daily_climate"))
                connection.execute(text("DELETE FROM climate_data.t_district"))
                connection.execute(text("DELETE FROM climate_data.t_province"))
        engine.dispose()


def test_point_inside_outside_boundary_and_repeated_mapping(mapping_db):
    engine, config = mapping_db
    with engine.begin() as conn:
        province_id = conn.execute(text("""
            INSERT INTO climate_data.t_province
                (country_code, province_name, source_dataset, source_version,
                 boundary_geometry)
            VALUES ('id','Fixture','grid_mapping_test','v1',
                climate_data.ST_GeomFromText(
                    'MULTIPOLYGON Z (((95 2 0, 97 2 0, 97 3 0, 95 3 0, 95 2 0)))',4326))
            RETURNING id
        """)).scalar_one()
        for index, (left, right) in enumerate(((95, 96), (96, 97)), 1):
            conn.execute(text("""
                INSERT INTO climate_data.t_district
                    (province_id,district_name,source_dataset,source_version,
                     source_feature_ref,boundary_geometry)
                VALUES (:province,:name,'grid_mapping_test','v1',:ref,
                    climate_data.ST_GeomFromText(:polygon,4326))
            """), {
                "province": province_id, "name": f"District {index}",
                "ref": str(index),
                "polygon": (
                    f"MULTIPOLYGON Z ((({left} 2 0, {right} 2 0, "
                    f"{right} 3 0, {left} 3 0, {left} 2 0)))"
                ),
            })
        parent = conn.execute(text("""
            INSERT INTO climate_data.t_daily_climate
                (request_key,dataset_name,source,start_date,end_date,
                 variable_signature,north_lat,south_lat,east_lon,west_lon,
                 downloaded_at,transformed_artifact_path,transformed_sha256,transformed_at)
            VALUES ('grid-mapping-test','fixture','cds','2024-05-01','2024-05-01',
                    'tp',6,1,100,94,NOW(),'fixture','fixture',NOW())
            RETURNING id
        """)).scalar_one()
        for longitude in (95.5, 96.0, 99.5):
            conn.execute(text("""
                INSERT INTO climate_data.t_daily_climate_detail
                    (daily_climate_id, valid_time, valid_time_local, latitude, longitude)
                VALUES (:parent,'2024-05-01 00:00:00','2024-05-01 07:00:00',2.5,:lon)
            """), {"parent": parent, "lon": longitude})

    preview = map_grid_regions(engine, config)
    assert (preview.coordinates, preview.matched, preview.unmatched, preview.ambiguous) == (
        3, 1, 1, 1
    )
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM climate_data.t_grid_region")).scalar_one() == 0

    first = map_grid_regions(engine, config, execute=True)
    assert first == preview
    with engine.connect() as conn:
        before = conn.execute(text("""
            SELECT longitude, match_status, match_count, province_id, district_id, mapped_at
            FROM climate_data.t_grid_region ORDER BY longitude
        """)).all()
    assert [(r.longitude, r.match_status, r.match_count) for r in before] == [
        (95.5, "matched", 1), (96.0, "ambiguous", 2), (99.5, "unmatched", 0)
    ]
    assert before[0].province_id is not None and before[0].district_id is not None
    assert all(r.province_id is None and r.district_id is None for r in before[1:])

    assert map_grid_regions(engine, config, execute=True) == first
    with engine.connect() as conn:
        after = conn.execute(text("""
            SELECT longitude, match_status, match_count, province_id, district_id, mapped_at
            FROM climate_data.t_grid_region ORDER BY longitude
        """)).all()
    assert before == after
