"""Real PostgreSQL coverage of production geospatial importer persistence.

The production create_spatial_tables() and import_districts() functions execute
against TEST_POSTGRES_* only. The dedicated TEST_POSTGRES_* database uses canonical climate_data schema.
Fixtures refuse to touch pre-existing spatial rows and clean up only test data. A progress callback injects failure
after a real district UPSERT to prove transaction rollback.
"""
from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from database.geospatial import create_spatial_tables
from database.config import DatabaseConfig, APPLICATION_SCHEMA, resolve_test_database_config
import geospatial.importers.districts as district_importer
from geospatial.config import ACEH_DISTRICTS

pytestmark = pytest.mark.integration


@pytest.fixture
def spatial_db():
    """Exercise the canonical spatial schema only in a dedicated empty test DB."""
    load_dotenv()
    keys = (
        "TEST_POSTGRES_HOST", "TEST_POSTGRES_PORT", "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER", "TEST_POSTGRES_PASSWORD",
    )
    missing = [key for key in keys if not os.getenv(key)]
    if missing:
        pytest.skip("Requires isolated " + ", ".join(missing))

    config = resolve_test_database_config()
    engine = create_engine(config.sqlalchemy_url, future=True)
    ready = False
    try:
        with engine.connect() as connection:
            actual = connection.execute(text("SELECT current_database()")).scalar_one()
            if actual != config.database:
                pytest.fail("Refusing unexpected test database identity.")
            postgis_schema = connection.execute(text(
                "SELECT n.nspname FROM pg_extension e "
                "JOIN pg_namespace n ON n.oid = e.extnamespace "
                "WHERE e.extname = 'postgis'"
            )).scalar_one_or_none()
            if postgis_schema not in (None, APPLICATION_SCHEMA):
                pytest.fail(
                    f"PostGIS is installed in {postgis_schema!r}; "
                    f"expected {APPLICATION_SCHEMA!r}. No automatic relocation.",
                    pytrace=False,
                )
        create_spatial_tables(engine, config)
        with engine.connect() as connection:
            existing = (
                connection.execute(text(
                    "SELECT COUNT(*) FROM climate_data.t_province"
                )).scalar_one(),
                connection.execute(text(
                    "SELECT COUNT(*) FROM climate_data.t_district"
                )).scalar_one(),
                connection.execute(text(
                    "SELECT COUNT(*) FROM climate_data.t_grid_region"
                )).scalar_one(),
            )
        if any(existing):
            pytest.fail(
                "Geospatial integration requires empty spatial tables in "
                "dedicated TEST_POSTGRES_* database; refusing to modify existing rows.",
                pytrace=False,
            )
        ready = True
        yield engine, config
    finally:
        if ready:
            # Only rows created by this fixture's test source; never drop schema
            # or truncate tables owned by another integration suite.
            with engine.begin() as connection:
                connection.execute(text(
                    "DELETE FROM climate_data.t_district "
                    "WHERE source_dataset = :source"
                ), {"source": ACEH_DISTRICTS.source_dataset})
                connection.execute(text(
                    "DELETE FROM climate_data.t_province "
                    "WHERE source_dataset = :source"
                ), {"source": ACEH_DISTRICTS.source_dataset})
        engine.dispose()


def _geojson(path: Path, *, names: tuple[str, ...]) -> Path:
    features = []
    for i, district in enumerate(names):
        start = 95.0 + (i * 0.2)
        features.append({
            "type": "Feature",
            "properties": {
                "OBJECTID": float(100 + i),
                "WADMPR": "Aceh", "WADMKK": district,
            },
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [[[
                    [start, 2.0, 0.0], [start + 0.1, 2.0, 0.0],
                    [start + 0.1, 2.1, 0.0], [start, 2.0, 0.0],
                ]]],
            },
        })
    payload = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": ACEH_DISTRICTS.source_crs}},
        "features": features,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _rows(engine, schema: str):
    with engine.connect() as conn:
        province = conn.execute(text(
            f"SELECT id, climate_data.ST_AsEWKT(boundary_geometry), climate_data.climate_data.ST_NDims(boundary_geometry) "
            f"FROM {schema}.t_province ORDER BY id"
        )).all()
        districts = conn.execute(text(
            f"SELECT id, district_name, source_feature_ref, "
            f"climate_data.ST_AsEWKT(boundary_geometry), ST_NDims(boundary_geometry) "
            f"FROM {schema}.t_district ORDER BY source_feature_ref"
        )).all()
    return province, districts


def test_repeat_import_preserves_ids_counts_and_xyz(spatial_db, tmp_path):
    engine, cfg = spatial_db
    path = _geojson(tmp_path / "two.geojson", names=("Aceh Barat", "Banda Aceh"))
    source = replace(ACEH_DISTRICTS, source_version="test-fixture-v1")
    before_bytes = path.read_bytes()

    first = district_importer.import_districts(
        engine, database=cfg, source=source, path=path
    )
    first_rows = _rows(engine, cfg.schema)
    second = district_importer.import_districts(
        engine, database=cfg, source=source, path=path
    )
    second_rows = _rows(engine, cfg.schema)

    assert first.features == second.features == 2
    assert first.provinces == second.provinces == 1
    assert first.source_sha256 == second.source_sha256
    assert path.read_bytes() == before_bytes
    assert first_rows == second_rows
    provinces, districts = second_rows
    assert len(provinces) == 1
    assert len(districts) == 2
    assert all(row[-1] == 3 and " 0" in row[-2] for row in districts)
    assert provinces[0][-1] == 3


def test_partial_import_preserves_other_districts_and_province(spatial_db, tmp_path):
    engine, cfg = spatial_db
    full = _geojson(tmp_path / "full.geojson", names=("Aceh Barat", "Banda Aceh"))
    source = replace(ACEH_DISTRICTS, source_version="test-fixture-v1")
    district_importer.import_districts(engine, database=cfg, source=source, path=full)
    before_province, before_districts = _rows(engine, cfg.schema)

    partial = _geojson(tmp_path / "partial.geojson", names=("Aceh Barat",))
    district_importer.import_districts(engine, database=cfg, source=source, path=partial)

    after_province, after_districts = _rows(engine, cfg.schema)
    assert len(after_districts) == 2
    assert after_province == before_province
    assert after_districts == before_districts


def test_failed_mid_import_rolls_back_all_rows(spatial_db, tmp_path):
    engine, cfg = spatial_db
    path = _geojson(tmp_path / "two.geojson", names=("Aceh Barat", "Banda Aceh"))
    source = replace(ACEH_DISTRICTS, source_version="test-fixture-v1")

    def fail_after_first(completed: int, total: int) -> None:
        assert total == 2
        if completed == 1:
            raise RuntimeError("Injected failure after first persisted district")

    with pytest.raises(RuntimeError, match="Injected failure"):
        district_importer.import_districts(
            engine, database=cfg, source=source, path=path, progress=fail_after_first
        )

    provinces, districts = _rows(engine, cfg.schema)
    assert provinces == []
    assert districts == []

    recovered = district_importer.import_districts(
        engine, database=cfg, source=source, path=path
    )
    assert recovered.features == 2
    assert len(_rows(engine, cfg.schema)[1]) == 2


def test_failed_retry_rolls_back_updates_to_existing_rows(spatial_db, tmp_path):
    engine, cfg = spatial_db
    path = _geojson(tmp_path / "two.geojson", names=("Aceh Barat", "Banda Aceh"))
    source = replace(ACEH_DISTRICTS, source_version="test-fixture-v1")
    district_importer.import_districts(engine, database=cfg, source=source, path=path)
    original = _rows(engine, cfg.schema)

    payload = json.loads(path.read_text())
    payload["features"][0]["geometry"]["coordinates"][0][0][1][0] += 0.01
    path.write_text(json.dumps(payload))
    def fail_after_first(completed: int, total: int) -> None:
        if completed == 1:
            raise RuntimeError("Injected failed retry")

    with pytest.raises(RuntimeError, match="failed retry"):
        district_importer.import_districts(
            engine, database=cfg, source=source, path=path, progress=fail_after_first
        )
    assert _rows(engine, cfg.schema) == original
