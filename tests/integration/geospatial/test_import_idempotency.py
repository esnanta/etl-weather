"""Real PostgreSQL coverage of production geospatial importer persistence.

The production create_spatial_tables() and import_districts() functions execute
against TEST_POSTGRES_* only. A unique throwaway schema isolates this suite
from weather.climate_data and other tests. A progress callback injects failure
after a real district UPSERT to prove transaction rollback.
"""
from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import geospatial.database.schema as spatial_schema
import geospatial.importers.districts as district_importer
from geospatial.config import ACEH_DISTRICTS
from pipeline.database.config import DatabaseConfig

pytestmark = pytest.mark.integration


@pytest.fixture
def spatial_db(monkeypatch):
    load_dotenv()
    keys = (
        "TEST_POSTGRES_HOST", "TEST_POSTGRES_PORT", "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER", "TEST_POSTGRES_PASSWORD",
    )
    missing = [key for key in keys if not os.getenv(key)]
    if missing:
        pytest.skip("Requires isolated " + ", ".join(missing))

    name = os.environ["TEST_POSTGRES_DATABASE"]
    if "test" not in name.lower() or name.lower() in {"weather", "postgres"}:
        pytest.fail("Refusing non-test database for geospatial integration.")
    schema = "geospatial_test_" + uuid4().hex
    config = DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=name,
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema=schema,
    )
    engine = create_engine(config.url, future=True)
    # The extension is global within this test database, not per temporary schema.
    # Fail before any test schema DDL, rather than repeatedly raising from bootstrap.
    try:
        with engine.connect() as connection:
            postgis_schema = connection.execute(text(
                "SELECT n.nspname FROM pg_extension e "
                "JOIN pg_namespace n ON n.oid = e.extnamespace "
                "WHERE e.extname = 'postgis'"
            )).scalar_one_or_none()
        if postgis_schema not in (None, "climate_data"):
            pytest.fail(
                f"TEST_POSTGRES_DATABASE={name!r} has PostGIS in "
                f"{postgis_schema!r}, but the project requires 'climate_data'. "
                "Prepare a dedicated test database with PostGIS installed in "
                "'climate_data' before running integration tests. "
                "Do not relocate or drop an existing extension automatically.",
                pytrace=False,
            )
    except BaseException:
        engine.dispose()
        raise

    # Only the schema location is isolated. The actual production SQL and
    # transaction control remain fully exercised by PostgreSQL and PostGIS.
    monkeypatch.setattr(spatial_schema, "APPLICATION_SCHEMA", schema)
    monkeypatch.setattr(district_importer, "APPLICATION_SCHEMA", schema)
    try:
        spatial_schema.create_spatial_tables(engine, config)
        yield engine, config
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
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
            f"SELECT id, climate_data.ST_AsEWKT(boundary_geometry), climate_data.ST_NDims(boundary_geometry) "
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
