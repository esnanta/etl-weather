"""Test the real geospatial schema builder without connecting to PostgreSQL."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pytest

from geospatial.config import ACEH_DISTRICTS, SPATIAL_SCHEMA
from geospatial.database.schema import create_spatial_tables
from pipeline.database.config import DatabaseConfig


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()

    @contextmanager
    def begin(self):
        yield self.connection


def _config(schema: str = "climate_data") -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost", port=5432, database="weather", user="test",
        password="test", schema=schema,
    )


def test_source_profile_maps_actual_aceh_properties() -> None:
    assert ACEH_DISTRICTS.country_code == "id"
    assert ACEH_DISTRICTS.province_name_property == "WADMPR"
    assert ACEH_DISTRICTS.district_name_property == "WADMKK"
    assert ACEH_DISTRICTS.feature_ref_property == "OBJECTID"
    assert ACEH_DISTRICTS.geometry_dimensions == 3
    assert ACEH_DISTRICTS.source_path.name == "Aceh.geojson"
    assert SPATIAL_SCHEMA == "climate_data"


def test_schema_builder_uses_postgis_and_protects_existing_climate_tables() -> None:
    engine = _Engine()
    with patch(
        "geospatial.database.schema.ensure_postgis_extension", return_value="3.4.2"
    ) as bootstrap:
        create_spatial_tables(engine, _config())

    bootstrap.assert_called_once_with(engine, expected_database="weather")
    sql = "\n".join(engine.connection.statements)
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_province" in sql
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_district" in sql
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_grid_region" in sql
    assert sql.count("boundary_geometry climate_data.geometry(MultiPolygonZ, 4326)") == 2
    assert "boundary_geometry geometry(MultiPolygonZ, 4326)" not in sql
    assert "CHECK (ST_IsValid(" not in sql
    assert "CHECK (NOT ST_IsEmpty(" not in sql
    assert sql.count("climate_data.ST_IsValid(boundary_geometry)") == 2
    assert sql.count("climate_data.ST_IsEmpty(boundary_geometry)") == 2
    assert "source_feature_ref VARCHAR(128)" in sql
    assert "FOREIGN KEY (district_id, province_id)" in sql
    assert "REFERENCES climate_data.t_district(id, province_id)" in sql
    assert "match_status IN ('matched', 'unmatched', 'ambiguous')" in sql
    assert "USING GIST (boundary_geometry)" in sql
    assert "DROP TABLE" not in sql
    assert "t_daily_climate" not in sql


def test_schema_builder_rejects_unexpected_application_schema() -> None:
    engine = _Engine()
    with pytest.raises(ValueError, match="climate_data"):
        create_spatial_tables(engine, _config(schema="public"))
    assert engine.connection.statements == []
