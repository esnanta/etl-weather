"""Behavioral tests for the production GeoJSON reader and importer."""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from geospatial.config import ACEH_DISTRICTS
from geospatial.importers.reader import normalize_feature_ref, read_district_features
from geospatial.importers.districts import import_districts
from pipeline.database.config import DatabaseConfig


def _fixture(path: Path, *, ident=113.0, district="Aceh Barat", crs=None) -> Path:
    doc = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {
            "name": crs or ACEH_DISTRICTS.source_crs
        }},
        "features": [{
            "type": "Feature",
            "properties": {"OBJECTID": ident, "WADMKK": district, "WADMPR": "Aceh"},
            "geometry": {"type": "MultiPolygon", "coordinates": [
                [[[95, 2, 0], [96, 2, 0], [96, 3, 0], [95, 2, 0]]]
            ]},
        }],
    }
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def test_real_reader_preserves_xyz_and_immutable_input(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "source.geojson")
    before = path.read_bytes()
    rows = read_district_features(ACEH_DISTRICTS, path=path)
    assert len(rows) == 1
    assert rows[0].source_feature_ref == "113"
    assert json.loads(rows[0].geometry_json)["coordinates"][0][0][0] == [95, 2, 0]
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "value,expected",
    [(113, "113"), (113.0, "113"), ("00113", "00113"),
     ("113.0", "113.0"), ("ACEH-113", "ACEH-113")],
)
def test_source_feature_reference_normalization(value, expected) -> None:
    assert normalize_feature_ref(value, numeric_integer=True) == expected


@pytest.mark.parametrize("invalid", [None, True, 113.5, "", " "])
def test_source_feature_reference_rejects_invalid(invalid) -> None:
    with pytest.raises(ValueError):
        normalize_feature_ref(invalid, numeric_integer=True)


def test_source_mapping_can_change_without_importer_changes(tmp_path: Path) -> None:
    source = replace(
        ACEH_DISTRICTS, province_name_property="province",
        district_name_property="district", feature_ref_property="feature",
    )
    path = _fixture(tmp_path / "source.geojson")
    doc = json.loads(path.read_text())
    doc["features"][0]["properties"] = {
        "province": "Aceh", "district": "Aceh Barat", "feature": "00113"
    }
    path.write_text(json.dumps(doc))
    assert read_district_features(source, path=path)[0].source_feature_ref == "00113"


def test_missing_property_and_wrong_crs_fail_before_db_write(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "source.geojson")
    doc = json.loads(path.read_text())
    del doc["features"][0]["properties"]["WADMKK"]
    path.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="missing mapped property"):
        read_district_features(ACEH_DISTRICTS, path=path)
    _fixture(path, crs="EPSG:3857")
    with pytest.raises(ValueError, match="CRS"):
        read_district_features(ACEH_DISTRICTS, path=path)


def test_duplicate_feature_ids_fail(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "source.geojson")
    doc = json.loads(path.read_text())
    second = json.loads(json.dumps(doc["features"][0]))
    second["properties"]["WADMKK"] = "Banda Aceh"
    doc["features"].append(second)
    path.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="duplicate"):
        read_district_features(ACEH_DISTRICTS, path=path)


def test_importer_preflight_validates_before_postgis(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "source.geojson", crs="EPSG:3857")
    db = DatabaseConfig(
        host="localhost", port=5432, database="weather",
        user="test", password="test",
    )
    with patch("geospatial.importers.districts.ensure_postgis_extension") as bootstrap:
        with pytest.raises(ValueError, match="CRS"):
            import_districts(object(), database=db, path=path)
        bootstrap.assert_not_called()

def test_reader_accepts_decimal_geojson_coordinates(tmp_path: Path) -> None:
    """Regression: json.load(parse_float=Decimal) must pass coordinate checks."""
    path = _fixture(tmp_path / "decimal.geojson")
    doc = json.loads(path.read_text())
    doc["features"][0]["geometry"]["coordinates"][0][0] = [
        [95.123456789, 2.123456789, 0.0],
        [96.123456789, 2.123456789, 0.0],
        [96.123456789, 3.123456789, 0.0],
        [95.123456789, 2.123456789, 0.0],
    ]
    path.write_text(json.dumps(doc), encoding="utf-8")
    before = path.read_bytes()
    features = read_district_features(ACEH_DISTRICTS, path=path)
    assert len(features) == 1
    coords = json.loads(features[0].geometry_json)["coordinates"][0][0][0]
    assert coords == [95.123456789, 2.123456789, 0.0]
    assert path.read_bytes() == before

