"""Protect the grid-to-region matching contract without touching PostgreSQL."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from database.config import DatabaseConfig
from geospatial.mapping import _MAPPING_SQL, _STATUS_SQL, MappingSummary, map_grid_regions


@pytest.fixture
def config():
    return DatabaseConfig(
        host="localhost", port=5432, database="weather", user="test", password="x"
    )


def test_spatial_matching_queries_keep_all_candidates_and_all_source_points():
    for sql in (_STATUS_SQL, _MAPPING_SQL):
        assert "SELECT DISTINCT latitude, longitude" in sql
        assert "ST_Covers(" in sql
        assert "ST_MakePoint(p.longitude, p.latitude)" in sql
        assert "LEFT JOIN climate_data.t_district" in sql
        assert "GROUP BY latitude, longitude" in sql
        assert "COUNT(district_id)" in sql


def test_ambiguous_and_unmatched_never_get_assigned_districts():
    assert "WHEN COUNT(district_id) = 1 THEN MIN(district_id)" in _MAPPING_SQL
    assert "WHEN COUNT(district_id) = 1 THEN MIN(province_id)" in _MAPPING_SQL
    assert "WHEN match_count = 0 THEN 'unmatched'" in _MAPPING_SQL
    assert "ELSE 'ambiguous'" in _MAPPING_SQL


def test_mapping_upsert_preserves_mapped_at_when_classification_unchanged():
    assert "ON CONFLICT (latitude, longitude) DO UPDATE" in _MAPPING_SQL
    assert "IS DISTINCT FROM" in _MAPPING_SQL
    assert "ELSE target.mapped_at" in _MAPPING_SQL


def test_wrong_schema_rejected_before_any_database_operations(config):
    engine = MagicMock()
    wrong = DatabaseConfig(
        host=config.host, port=config.port, database=config.database,
        user=config.user, password=config.password, schema="public"
    )
    with pytest.raises(ValueError, match="climate_data"):
        map_grid_regions(engine, wrong)
    engine.begin.assert_not_called()


def test_summary_is_explicit():
    summary = MappingSummary(5, 2, 1, 2)
    assert summary.coordinates == summary.matched + summary.unmatched + summary.ambiguous
