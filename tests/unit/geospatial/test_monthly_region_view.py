"""Prospective monthly SQL contracts, without generating monthly climate values."""
from __future__ import annotations

from contextlib import contextmanager

from database.config import DatabaseConfig
from database.climate import create_monthly_climate_tables
from database.vw_monthly_climate_region import (
    _MONTHLY_REGION_VIEW_QUERY, _REQUIRED_MONTHLY_TABLES,
    create_monthly_region_view,
)


class _Scalar:
    def scalar_one_or_none(self):
        return "present"


class _Connection:
    def __init__(self):
        self.statements = []

    def execute(self, query, params=None):
        self.statements.append((str(query), params))
        return _Scalar()


class _Engine:
    def __init__(self):
        self.connection = _Connection()

    @contextmanager
    def begin(self):
        yield self.connection


def _config():
    return DatabaseConfig(
        host="localhost", port=5432, database="weather_test",
        user="test", password="test",
    )


def test_monthly_tables_have_parent_detail_and_unique_grid_month():
    engine = _Engine()
    create_monthly_climate_tables(engine, _config())
    sql = "\n".join(statement for statement, _ in engine.connection.statements)
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_monthly_climate (" in sql
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_monthly_climate_detail (" in sql
    assert "UNIQUE (monthly_climate_id, month_start, latitude, longitude)" in sql
    assert "ON DELETE CASCADE" in sql
    assert "DROP TABLE" not in sql
    assert "INSERT INTO" not in sql


def test_monthly_view_keeps_unmapped_observations():
    assert "CREATE OR REPLACE VIEW climate_data.vw_monthly_climate_region" in _MONTHLY_REGION_VIEW_QUERY
    assert _MONTHLY_REGION_VIEW_QUERY.count("LEFT JOIN") == 3
    assert "AND g.longitude = d.longitude" in _MONTHLY_REGION_VIEW_QUERY
    assert "GROUP BY" not in _MONTHLY_REGION_VIEW_QUERY
    engine = _Engine()
    create_monthly_region_view(engine)
    assert sum("to_regclass" in s for s, _ in engine.connection.statements) == len(_REQUIRED_MONTHLY_TABLES)
    assert any("CREATE OR REPLACE VIEW climate_data.vw_monthly_climate_region" in s for s, _ in engine.connection.statements)
