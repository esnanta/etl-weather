"""Daily region view contracts without a live PostgreSQL connection."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pytest

from database.region_views import (
    _DAILY_REGION_VIEW_QUERY,
    _REQUIRED_TABLES,
    create_daily_region_view,
)
from database.schema import initialize_geospatial_schema


class _Scalar:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _Connection:
    def __init__(self, missing=()):
        self.missing = set(missing)
        self.statements = []

    def execute(self, sql, params=None):
        statement = str(sql)
        self.statements.append((statement, params))
        if "to_regclass" in statement:
            name = params["relation"].split(".")[-1]
            return _Scalar(None if name in self.missing else name)
        return _Scalar(None)


class _Engine:
    def __init__(self, missing=()):
        self.connection = _Connection(missing)

    @contextmanager
    def begin(self):
        yield self.connection


def test_daily_view_preserves_all_observations_by_left_join():
    assert "CREATE OR REPLACE VIEW climate_data.vw_daily_climate_region" in _DAILY_REGION_VIEW_QUERY
    assert "JOIN climate_data.t_daily_climate AS c" in _DAILY_REGION_VIEW_QUERY
    assert _DAILY_REGION_VIEW_QUERY.count("LEFT JOIN") == 3
    assert "ON g.latitude = d.latitude" in _DAILY_REGION_VIEW_QUERY
    assert "AND g.longitude = d.longitude" in _DAILY_REGION_VIEW_QUERY
    assert "GROUP BY" not in _DAILY_REGION_VIEW_QUERY
    assert "INSERT INTO" not in _DAILY_REGION_VIEW_QUERY


def test_daily_view_checks_dependencies_and_creates_view():
    engine = _Engine()
    create_daily_region_view(engine)
    statements = engine.connection.statements
    assert sum("to_regclass" in sql for sql, _ in statements) == len(_REQUIRED_TABLES)
    assert any("CREATE OR REPLACE VIEW climate_data.vw_daily_climate_region" in sql
               for sql, _ in statements)


def test_daily_view_refuses_missing_climate_tables():
    engine = _Engine({"t_daily_climate_detail"})
    with pytest.raises(RuntimeError, match="t_daily_climate_detail"):
        create_daily_region_view(engine)
    assert not any("CREATE OR REPLACE VIEW" in sql for sql, _ in engine.connection.statements)


def test_geospatial_initializer_orders_tables_before_view():
    engine = _Engine()
    with (
        patch("database.geospatial.create_spatial_tables") as tables,
        patch("database.region_views.create_daily_region_view") as view,
    ):
        initialize_geospatial_schema(engine)
    tables.assert_called_once_with(engine, None)
    view.assert_called_once_with(engine)
