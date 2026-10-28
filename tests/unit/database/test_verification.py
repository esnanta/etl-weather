"""Verify read-only seven-table readiness using an isolated SQLAlchemy boundary.

The fake connection only supplies catalog responses. Integration tests own real
PostgreSQL catalog, transaction, and DDL behavior.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from database.config import (
    APPLICATION_SCHEMA,
    MONTHLY_CLIMATE_TABLE,
    MONTHLY_CLIMATE_DETAIL_TABLE,
)
from database.verification import (
    CLIMATE_TABLES,
    GEOSPATIAL_TABLES,
    inspect_database,
    require_schema_ready,
)


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value

    def scalar_one_or_none(self):
        return self.value

    def scalars(self):
        return iter(self.value)


class _Connection:
    def __init__(self, tables):
        self.tables = tables
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "current_database()" in sql:
            return _Result("weather_test")
        if "pg_extension" in sql:
            return _Result(APPLICATION_SCHEMA)
        if "pg_namespace" in sql:
            return _Result(1)
        if "information_schema.tables" in sql:
            assert params == {"schema": APPLICATION_SCHEMA}
            return _Result(self.tables)
        raise AssertionError(f"Unexpected SQL: {sql}")


class _Engine:
    def __init__(self, tables):
        self.connection_obj = _Connection(tables)

    @contextmanager
    def connect(self):
        yield self.connection_obj


@pytest.mark.parametrize("missing", [MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE])
def test_missing_monthly_table_fails_database_readiness(missing):
    engine = _Engine(CLIMATE_TABLES - {missing})
    result = inspect_database(engine)
    assert result["climate_tables_missing"] == (missing,)
    with pytest.raises(RuntimeError, match=missing):
        require_schema_ready(engine)
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in engine.connection_obj.statements)


def test_all_seven_climate_tables_are_ready_without_postgis_requirement():
    assert len(CLIMATE_TABLES) == 7
    engine = _Engine(CLIMATE_TABLES)
    require_schema_ready(engine)
    assert inspect_database(engine)["climate_tables_missing"] == ()


def test_geospatial_readiness_remains_independent():
    engine = _Engine(CLIMATE_TABLES | GEOSPATIAL_TABLES)
    require_schema_ready(engine, geospatial=True)
    assert inspect_database(engine)["geospatial_tables_missing"] == ()
