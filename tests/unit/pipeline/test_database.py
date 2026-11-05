"""Verify canonical PostgreSQL schema DDL emitted by production schema builders.

These unit tests execute the production create_schema/create-table functions
through a minimal recording engine and assert the generated SQL contract only.
Actual PostgreSQL DDL execution and repeated bootstrap behavior are verified by
integration tests, so this module does not emulate database semantics.
"""

from typing import cast

from sqlalchemy.engine import Engine

from database.climate import (
    DAILY_CLIMATE_COLUMNS,
    _DAILY_DETAIL_STRUCTURAL_COLUMNS,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)


from database.config import (
    DatabaseConfig,
    TABLE_PREFIX,
    VIEW_PREFIX,
    table_name,
    view_name,
    GRID_REGION_TABLE,
    PIPELINE_PROGRESS_TABLE,
    PIPELINE_PROGRESS_DETAIL_TABLE,
    PACING_TABLE,
    DAILY_CLIMATE_REGION_VIEW,
    MONTHLY_CLIMATE_REGION_VIEW,
)

def _config() -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="weather",
        user="weather",
        password="secret",
    )


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return iter(self.rows)


class _Connection:
    def __init__(self, rows=None):
        self.statements: list[str] = []
        self.rows = rows if rows is not None else [
            {"column_name": name, "data_type": kind, "is_nullable": nullable}
            for name, (kind, nullable) in _DAILY_DETAIL_STRUCTURAL_COLUMNS.items()
        ] + [
            {"column_name": name, "data_type": "double precision", "is_nullable": "YES"}
            for name in DAILY_CLIMATE_COLUMNS
        ]

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        if "information_schema.columns" in str(statement):
            return _Rows(self.rows)
        return None


class _BeginContext:
    def __init__(self, connection: _Connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def __init__(self, rows=None):
        self.connection = _Connection(rows)

    def begin(self):
        return _BeginContext(self.connection)


def test_create_schema_uses_canonical_climate_schema() -> None:
    engine = _Engine()

    create_schema(cast(Engine, cast(object, engine)), _config())

    assert engine.connection.statements == [
        "CREATE SCHEMA IF NOT EXISTS climate_data"
    ]


def test_daily_climate_table_protects_request_identity_and_provenance() -> None:
    engine = _Engine()

    create_daily_climate_table(cast(Engine, cast(object, engine)), _config())

    sql = "\n".join(engine.connection.statements)
    assert "CREATE TYPE climate_data.data_source_enum" in sql
    assert "AS ENUM ('cds', 'other')" in sql
    assert "'arco'" not in sql
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_daily_climate" in sql
    assert "request_key TEXT NOT NULL" in sql
    assert "variable_signature TEXT NOT NULL" in sql
    assert "north_lat DOUBLE PRECISION NOT NULL" in sql
    assert "downloaded_at TIMESTAMPTZ NOT NULL" in sql
    assert "transformed_artifact_path TEXT NOT NULL" in sql
    assert "transformed_sha256 TEXT NOT NULL" in sql
    assert "transformed_at TIMESTAMPTZ NOT NULL" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_t_daily_climate_request_key" in sql
    assert "ON climate_data.t_daily_climate (request_key)" in sql


def test_daily_climate_detail_table_protects_fk_and_observation_identity() -> None:
    engine = _Engine()

    create_daily_climate_detail_table(cast(Engine, cast(object, engine)), _config())

    sql = "\n".join(engine.connection.statements)
    assert "CREATE TABLE IF NOT EXISTS climate_data.t_daily_climate_detail" in sql
    assert "daily_climate_id BIGINT NOT NULL" in sql
    assert "valid_time TIMESTAMP NOT NULL" in sql
    assert "valid_time_local TIMESTAMP NOT NULL" in sql
    assert "tp_daily_total_mm DOUBLE PRECISION" in sql
    assert "t2m_daily_mean_c DOUBLE PRECISION" in sql
    assert "d2m_daily_mean_c DOUBLE PRECISION" in sql
    assert "swvl1_daily_mean_m3_m3 DOUBLE PRECISION" in sql
    assert "ssrd_daily_total_j_m2 DOUBLE PRECISION" in sql
    for column in DAILY_CLIMATE_COLUMNS:
        assert f"{column} DOUBLE PRECISION" in sql
    assert "information_schema.columns" in sql
    assert "FOREIGN KEY (daily_climate_id)" in sql
    assert "REFERENCES climate_data.t_daily_climate(id)" in sql
    assert "ON DELETE CASCADE" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_t_daily_climate_detail_grid_time" in sql
    assert (
        "ON climate_data.t_daily_climate_detail "
        "(daily_climate_id, valid_time, latitude, longitude)" in sql
    )


def test_canonical_database_names_are_owned_by_database_config() -> None:
    """Resolve physical table and view identifiers using production configuration."""
    config = _config()
    assert TABLE_PREFIX == "t_"
    assert VIEW_PREFIX == "vw_"
    assert config.daily_climate_table == table_name("daily_climate")
    assert config.daily_climate_detail_table == table_name("daily_climate_detail")
    assert GRID_REGION_TABLE == table_name("grid_region")
    assert PIPELINE_PROGRESS_TABLE == table_name("pipeline_progress")
    assert PIPELINE_PROGRESS_DETAIL_TABLE == table_name("pipeline_progress_detail")
    assert PACING_TABLE == table_name("submission_pacing")
    assert DAILY_CLIMATE_REGION_VIEW == view_name("daily_climate_region")
    assert MONTHLY_CLIMATE_REGION_VIEW == view_name("monthly_climate_region")


def test_missing_observation_columns_emit_only_additive_ddl() -> None:
    from database.climate import _converge_daily_climate_detail_columns
    rows = [
        {"column_name": name, "data_type": kind, "is_nullable": nullable}
        for name, (kind, nullable) in _DAILY_DETAIL_STRUCTURAL_COLUMNS.items()
    ] + [
        {"column_name": name, "data_type": "double precision", "is_nullable": "YES"}
        for name in DAILY_CLIMATE_COLUMNS[:5]
    ]
    engine = _Engine(rows)
    _converge_daily_climate_detail_columns(engine.connection, _config())
    alters = [statement for statement in engine.connection.statements if statement.startswith("ALTER TABLE")]
    assert alters == [
        f"ALTER TABLE climate_data.t_daily_climate_detail ADD COLUMN {column} DOUBLE PRECISION"
        for column in DAILY_CLIMATE_COLUMNS[5:]
    ]


def test_compatible_columns_require_no_additive_ddl() -> None:
    from database.climate import _converge_daily_climate_detail_columns
    engine = _Engine()
    _converge_daily_climate_detail_columns(engine.connection, _config())
    assert not any(statement.startswith("ALTER TABLE") for statement in engine.connection.statements)


def test_incompatible_observation_type_fails_explicitly() -> None:
    import pytest
    from database.climate import _converge_daily_climate_detail_columns
    engine = _Engine()
    engine.connection.rows[-1]["data_type"] = "text"
    with pytest.raises(RuntimeError, match="Incompatible.*wind_direction_daily_deg"):
        _converge_daily_climate_detail_columns(engine.connection, _config())


def test_missing_structural_column_fails_explicitly() -> None:
    import pytest
    from database.climate import _converge_daily_climate_detail_columns
    engine = _Engine()
    engine.connection.rows = [
        row for row in engine.connection.rows if row["column_name"] != "valid_time"
    ]
    with pytest.raises(RuntimeError, match="structural column.*valid_time"):
        _converge_daily_climate_detail_columns(engine.connection, _config())
