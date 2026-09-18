from pipeline.database import (
    DatabaseConfig,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)


def _config() -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="weather",
        user="weather",
        password="secret",
    )


class _Connection:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))
        return None


class _BeginContext:
    def __init__(self, connection: _Connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def __init__(self):
        self.connection = _Connection()

    def begin(self):
        return _BeginContext(self.connection)


def test_create_schema_uses_canonical_climate_schema() -> None:
    engine = _Engine()

    create_schema(engine, _config())

    assert engine.connection.statements == [
        "CREATE SCHEMA IF NOT EXISTS climate_data"
    ]


def test_daily_climate_table_protects_request_identity_and_provenance() -> None:
    engine = _Engine()

    create_daily_climate_table(engine, _config())

    sql = "\n".join(engine.connection.statements)
    assert "CREATE TYPE climate_data.data_source_enum" in sql
    assert "CREATE TABLE IF NOT EXISTS climate_data.daily_climate" in sql
    assert "request_key TEXT NOT NULL" in sql
    assert "variable_signature TEXT NOT NULL" in sql
    assert "north_lat DOUBLE PRECISION NOT NULL" in sql
    assert "downloaded_at TIMESTAMPTZ NOT NULL" in sql
    assert "transformed_artifact_path TEXT NOT NULL" in sql
    assert "transformed_sha256 TEXT NOT NULL" in sql
    assert "transformed_at TIMESTAMPTZ NOT NULL" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_climate_request_key" in sql
    assert "ON climate_data.daily_climate (request_key)" in sql


def test_daily_climate_detail_table_protects_fk_and_observation_identity() -> None:
    engine = _Engine()

    create_daily_climate_detail_table(engine, _config())

    sql = "\n".join(engine.connection.statements)
    assert "CREATE TABLE IF NOT EXISTS climate_data.daily_climate_detail" in sql
    assert "daily_climate_id BIGINT NOT NULL" in sql
    assert "valid_time TIMESTAMP NOT NULL" in sql
    assert "valid_time_local TIMESTAMP NOT NULL" in sql
    assert "tp_daily_total_mm DOUBLE PRECISION" in sql
    assert "t2m_daily_mean_c DOUBLE PRECISION" in sql
    assert "d2m_daily_mean_c DOUBLE PRECISION" in sql
    assert "swvl1_daily_mean_m3_m3 DOUBLE PRECISION" in sql
    assert "ssrd_daily_total_j_m2 DOUBLE PRECISION" in sql
    assert "FOREIGN KEY (daily_climate_id)" in sql
    assert "REFERENCES climate_data.daily_climate(id)" in sql
    assert "ON DELETE CASCADE" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_climate_detail_grid_time" in sql
    assert (
        "ON climate_data.daily_climate_detail "
        "(daily_climate_id, valid_time, latitude, longitude)" in sql
    )
