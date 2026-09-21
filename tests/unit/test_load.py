from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from sqlalchemy.engine import Connection

import pandas as pd

from pipeline.database import DatabaseConfig
from pipeline.loaders.postgres import (
    _prepare_observation_dataframe,
    find_unchanged_daily_climate_partition,
    load_to_postgresql,
    upsert_daily_climate_metadata,
    upsert_daily_climate_observations,
)
from pipeline.model import (
    BoundingBox,
    DataLayer,
    DataSource,
    DatasetArtifact,
    TransformedWeatherDataset,
    WeatherRequest,
)


def _request() -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-01",
        variables=["tp", "t2m", "swvl1", "ssrd", "d2m"],
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )


def _transformed_at() -> datetime:
    return datetime(2022, 1, 2, 4, 5, 6, tzinfo=UTC)


def _dataset() -> TransformedWeatherDataset:
    return TransformedWeatherDataset(
        request=_request(),
        transformed_at=_transformed_at(),
        dataset=pd.DataFrame(
            {
                "valid_time": [pd.Timestamp("2021-12-31 17:00:00")],
                "valid_time_local": [pd.Timestamp("2022-01-01 00:00:00")],
                "latitude": [4.5],
                "longitude": [96.8],
                "tp": [10.0],
                "t2m": [26.0],
                "swvl1": [0.3],
                "ssrd": [12_000_000.0],
                "d2m": [22.0],
            }
        ),
    )


def _artifact() -> DatasetArtifact:
    return DatasetArtifact(
        layer=DataLayer.TRANSFORMED,
        created_at=_transformed_at(),
        path=Path("/tmp/data/reanalysis-era5-land/transformed/cds/2022/daily.parquet"),
    )


def _config() -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="weather",
        user="weather",
        password="secret",
    )


def test_database_config_uses_daily_climate_table_names() -> None:
    config = _config()

    assert config.daily_climate_table == "daily_climate"
    assert config.daily_climate_detail_table == "daily_climate_detail"


def test_prepare_observation_dataframe_uses_daily_database_columns() -> None:
    dataframe, rename_map = _prepare_observation_dataframe(
        _dataset(), daily_climate_id=7
    )

    assert rename_map == {
        "tp": "tp_daily_total_mm",
        "t2m": "t2m_daily_mean_c",
        "d2m": "d2m_daily_mean_c",
        "swvl1": "swvl1_daily_mean_m3_m3",
        "ssrd": "ssrd_daily_total_j_m2",
    }
    assert dataframe.loc[0, "daily_climate_id"] == 7
    assert dataframe.loc[0, "tp_daily_total_mm"] == 10.0
    assert dataframe.loc[0, "t2m_daily_mean_c"] == 26.0
    assert "tp" not in dataframe.columns
    assert "t2m" not in dataframe.columns


def test_prepare_observation_dataframe_preserves_missing_as_null() -> None:
    dataset = _dataset()
    dataset.dataset.loc[0, "tp"] = float("nan")

    dataframe, _ = _prepare_observation_dataframe(dataset, daily_climate_id=7)

    assert dataframe.loc[0, "tp_daily_total_mm"] is None


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _MappingResult:
    def __init__(self, value):
        self.value = value

    def mappings(self):
        return self

    def one_or_none(self):
        return self.value


class _CountResult:
    def __init__(self, value: int):
        self.value = value

    def scalar_one(self):
        return self.value


class _PartitionLookupConnection:
    def __init__(self, metadata, database_rows: int):
        self.metadata = metadata
        self.database_rows = database_rows
        self.statements: list[str] = []
        self.params: list[object] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(params)
        if "WHERE request_key = :request_key" in sql:
            return _MappingResult(self.metadata)
        if "WHERE daily_climate_id = :daily_climate_id" in sql:
            return _CountResult(self.database_rows)
        raise AssertionError(f"Unexpected SQL: {sql}")


class _BeginContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class _Engine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return _BeginContext(self.connection)


class _UpsertConnection:
    def __init__(self, daily_climate_id: int = 42):
        self.daily_climate_id = daily_climate_id
        self.statements: list[str] = []
        self.params: list[object] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(params)
        if "INSERT INTO" in sql and "ON CONFLICT (request_key)" in sql:
            return _ScalarResult(self.daily_climate_id)
        if "INSERT INTO" in sql and "ON CONFLICT (daily_climate_id" in sql:
            return None
        raise AssertionError(f"Unexpected SQL: {sql}")


def test_upsert_daily_climate_metadata_uses_request_key_and_provenance(
    monkeypatch,
) -> None:
    connection = _UpsertConnection(daily_climate_id=42)
    artifact = _artifact()

    monkeypatch.setattr(
        "pipeline.loaders.postgres.read_parquet_provenance",
        lambda _: {
            "raw_artifact_path": "data/raw.nc",
            "raw_sha256": "raw-hash",
            "downloaded_at": "2022-01-02T03:04:05+00:00",
            "transformed_at": _transformed_at().isoformat(),
        },
    )
    monkeypatch.setattr(
        "pipeline.loaders.postgres.artifact_path_for_metadata",
        lambda _: "data/transformed/daily.parquet",
    )
    monkeypatch.setattr(
        "pipeline.loaders.postgres.calculate_file_sha256",
        lambda _: "transformed-hash",
    )

    daily_climate_id = upsert_daily_climate_metadata(
        _dataset(),
        artifact,
        cast(Connection, cast(object, connection)),
        _config(),
    )

    assert daily_climate_id == 42
    assert len(connection.statements) == 1
    statement_text = connection.statements[0]
    assert "INSERT INTO" in statement_text
    assert "climate_data.daily_climate" in statement_text
    assert "ON CONFLICT (request_key)" in statement_text
    assert "downloaded_at" in statement_text
    assert "transformed_artifact_path" in statement_text
    assert "transformed_sha256" in statement_text
    assert "RETURNING id" in statement_text

    params = connection.params[0]
    assert params["request_key"]
    assert params["downloaded_at"] == datetime(
        2022, 1, 2, 3, 4, 5, tzinfo=UTC
    )
    assert params["transformed_artifact_path"] == "data/transformed/daily.parquet"
    assert params["transformed_sha256"] == "transformed-hash"
    assert params["transformed_at"] == _transformed_at()


def test_unchanged_partition_matches_provenance_and_observation_count() -> None:
    connection = _PartitionLookupConnection(
        metadata={
            "id": 42,
            "transformed_artifact_path": "data/transformed/daily.parquet",
            "transformed_sha256": "transformed-hash",
        },
        database_rows=1,
    )

    result = find_unchanged_daily_climate_partition(
        request_key="request-key",
        transformed_artifact_path="data/transformed/daily.parquet",
        transformed_sha256="transformed-hash",
        expected_rows=1,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert result == (42, 1)
    assert len(connection.statements) == 2


def test_partition_is_not_skipped_when_checksum_changes() -> None:
    connection = _PartitionLookupConnection(
        metadata={
            "id": 42,
            "transformed_artifact_path": "data/transformed/daily.parquet",
            "transformed_sha256": "old-hash",
        },
        database_rows=1,
    )

    result = find_unchanged_daily_climate_partition(
        request_key="request-key",
        transformed_artifact_path="data/transformed/daily.parquet",
        transformed_sha256="new-hash",
        expected_rows=1,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert result is None
    assert len(connection.statements) == 1


def test_partition_is_not_skipped_when_artifact_path_changes() -> None:
    connection = _PartitionLookupConnection(
        metadata={
            "id": 42,
            "transformed_artifact_path": "data/transformed/old.parquet",
            "transformed_sha256": "transformed-hash",
        },
        database_rows=1,
    )

    result = find_unchanged_daily_climate_partition(
        request_key="request-key",
        transformed_artifact_path="data/transformed/daily.parquet",
        transformed_sha256="transformed-hash",
        expected_rows=1,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert result is None
    assert len(connection.statements) == 1


def test_partition_is_not_skipped_when_observation_count_changes() -> None:
    connection = _PartitionLookupConnection(
        metadata={
            "id": 42,
            "transformed_artifact_path": "data/transformed/daily.parquet",
            "transformed_sha256": "transformed-hash",
        },
        database_rows=0,
    )

    result = find_unchanged_daily_climate_partition(
        request_key="request-key",
        transformed_artifact_path="data/transformed/daily.parquet",
        transformed_sha256="transformed-hash",
        expected_rows=1,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert result is None
    assert len(connection.statements) == 2


def test_load_skips_database_writes_for_unchanged_partition(monkeypatch) -> None:
    connection = _PartitionLookupConnection(
        metadata={
            "id": 42,
            "transformed_artifact_path": "data/transformed/daily.parquet",
            "transformed_sha256": "transformed-hash",
        },
        database_rows=1,
    )
    monkeypatch.setattr(
        "pipeline.loaders.postgres.create_database_engine",
        lambda _: _Engine(connection),
    )
    monkeypatch.setattr("pipeline.loaders.postgres.create_schema", lambda *_: None)
    monkeypatch.setattr("pipeline.loaders.postgres.create_daily_climate_table", lambda *_: None)
    monkeypatch.setattr(
        "pipeline.loaders.postgres.create_daily_climate_detail_table",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "pipeline.loaders.postgres.artifact_path_for_metadata",
        lambda _: "data/transformed/daily.parquet",
    )
    monkeypatch.setattr(
        "pipeline.loaders.postgres.calculate_file_sha256",
        lambda _: "transformed-hash",
    )

    result = load_to_postgresql(_dataset(), _artifact(), _config())

    assert result == (42, 0)
    assert len(connection.statements) == 2
    assert all("INSERT INTO" not in sql for sql in connection.statements)


def test_upsert_daily_climate_observations_uses_grid_time_conflict_key() -> None:
    connection = _UpsertConnection()

    rows_processed = upsert_daily_climate_observations(
        _dataset(),
        daily_climate_id=42,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert rows_processed == 1
    assert len(connection.statements) == 1
    sql = connection.statements[0]
    assert "INSERT INTO climate_data.daily_climate_detail" in sql
    assert (
        "ON CONFLICT (daily_climate_id, valid_time, latitude, longitude)" in sql
    )
    assert "DO UPDATE SET" in sql

    records = connection.params[0]
    assert isinstance(records, list)
    assert records[0]["daily_climate_id"] == 42
    assert records[0]["tp_daily_total_mm"] == 10.0
    assert records[0]["valid_time"] == datetime(2021, 12, 31, 17, 0)
    assert records[0]["valid_time_local"] == datetime(2022, 1, 1, 0, 0)


def test_upsert_daily_climate_observations_returns_zero_for_empty_dataset() -> None:
    connection = _UpsertConnection()
    dataset = _dataset()
    empty_dataset = TransformedWeatherDataset(
        request=dataset.request,
        transformed_at=dataset.transformed_at,
        dataset=dataset.dataset.iloc[0:0].copy(),
    )

    rows_processed = upsert_daily_climate_observations(
        empty_dataset,
        daily_climate_id=42,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert rows_processed == 0
    assert connection.statements == []
