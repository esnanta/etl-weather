"""Verify pure PostgreSQL-loader data preparation and SQL construction.

These unit tests exercise production loader functions with real weather,
artifact, and database configuration models. A minimal recording connection is
used only to capture SQLAlchemy statements and bound parameters; PostgreSQL
idempotency, conflict handling, unchanged-partition decisions, and transaction
rollback are verified by integration tests against an isolated real database.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pandas as pd
from sqlalchemy.engine import Connection

import pipeline.loaders.daily_climate_postgres as postgres_loader
from pipeline.database import DatabaseConfig
from pipeline.loaders.daily_climate_postgres import (
    upsert_daily_climate_metadata,
    upsert_daily_climate_observations,
)
from pipeline.models.weather import (
    BoundingBox,
    DataLayer,
    DataSource,
    DatasetArtifact,
    TransformedWeatherDataset,
    WeatherRequest,
)

prepare_observation_dataframe = getattr(
    postgres_loader,
    "_prepare_observation_dataframe",
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


class _ScalarResult:
    """Return the one scalar value required by metadata INSERT ... RETURNING."""

    def __init__(self, value: int) -> None:
        self.value = value

    def scalar(self) -> int:
        return self.value


class _RecordingConnection:
    """Capture SQL and parameters without emulating persistence semantics."""

    def __init__(self, *, scalar_value: int = 42) -> None:
        self.scalar_value = scalar_value
        self.statements: list[str] = []
        self.params: list[object] = []

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        self.params.append(params)
        return _ScalarResult(self.scalar_value)


def test_database_config_uses_daily_climate_table_names() -> None:
    config = _config()

    assert config.daily_climate_table == "daily_climate"
    assert config.daily_climate_detail_table == "daily_climate_detail"


def test_prepare_observation_dataframe_uses_daily_database_columns() -> None:
    dataframe, rename_map = prepare_observation_dataframe(
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

    dataframe, _ = prepare_observation_dataframe(dataset, daily_climate_id=7)

    assert dataframe.loc[0, "tp_daily_total_mm"] is None


def test_upsert_daily_climate_metadata_builds_identity_and_provenance(
    monkeypatch,
) -> None:
    connection = _RecordingConnection(scalar_value=42)
    artifact = _artifact()

    monkeypatch.setattr(
        "pipeline.loaders.daily_climate_postgres.read_parquet_provenance",
        lambda _: {
            "raw_artifact_path": "data/raw.nc",
            "raw_sha256": "raw-hash",
            "downloaded_at": "2022-01-02T03:04:05+00:00",
            "transformed_at": _transformed_at().isoformat(),
        },
    )
    monkeypatch.setattr(
        "pipeline.loaders.daily_climate_postgres.artifact_path_for_metadata",
        lambda _: "data/transformed/daily.parquet",
    )
    monkeypatch.setattr(
        "pipeline.loaders.daily_climate_postgres.calculate_file_sha256",
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
    assert "INSERT INTO climate_data.daily_climate" in statement_text
    assert "ON CONFLICT (request_key)" in statement_text
    assert "RETURNING id" in statement_text

    params = connection.params[0]
    assert isinstance(params, dict)
    assert params["request_key"]
    assert params["dataset_name"] == "reanalysis-era5-land"
    assert params["source"] == "cds"
    assert params["downloaded_at"] == datetime(
        2022, 1, 2, 3, 4, 5, tzinfo=UTC
    )
    assert params["transformed_artifact_path"] == "data/transformed/daily.parquet"
    assert params["transformed_sha256"] == "transformed-hash"
    assert params["transformed_at"] == _transformed_at()


def test_upsert_daily_climate_observations_builds_grid_time_conflict_upsert() -> None:
    connection = _RecordingConnection()

    rows_processed = upsert_daily_climate_observations(
        _dataset(),
        daily_climate_id=42,
        connection=cast(Connection, cast(object, connection)),
        config=_config(),
    )

    assert rows_processed == 1
    assert len(connection.statements) == 1
    statement_text = connection.statements[0]
    assert "INSERT INTO climate_data.daily_climate_detail" in statement_text
    assert (
        "ON CONFLICT (daily_climate_id, valid_time, latitude, longitude)"
        in statement_text
    )
    assert "DO UPDATE SET" in statement_text

    records = connection.params[0]
    assert isinstance(records, list)
    assert records[0]["daily_climate_id"] == 42
    assert records[0]["tp_daily_total_mm"] == 10.0
    assert records[0]["valid_time"] == datetime(2021, 12, 31, 17, 0)
    assert records[0]["valid_time_local"] == datetime(2022, 1, 1, 0, 0)


def test_upsert_daily_climate_observations_returns_zero_for_empty_dataset() -> None:
    connection = _RecordingConnection()
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
