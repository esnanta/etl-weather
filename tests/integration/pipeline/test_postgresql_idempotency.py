"""Verify canonical daily-climate persistence against isolated PostgreSQL.

These integration tests exercise the production storage artifacts and
load_to_postgresql() path with a real PostgreSQL database selected only through
TEST_POSTGRES_* settings. They are the authoritative proof for idempotent
repeated loads, transaction rollback, provenance/count mismatch recovery, and
observation UPSERT behavior.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
import pytest
import xarray as xr
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from pipeline.database import DatabaseConfig
from pipeline.loaders.daily_climate_postgres import load_to_postgresql
from pipeline.models.weather import (
    BoundingBox,
    DataSource,
    RawWeatherDataset,
    TransformedWeatherDataset,
    WeatherRequest,
)
from pipeline.storage import (
    calculate_file_sha256,
    save_raw_netcdf,
    save_transformed_parquet,
)

pytestmark = pytest.mark.integration

TEST_POSTGRES_ENV_KEYS = (
    "TEST_POSTGRES_HOST",
    "TEST_POSTGRES_PORT",
    "TEST_POSTGRES_DATABASE",
    "TEST_POSTGRES_USER",
    "TEST_POSTGRES_PASSWORD",
)


def _isolated_database_config() -> DatabaseConfig:
    load_dotenv()
    missing = [key for key in TEST_POSTGRES_ENV_KEYS if not os.getenv(key)]
    if missing:
        pytest.skip(
            "PostgreSQL integration test requires: " + ", ".join(missing)
        )

    database = os.environ["TEST_POSTGRES_DATABASE"]
    if "test" not in database.lower():
        pytest.fail(
            "TEST_POSTGRES_DATABASE must contain 'test' to prevent use of the "
            "development or production database."
        )

    return DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=database,
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema=f"climate_test_{uuid4().hex}",
    )


@pytest.fixture
def postgres_config():
    config = _isolated_database_config()
    yield config

    engine = create_engine(config.url, future=True)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(f'DROP SCHEMA IF EXISTS "{config.schema}" CASCADE')
            )
    finally:
        engine.dispose()


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


def _save_test_partition(tmp_path, monkeypatch, *, invalid_detail: bool = False):
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    request = _request()
    downloaded_at = datetime(2022, 1, 2, 3, 4, 5, tzinfo=UTC)
    transformed_at = datetime(2022, 1, 2, 4, 5, 6, tzinfo=UTC)

    raw_artifact = save_raw_netcdf(
        RawWeatherDataset(
            request=request,
            downloaded_at=downloaded_at,
            raw_data=xr.Dataset(
                {
                    "tp": (
                        ("valid_time", "latitude", "longitude"),
                        [[[0.001]]],
                    )
                },
                coords={
                    "valid_time": pd.to_datetime(["2022-01-01 00:00:00"]),
                    "latitude": [4.5],
                    "longitude": [96.8],
                },
            ),
        )
    )
    dataframe = pd.DataFrame(
        {
            "valid_time": pd.to_datetime(
                ["2021-12-31 17:00:00", "2021-12-31 17:00:00"]
            ),
            "valid_time_local": pd.to_datetime(
                ["2022-01-01 00:00:00", "2022-01-01 00:00:00"]
            ),
            "latitude": [4.5, 4.6],
            "longitude": [96.8, 96.9],
            "tp": [10.0, 11.0],
            "t2m": [26.0, 27.0],
            "swvl1": [0.30, 0.31],
            "ssrd": [12_000_000.0, 13_000_000.0],
            "d2m": [22.0, 23.0],
        }
    )
    if invalid_detail:
        dataframe.loc[0, "valid_time_local"] = pd.NaT

    transformed = TransformedWeatherDataset(
        request=request,
        transformed_at=transformed_at,
        dataset=dataframe,
    )
    transformed_artifact = save_transformed_parquet(
        transformed.dataset,
        request,
        downloaded_at,
        transformed_at,
        raw_artifact,
    )
    return transformed, transformed_artifact


def test_loading_same_partition_twice_does_not_create_duplicates(
    postgres_config,
    tmp_path,
    monkeypatch,
) -> None:
    dataset, artifact = _save_test_partition(tmp_path, monkeypatch)

    first_id, first_rows = load_to_postgresql(
        dataset,
        artifact,
        postgres_config,
    )
    second_id, second_rows = load_to_postgresql(
        dataset,
        artifact,
        postgres_config,
    )

    assert second_id == first_id
    assert first_rows == 2
    assert second_rows == 0

    engine = create_engine(postgres_config.url, future=True)
    try:
        with engine.connect() as connection:
            metadata_count = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_table}"
                )
            ).scalar_one()
            detail_count = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_detail_table}"
                )
            ).scalar_one()
            duplicate_key_count = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT daily_climate_id, valid_time, latitude, longitude
                        FROM {postgres_config.schema}.
                             {postgres_config.daily_climate_detail_table}
                        GROUP BY daily_climate_id, valid_time, latitude, longitude
                        HAVING COUNT(*) > 1
                    ) AS duplicate_keys
                    """
                )
            ).scalar_one()
    finally:
        engine.dispose()

    assert metadata_count == 1
    assert detail_count == 2
    assert duplicate_key_count == 0


def test_detail_failure_rolls_back_metadata_and_observations(
    postgres_config,
    tmp_path,
    monkeypatch,
) -> None:
    dataset, artifact = _save_test_partition(
        tmp_path,
        monkeypatch,
        invalid_detail=True,
    )

    with pytest.raises(IntegrityError) as error:
        load_to_postgresql(dataset, artifact, postgres_config)

    statement = error.value.statement
    assert statement is not None
    assert "daily_climate_detail" in statement

    engine = create_engine(postgres_config.url, future=True)
    try:
        with engine.connect() as connection:
            metadata_count = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_table}"
                )
            ).scalar_one()
            detail_count = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_detail_table}"
                )
            ).scalar_one()
    finally:
        engine.dispose()

    assert metadata_count == 0
    assert detail_count == 0



def test_missing_observation_forces_rewrite_and_restores_row_count(
    postgres_config,
    tmp_path,
    monkeypatch,
) -> None:
    dataset, artifact = _save_test_partition(tmp_path, monkeypatch)
    daily_climate_id, first_rows = load_to_postgresql(
        dataset,
        artifact,
        postgres_config,
    )
    assert first_rows == 2

    engine = create_engine(postgres_config.url, future=True)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_detail_table} "
                    "WHERE daily_climate_id = :daily_climate_id "
                    "AND latitude = :latitude"
                ),
                {"daily_climate_id": daily_climate_id, "latitude": 4.6},
            )

        repeated_id, rewritten_rows = load_to_postgresql(
            dataset,
            artifact,
            postgres_config,
        )

        with engine.connect() as connection:
            detail_count = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_detail_table} "
                    "WHERE daily_climate_id = :daily_climate_id"
                ),
                {"daily_climate_id": daily_climate_id},
            ).scalar_one()
    finally:
        engine.dispose()

    assert repeated_id == daily_climate_id
    assert rewritten_rows == 2
    assert detail_count == 2


def test_provenance_mismatch_forces_upsert_and_restores_canonical_values(
    postgres_config,
    tmp_path,
    monkeypatch,
) -> None:
    dataset, artifact = _save_test_partition(tmp_path, monkeypatch)
    daily_climate_id, first_rows = load_to_postgresql(
        dataset,
        artifact,
        postgres_config,
    )
    assert first_rows == 2

    engine = create_engine(postgres_config.url, future=True)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"UPDATE {postgres_config.schema}."
                    f"{postgres_config.daily_climate_table} "
                    "SET transformed_artifact_path = 'stale/path.parquet', "
                    "transformed_sha256 = 'stale-sha' "
                    "WHERE id = :daily_climate_id"
                ),
                {"daily_climate_id": daily_climate_id},
            )
            connection.execute(
                text(
                    f"UPDATE {postgres_config.schema}."
                    f"{postgres_config.daily_climate_detail_table} "
                    "SET tp_daily_total_mm = -999 "
                    "WHERE daily_climate_id = :daily_climate_id"
                ),
                {"daily_climate_id": daily_climate_id},
            )

        repeated_id, rewritten_rows = load_to_postgresql(
            dataset,
            artifact,
            postgres_config,
        )

        with engine.connect() as connection:
            metadata = connection.execute(
                text(
                    f"SELECT transformed_artifact_path, transformed_sha256 "
                    f"FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_table} "
                    "WHERE id = :daily_climate_id"
                ),
                {"daily_climate_id": daily_climate_id},
            ).one()
            precipitation = connection.execute(
                text(
                    f"SELECT tp_daily_total_mm "
                    f"FROM {postgres_config.schema}."
                    f"{postgres_config.daily_climate_detail_table} "
                    "WHERE daily_climate_id = :daily_climate_id "
                    "ORDER BY latitude"
                ),
                {"daily_climate_id": daily_climate_id},
            ).scalars().all()
    finally:
        engine.dispose()

    assert repeated_id == daily_climate_id
    assert rewritten_rows == 2
    assert metadata[0].endswith(".parquet")
    assert metadata[0] != "stale/path.parquet"
    assert metadata[1] == calculate_file_sha256(artifact.path)
    assert precipitation == [10.0, 11.0]


def test_database_enforces_detail_fk_uniqueness_and_cascade(
    postgres_config,
    tmp_path,
    monkeypatch,
) -> None:
    """Exercise actual PostgreSQL constraints created by the production loader."""
    dataset, artifact = _save_test_partition(tmp_path, monkeypatch)
    parent_id, inserted = load_to_postgresql(dataset, artifact, postgres_config)
    assert inserted == 2

    engine = create_engine(postgres_config.url, future=True)
    detail = (
        f"{postgres_config.schema}.{postgres_config.daily_climate_detail_table}"
    )
    parent = f"{postgres_config.schema}.{postgres_config.daily_climate_table}"
    try:
        with engine.connect() as connection:
            original = connection.execute(
                text(
                    f"SELECT valid_time, valid_time_local, latitude, longitude "
                    f"FROM {detail} WHERE daily_climate_id = :id "
                    f"ORDER BY latitude LIMIT 1"
                ),
                {"id": parent_id},
            ).one()

        def insert_observation(connection, target_id: int) -> None:
            connection.execute(
                text(
                    f"INSERT INTO {detail} "
                    "(daily_climate_id, valid_time, valid_time_local, latitude, longitude) "
                    "VALUES (:id, :valid_time, :valid_time_local, :latitude, :longitude)"
                ),
                {
                    "id": target_id,
                    "valid_time": original.valid_time,
                    "valid_time_local": original.valid_time_local,
                    "latitude": original.latitude,
                    "longitude": original.longitude,
                },
            )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                insert_observation(connection, parent_id + 10_000_000)

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                insert_observation(connection, parent_id)

        connection = engine.connect()
        transaction = connection.begin()
        try:
            connection.execute(
                text(f"DELETE FROM {parent} WHERE id = :id"),
                {"id": parent_id},
            )
            assert connection.execute(
                text(f"SELECT COUNT(*) FROM {detail} WHERE daily_climate_id = :id"),
                {"id": parent_id},
            ).scalar_one() == 0
        finally:
            transaction.rollback()
            connection.close()

        with engine.connect() as connection:
            assert connection.execute(
                text(f"SELECT COUNT(*) FROM {detail} WHERE daily_climate_id = :id"),
                {"id": parent_id},
            ).scalar_one() == 2
    finally:
        engine.dispose()
