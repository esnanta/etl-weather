"""Verify orchestration progress semantics against isolated PostgreSQL.

These integration tests are authoritative for persisted-evidence reconciliation,
atomic claims, release behavior, failed-partition recovery, and extraction
attempt barriers. They use only TEST_POSTGRES_* database settings and production
progress repository functions.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from config import (
    PIPELINE_PROGRESS_SCHEMA,
    PIPELINE_PROGRESS_TABLE,
)
from orchestration.progress.definition import PipelineProgressDefinition, ProgressPartition
from orchestration.repositories.pipeline_progress import (
    are_extract_attempts_terminal,
    create_pipeline_progress_tables,
    get_pipeline_progress_by_id,
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
    release_pipeline_progress_claim,
    try_claim_pipeline_progress,
    update_pipeline_progress_detail,
)
from pipeline.database import (
    DatabaseConfig,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)

pytestmark = pytest.mark.integration


def test_completion_requires_matching_persisted_partition() -> None:
    load_dotenv()
    names = (
        "TEST_POSTGRES_HOST",
        "TEST_POSTGRES_PORT",
        "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER",
        "TEST_POSTGRES_PASSWORD",
    )
    if any(not os.getenv(name) for name in names):
        pytest.skip("Isolated TEST_POSTGRES_* connection is required.")
    if "test" not in os.environ["TEST_POSTGRES_DATABASE"].lower():
        pytest.fail("TEST_POSTGRES_DATABASE must name an isolated test database.")

    config = DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=os.environ["TEST_POSTGRES_DATABASE"],
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema="climate_data",
    )
    engine = create_engine(config.url, future=True)
    token = uuid4().hex
    request_key = f"progress-test-{token}"
    definition = PipelineProgressDefinition(
        pipeline_name=f"progress-test-{token}",
        source="cds",
        dataset_name="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-01",
        configuration_identity=token,
        partitions=(ProgressPartition(request_key, "2024-01-01", "2024-01-01"),),
    )
    progress_id = None
    tables_ready = False
    try:
        create_schema(engine, config)
        create_daily_climate_table(engine, config)
        create_daily_climate_detail_table(engine, config)
        create_pipeline_progress_tables(engine)
        tables_ready = True
        progress_id = initialize_pipeline_progress(engine, definition)
        assert progress_id is not None
        assert try_claim_pipeline_progress(engine, progress_id, "scheduled__one")
        update_pipeline_progress_detail(
            engine,
            progress_id,
            request_key,
            extract_status="complete",
            transform_status="complete",
            load_status="complete",
            validation_status="valid",
            transformed_artifact_path="test/partition.parquet",
            transformed_sha256="test-sha",
            row_count=2,
        )

        def status() -> tuple[str, int]:
            with engine.connect() as connection:
                row = connection.execute(
                    text(
                        "SELECT execution_status, completed_partition_count "
                        f"FROM {PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE} "
                        "WHERE id = :id"
                    ),
                    {"id": progress_id},
                ).one()
            return str(row[0]), int(row[1])

        reconcile_pipeline_progress(engine, progress_id)
        assert status() == ("pending", 0)
        assert not release_pipeline_progress_claim(
            engine, progress_id, require_complete=True
        )
        assert (
            get_pipeline_progress_by_id(engine, progress_id).validation_status
            == "pending"
        )

        timestamp = datetime(2024, 1, 2, tzinfo=UTC)
        with engine.begin() as connection:
            daily_climate_id = connection.execute(
                text(
                    """
                    INSERT INTO climate_data.daily_climate
                    (request_key, dataset_name, source, start_date, end_date,
                     variable_signature, north_lat, south_lat, east_lon, west_lon,
                     downloaded_at, transformed_artifact_path, transformed_sha256,
                     transformed_at)
                    VALUES
                    (:key, 'reanalysis-era5-land', 'cds', '2024-01-01',
                     '2024-01-01', 'tp', 6.1, 1.9, 98.3, 94.9,
                     :timestamp, 'test/partition.parquet', 'test-sha',
                     :timestamp)
                    RETURNING id
                    """
                ),
                {"key": request_key, "timestamp": timestamp},
            ).scalar_one()
            for latitude in (4.0, 4.1):
                connection.execute(
                    text(
                        """
                        INSERT INTO climate_data.daily_climate_detail
                        (daily_climate_id, valid_time, valid_time_local,
                         latitude, longitude)
                        VALUES (:id, '2023-12-31 17:00:00',
                                '2024-01-01 00:00:00', :latitude, 96.0)
                        """
                    ),
                    {"id": daily_climate_id, "latitude": latitude},
                )

        reconcile_pipeline_progress(engine, progress_id)
        assert status() == ("complete", 1)
        assert release_pipeline_progress_claim(
            engine, progress_id, require_complete=True
        )
        assert (
            get_pipeline_progress_by_id(engine, progress_id).validation_status
            == "valid"
        )
        completed_at = get_pipeline_progress_by_id(engine, progress_id).completed_at
        reconcile_pipeline_progress(engine, progress_id)
        assert (
            get_pipeline_progress_by_id(engine, progress_id).completed_at
            == completed_at
        )
        assert initialize_pipeline_progress(engine, definition) == progress_id
        assert status() == ("complete", 1)

        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE climate_data.daily_climate "
                    "SET transformed_sha256 = 'different-sha' WHERE id = :id"
                ),
                {"id": daily_climate_id},
            )
        reconcile_pipeline_progress(engine, progress_id)
        assert status() == ("pending", 0)
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE climate_data.daily_climate "
                    "SET transformed_sha256 = 'test-sha' WHERE id = :id"
                ),
                {"id": daily_climate_id},
            )
        reconcile_pipeline_progress(engine, progress_id)
        assert status() == ("complete", 1)

        with engine.begin() as connection:
            connection.execute(
                text(
                    "DELETE FROM climate_data.daily_climate_detail "
                    "WHERE daily_climate_id = :id AND latitude = 4.1"
                ),
                {"id": daily_climate_id},
            )
        reconcile_pipeline_progress(engine, progress_id)
        assert status() == ("pending", 0)
    finally:
        if tables_ready:
            with engine.begin() as connection:
                connection.execute(
                    text("DELETE FROM climate_data.daily_climate WHERE request_key = :key"),
                    {"key": request_key},
                )
                if progress_id is not None:
                    connection.execute(
                        text(
                            f"DELETE FROM {PIPELINE_PROGRESS_SCHEMA}."
                            f"{PIPELINE_PROGRESS_TABLE} WHERE id = :id"
                        ),
                        {"id": progress_id},
                    )
        engine.dispose()


def test_manual_month_repair_completes_only_after_all_three_months() -> None:
    load_dotenv()
    keys = (
        "TEST_POSTGRES_HOST", "TEST_POSTGRES_PORT", "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER", "TEST_POSTGRES_PASSWORD",
    )
    if any(not os.getenv(key) for key in keys):
        pytest.skip("Isolated TEST_POSTGRES_* connection is required.")
    if "test" not in os.environ["TEST_POSTGRES_DATABASE"].lower():
        pytest.fail("TEST_POSTGRES_DATABASE must name an isolated test database.")
    config = DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=os.environ["TEST_POSTGRES_DATABASE"],
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema="climate_data",
    )
    engine = create_engine(config.url, future=True)
    token = uuid4().hex
    months = (
        ("2024-01-01", "2024-01-31"),
        ("2024-02-01", "2024-02-29"),
        ("2024-03-01", "2024-03-31"),
    )
    partitions = tuple(
        ProgressPartition(f"repair-{token}-{number}", start, end)
        for number, (start, end) in enumerate(months, start=1)
    )
    definition = PipelineProgressDefinition(
        pipeline_name=f"repair-test-{token}",
        source="cds",
        dataset_name="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-03-31",
        configuration_identity=token,
        partitions=partitions,
    )
    progress_id = None
    tables_ready = False
    try:
        create_schema(engine, config)
        create_daily_climate_table(engine, config)
        create_daily_climate_detail_table(engine, config)
        create_pipeline_progress_tables(engine)
        tables_ready = True
        progress_id = initialize_pipeline_progress(engine, definition)
        assert progress_id is not None
        claimed_progress_id: int = progress_id
        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(
                executor.map(
                    lambda owner: try_claim_pipeline_progress(
                        engine, claimed_progress_id, owner
                    ),
                    ("scheduled__one", "scheduled__two"),
                )
            )
        assert claims.count(True) == 1
        assert claims.count(False) == 1
        assert release_pipeline_progress_claim(
            engine, progress_id, require_complete=False
        )
        update_pipeline_progress_detail(
            engine, progress_id, partitions[0].request_key,
            extract_status="failed", validation_status="failed",
            last_error="ValidationError",
        )
        reconcile_pipeline_progress(engine, progress_id)
        assert get_pipeline_progress_by_id(
            engine, progress_id
        ).failed_partition_count == 1
        assert not try_claim_pipeline_progress(
            engine, progress_id, "scheduled__next"
        )
        for number, partition in enumerate(partitions, start=1):
            update_pipeline_progress_detail(
                engine, progress_id, partition.request_key,
                extract_status="complete", transform_status="complete",
                load_status="complete", validation_status="valid",
                transformed_artifact_path=f"test/{number}.parquet",
                transformed_sha256=f"sha-{number}", row_count=1,
            )

        def persist_month(number: int) -> None:
            partition = partitions[number - 1]
            timestamp = datetime(2024, 4, 1, tzinfo=UTC)
            local_midnight = datetime.fromisoformat(partition.start_date)
            utc_instant = local_midnight - timedelta(hours=7)
            with engine.begin() as connection:
                metadata_id = connection.execute(
                    text(
                        """
                        INSERT INTO climate_data.daily_climate
                        (request_key, dataset_name, source, start_date, end_date,
                         variable_signature, north_lat, south_lat, east_lon, west_lon,
                         downloaded_at, transformed_artifact_path,
                         transformed_sha256, transformed_at)
                        VALUES (:key, 'reanalysis-era5-land', 'cds', :start, :end,
                                'tp', 6.1, 1.9, 98.3, 94.9, :timestamp,
                                :path, :sha, :timestamp)
                        RETURNING id
                        """
                    ),
                    {
                        "key": partition.request_key,
                        "start": partition.start_date,
                        "end": partition.end_date,
                        "timestamp": timestamp,
                        "path": f"test/{number}.parquet",
                        "sha": f"sha-{number}",
                    },
                ).scalar_one()
                connection.execute(
                    text(
                        """
                        INSERT INTO climate_data.daily_climate_detail
                        (daily_climate_id, valid_time, valid_time_local,
                         latitude, longitude)
                        VALUES (:id, :utc_instant, :local_midnight, 4.0, 96.0)
                        """
                    ),
                    {
                        "id": metadata_id,
                        "utc_instant": utc_instant,
                        "local_midnight": local_midnight,
                    },
                )

        for number in (1, 2, 3):
            persist_month(number)
            reconcile_pipeline_progress(engine, progress_id)
            state = get_pipeline_progress_by_id(engine, progress_id)
            assert state.completed_partition_count == number
            assert state.execution_status == (
                "complete" if number == 3 else "running"
            )
    finally:
        if tables_ready:
            with engine.begin() as connection:
                for partition in partitions:
                    connection.execute(
                        text(
                            "DELETE FROM climate_data.daily_climate "
                            "WHERE request_key = :key"
                        ),
                        {"key": partition.request_key},
                    )
                if progress_id is not None:
                    connection.execute(
                        text(
                            f"DELETE FROM {PIPELINE_PROGRESS_SCHEMA}."
                            f"{PIPELINE_PROGRESS_TABLE} WHERE id = :id"
                        ),
                        {"id": progress_id},
                    )
        engine.dispose()



def test_extract_attempt_barrier_requires_every_expected_partition_terminal() -> None:
    load_dotenv()
    keys = (
        "TEST_POSTGRES_HOST",
        "TEST_POSTGRES_PORT",
        "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER",
        "TEST_POSTGRES_PASSWORD",
    )
    if any(not os.getenv(key) for key in keys):
        pytest.skip("Isolated TEST_POSTGRES_* connection is required.")
    if "test" not in os.environ["TEST_POSTGRES_DATABASE"].lower():
        pytest.fail("TEST_POSTGRES_DATABASE must name an isolated test database.")

    config = DatabaseConfig(
        host=os.environ["TEST_POSTGRES_HOST"],
        port=int(os.environ["TEST_POSTGRES_PORT"]),
        database=os.environ["TEST_POSTGRES_DATABASE"],
        user=os.environ["TEST_POSTGRES_USER"],
        password=os.environ["TEST_POSTGRES_PASSWORD"],
        schema="climate_data",
    )
    engine = create_engine(config.url, future=True)
    token = uuid4().hex
    partitions = (
        ProgressPartition(f"barrier-{token}-1", "2024-01-01", "2024-01-31"),
        ProgressPartition(f"barrier-{token}-2", "2024-02-01", "2024-02-29"),
        ProgressPartition(f"barrier-{token}-3", "2024-03-01", "2024-03-31"),
    )
    definition = PipelineProgressDefinition(
        pipeline_name=f"barrier-test-{token}",
        source="cds",
        dataset_name="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-03-31",
        configuration_identity=token,
        partitions=partitions,
    )
    progress_id = None
    try:
        create_pipeline_progress_tables(engine)
        progress_id = initialize_pipeline_progress(engine, definition)

        assert are_extract_attempts_terminal(engine, progress_id) is False

        update_pipeline_progress_detail(
            engine,
            progress_id,
            partitions[0].request_key,
            extract_status="complete",
        )
        update_pipeline_progress_detail(
            engine,
            progress_id,
            partitions[1].request_key,
            extract_status="failed",
            validation_status="failed",
            last_error="SubmissionError",
        )
        assert are_extract_attempts_terminal(engine, progress_id) is False

        update_pipeline_progress_detail(
            engine,
            progress_id,
            partitions[2].request_key,
            extract_status="complete",
        )
        assert are_extract_attempts_terminal(engine, progress_id) is True

        with pytest.raises(ValueError, match="unknown pipeline progress"):
            are_extract_attempts_terminal(engine, progress_id + 10_000_000)
    finally:
        if progress_id is not None:
            with engine.begin() as connection:
                connection.execute(
                    text(
                        f"DELETE FROM {PIPELINE_PROGRESS_SCHEMA}."
                        f"{PIPELINE_PROGRESS_TABLE} WHERE id = :id"
                    ),
                    {"id": progress_id},
                )
        engine.dispose()
