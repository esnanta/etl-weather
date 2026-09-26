from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from orchestration.progress import PipelineProgressDefinition, ProgressPartition
from orchestration.repositories.pipeline_progress import (
    create_pipeline_progress_tables,
    initialize_pipeline_progress,
    reconcile_pipeline_progress,
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
                        "FROM climate.pipeline_progress WHERE id = :id"
                    ),
                    {"id": progress_id},
                ).one()
            return str(row[0]), int(row[1])

        reconcile_pipeline_progress(engine, progress_id)
        assert status() == ("pending", 0)

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
                        text("DELETE FROM climate.pipeline_progress WHERE id = :id"),
                        {"id": progress_id},
                    )
        engine.dispose()
