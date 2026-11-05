"""Prove old completed PostgreSQL progress cannot complete Plan C progress.

Uses production progress DDL/reconciliation and a temporary TEST_POSTGRES_*
schema. No Airflow, CDS or application campaign tables are touched.
"""

import os
from dataclasses import replace
from datetime import date
from uuid import uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import database.schema as schema_module
import orchestration.repositories.pipeline_progress as progress_repo
from database.climate import (
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_schema,
)
from database.config import resolve_test_database_config
from orchestration.progress.definition import build_pipeline_progress_definition
from orchestration.scheduling.requests import build_historical_request_specs
from pipeline.requests.weather import weather_request_from_spec, weather_request_to_spec

pytestmark = pytest.mark.integration

OLD_VARIABLES = ["tp", "t2m", "d2m", "swvl1", "ssrd"]


def test_completed_legacy_partition_is_not_plan_c_completion_evidence(monkeypatch):
    load_dotenv()
    required = (
        "TEST_POSTGRES_HOST", "TEST_POSTGRES_PORT", "TEST_POSTGRES_DATABASE",
        "TEST_POSTGRES_USER", "TEST_POSTGRES_PASSWORD",
    )
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        pytest.skip("Isolated PostgreSQL integration test requires: " + ", ".join(missing))
    config = replace(
        resolve_test_database_config(),
        schema=f"climate_test_plan_c_progress_{uuid4().hex}",
    )
    engine = create_engine(config.url, future=True)
    monkeypatch.setattr(schema_module, "APPLICATION_SCHEMA", config.schema)
    monkeypatch.setattr(progress_repo, "PIPELINE_PROGRESS_SCHEMA", config.schema)
    monkeypatch.setattr(progress_repo, "SCHEMA_NAME", config.schema)

    try:
        create_schema(engine, config)
        create_daily_climate_table(engine, config)
        create_daily_climate_detail_table(engine, config)
        schema_module.create_pipeline_progress_tables(engine)

        current_spec = build_historical_request_specs(
            "2016-01-01", "2016-01-31", current_local_date=date.max
        )[0]
        old_request = replace(
            weather_request_from_spec(current_spec), variables=OLD_VARIABLES,
        )
        old_spec = weather_request_to_spec(old_request)
        old_def = build_pipeline_progress_definition([old_spec])
        new_def = build_pipeline_progress_definition([current_spec])

        old_progress_id = progress_repo.initialize_pipeline_progress(engine, old_def)
        new_progress_id = progress_repo.initialize_pipeline_progress(engine, new_def)
        assert old_progress_id != new_progress_id
        assert old_def.configuration_identity != new_def.configuration_identity

        old_key = old_def.partitions[0].request_key
        new_key = new_def.partitions[0].request_key
        assert old_key != new_key

        parent = f"{config.schema}.{config.daily_climate_table}"
        detail = f"{config.schema}.{config.daily_climate_detail_table}"
        with engine.begin() as connection:
            climate_id = connection.execute(text(f"""
                INSERT INTO {parent} (
                    request_key, dataset_name, source, start_date, end_date,
                    variable_signature, north_lat, south_lat, east_lon, west_lon,
                    downloaded_at, transformed_artifact_path,
                    transformed_sha256, transformed_at
                ) VALUES (
                    :request_key, :dataset, 'cds', '2016-01-01', '2016-01-31',
                    'd2m_ssrd_swvl1_t2m_tp', :north, :south, :east, :west,
                    CURRENT_TIMESTAMP, 'legacy.parquet', 'legacy-checksum',
                    CURRENT_TIMESTAMP
                ) RETURNING id
            """), {
                "request_key": old_key, "dataset": old_request.dataset,
                "north": old_request.bbox.north_lat,
                "south": old_request.bbox.south_lat,
                "east": old_request.bbox.east_lon,
                "west": old_request.bbox.west_lon,
            }).scalar_one()
            connection.execute(text(f"""
                INSERT INTO {detail} (
                    daily_climate_id, valid_time, valid_time_local, latitude,
                    longitude, tp_daily_total_mm
                ) VALUES (
                    :id, '2015-12-31 17:00:00', '2016-01-01 00:00:00',
                    4.5, 96.8, 5.0
                )
            """), {"id": climate_id})

        progress_repo.update_pipeline_progress_detail(
            engine, old_progress_id, old_key,
            extract_status="complete", transform_status="complete",
            load_status="complete", validation_status="valid",
            row_count=1, transformed_artifact_path="legacy.parquet",
            transformed_sha256="legacy-checksum",
        )
        progress_repo.reconcile_pipeline_progress(engine, old_progress_id)
        progress_repo.reconcile_pipeline_progress(engine, new_progress_id)

        legacy = progress_repo.get_pipeline_progress_by_id(engine, old_progress_id)
        current = progress_repo.get_pipeline_progress_by_id(engine, new_progress_id)
        assert legacy is not None
        assert current is not None
        assert legacy.execution_status == "complete"
        assert legacy.validation_status == "valid"
        assert legacy.completed_partition_count == 1
        assert current.execution_status == "pending"
        assert current.validation_status == "pending"
        assert current.completed_partition_count == 0
    finally:
        with engine.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {config.schema} CASCADE"))
        engine.dispose()
