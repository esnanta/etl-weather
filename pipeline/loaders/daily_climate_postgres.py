"""Load canonical daily-climate partitions transactionally into PostgreSQL."""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from pipeline.database import (
    DatabaseConfig,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_database_engine,
    create_schema,
    resolve_database_config,
)
from pipeline.interfaces import DataLoader
from pipeline.logging import log_metric
from pipeline.model import DatasetArtifact, TransformedWeatherDataset
from pipeline.storage import (
    artifact_path_for_metadata,
    calculate_file_sha256,
    read_parquet_provenance,
)
from pipeline.request import build_request_key, build_variable_signature

logger = logging.getLogger(__name__)

ERA5_LAND_DATABASE_COLUMNS = {
    "tp": "tp_daily_total_mm",
    "t2m": "t2m_daily_mean_c",
    "d2m": "d2m_daily_mean_c",
    "swvl1": "swvl1_daily_mean_m3_m3",
    "ssrd": "ssrd_daily_total_j_m2",
}


def upsert_daily_climate_metadata(
    dataset: TransformedWeatherDataset,
    artifact: DatasetArtifact,
    connection: Connection,
    config: DatabaseConfig,
    *,
    transformed_artifact_path: str | None = None,
    transformed_sha256: str | None = None,
) -> int:
    """Create or reuse metadata for one logical daily-climate partition."""
    request = dataset.request
    request_key = build_request_key(request)
    provenance = read_parquet_provenance(artifact)
    downloaded_at = datetime.fromisoformat(provenance["downloaded_at"])
    transformed_artifact_path = (
        transformed_artifact_path or artifact_path_for_metadata(artifact.path)
    )
    transformed_sha256 = transformed_sha256 or calculate_file_sha256(artifact.path)

    result = connection.execute(
        text(
            f"""
            INSERT INTO {config.schema}.{config.daily_climate_table}
            (
                request_key, dataset_name, source, start_date, end_date,
                variable_signature, north_lat, south_lat, east_lon, west_lon,
                downloaded_at, transformed_artifact_path, transformed_sha256,
                transformed_at
            )
            VALUES
            (
                :request_key, :dataset_name, :source, :start_date, :end_date,
                :variable_signature, :north_lat, :south_lat, :east_lon, :west_lon,
                :downloaded_at, :transformed_artifact_path, :transformed_sha256,
                :transformed_at
            )
            ON CONFLICT (request_key)
            DO UPDATE SET
                dataset_name = EXCLUDED.dataset_name,
                source = EXCLUDED.source,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                variable_signature = EXCLUDED.variable_signature,
                north_lat = EXCLUDED.north_lat,
                south_lat = EXCLUDED.south_lat,
                east_lon = EXCLUDED.east_lon,
                west_lon = EXCLUDED.west_lon,
                downloaded_at = EXCLUDED.downloaded_at,
                transformed_artifact_path = EXCLUDED.transformed_artifact_path,
                transformed_sha256 = EXCLUDED.transformed_sha256,
                transformed_at = EXCLUDED.transformed_at
            RETURNING id
            """
        ),
        {
            "request_key": request_key,
            "dataset_name": request.dataset,
            "source": request.source.value,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "variable_signature": build_variable_signature(request.variables),
            "north_lat": request.bbox.north_lat,
            "south_lat": request.bbox.south_lat,
            "east_lon": request.bbox.east_lon,
            "west_lon": request.bbox.west_lon,
            "downloaded_at": downloaded_at,
            "transformed_artifact_path": transformed_artifact_path,
            "transformed_sha256": transformed_sha256,
            "transformed_at": dataset.transformed_at,
        },
    )
    daily_climate_id = result.scalar()
    if not isinstance(daily_climate_id, int):
        raise RuntimeError(
            "Failed to retrieve integer daily climate id after metadata upsert."
        )

    log_metric(
        "Daily climate metadata upsert",
        {
            "daily_climate_id": daily_climate_id,
            "request_key": request_key,
            "variable_signature": build_variable_signature(request.variables),
            "downloaded_at": downloaded_at,
            "transformed_artifact_path": transformed_artifact_path,
            "transformed_sha256": transformed_sha256,
        },
    )
    return daily_climate_id


def find_unchanged_daily_climate_partition(
    *,
    request_key: str,
    transformed_artifact_path: str,
    transformed_sha256: str,
    expected_rows: int,
    connection: Connection,
    config: DatabaseConfig,
) -> tuple[int, int] | None:
    """Return an unchanged partition only when provenance and row count match."""
    metadata_result = connection.execute(
        text(
            f"""
            SELECT id, transformed_artifact_path, transformed_sha256
            FROM {config.schema}.{config.daily_climate_table}
            WHERE request_key = :request_key
            """
        ),
        {"request_key": request_key},
    )
    metadata = metadata_result.mappings().one_or_none()
    if metadata is None:
        return None

    checksum_matches = metadata["transformed_sha256"] == transformed_sha256
    path_matches = (
        metadata["transformed_artifact_path"] == transformed_artifact_path
    )
    if not checksum_matches or not path_matches:
        return None

    daily_climate_id = int(metadata["id"])
    database_rows = int(
        connection.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {config.schema}.{config.daily_climate_detail_table}
                WHERE daily_climate_id = :daily_climate_id
                """
            ),
            {"daily_climate_id": daily_climate_id},
        ).scalar_one()
    )
    if database_rows != expected_rows:
        return None

    log_metric(
        "PostgreSQL load skipped",
        {
            "daily_climate_id": daily_climate_id,
            "request_key_match": True,
            "checksum_match": True,
            "artifact_path_match": True,
            "expected_rows": expected_rows,
            "database_rows": database_rows,
            "status": "unchanged",
            "database_write": "skipped",
        },
    )
    return daily_climate_id, database_rows


def _prepare_observation_dataframe(
    dataset: TransformedWeatherDataset,
    daily_climate_id: int,
) -> tuple[pd.DataFrame, dict[str, str]]:
    dataframe = dataset.dataset.copy()
    for time_column in ("valid_time", "valid_time_local"):
        if time_column in dataframe.columns:
            dataframe[time_column] = pd.to_datetime(dataframe[time_column]).map(
                lambda value: value.to_pydatetime()
            )

    dataframe["daily_climate_id"] = daily_climate_id
    rename_map = {
        provider_name: database_name
        for provider_name, database_name in ERA5_LAND_DATABASE_COLUMNS.items()
        if provider_name in dataframe.columns
    }
    if rename_map:
        dataframe = dataframe.rename(columns=rename_map)

    dataframe = dataframe.astype(object).where(pd.notna(dataframe), None)
    return dataframe, rename_map


def upsert_daily_climate_observations(
    dataset: TransformedWeatherDataset,
    daily_climate_id: int,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """UPSERT daily observations using partition/time/grid identity."""
    dataframe, _ = _prepare_observation_dataframe(dataset, daily_climate_id)
    if dataframe.empty:
        return 0

    columns = list(dataframe.columns)
    insert_columns = ", ".join(columns)
    bind_columns = ", ".join(f":{column}" for column in columns)
    key_columns = {"daily_climate_id", "valid_time", "latitude", "longitude"}
    update_columns = [column for column in columns if column not in key_columns]
    conflict_action = (
        "DO UPDATE SET "
        + ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
        if update_columns
        else "DO NOTHING"
    )

    statement = text(
        f"""
        INSERT INTO {config.schema}.{config.daily_climate_detail_table}
        ({insert_columns})
        VALUES ({bind_columns})
        ON CONFLICT (daily_climate_id, valid_time, latitude, longitude)
        {conflict_action}
        """
    )

    records = dataframe.to_dict(orient="records")
    chunk_size = 1000
    for start in range(0, len(records), chunk_size):
        connection.execute(statement, records[start : start + chunk_size])

    log_metric(
        "Daily climate observations upserted",
        {
            "daily_climate_id": daily_climate_id,
            "rows_processed": len(dataframe),
            "variables": ", ".join(dataset.request.variables),
            "conflict_key": "daily_climate_id + valid_time + latitude + longitude",
            "mode": "idempotent upsert",
        },
    )
    return len(dataframe)


def load_to_postgresql(
    dataset: TransformedWeatherDataset,
    artifact: DatasetArtifact,
    config: DatabaseConfig | None = None,
) -> tuple[int, int]:
    """Load one daily-climate partition safely and idempotently."""
    resolved_config = resolve_database_config(config)
    engine = create_database_engine(resolved_config)
    create_schema(engine, resolved_config)
    create_daily_climate_table(engine, resolved_config)
    create_daily_climate_detail_table(engine, resolved_config)

    request_key = build_request_key(dataset.request)
    transformed_artifact_path = artifact_path_for_metadata(artifact.path)
    transformed_sha256 = calculate_file_sha256(artifact.path)
    expected_rows = len(dataset.dataset)

    with engine.begin() as connection:
        unchanged_partition = find_unchanged_daily_climate_partition(
            request_key=request_key,
            transformed_artifact_path=transformed_artifact_path,
            transformed_sha256=transformed_sha256,
            expected_rows=expected_rows,
            connection=connection,
            config=resolved_config,
        )
        if unchanged_partition is not None:
            daily_climate_id, _ = unchanged_partition
            return daily_climate_id, 0

        daily_climate_id = upsert_daily_climate_metadata(
            dataset=dataset,
            artifact=artifact,
            connection=connection,
            config=resolved_config,
            transformed_artifact_path=transformed_artifact_path,
            transformed_sha256=transformed_sha256,
        )
        rows_processed = upsert_daily_climate_observations(
            dataset=dataset,
            daily_climate_id=daily_climate_id,
            connection=connection,
            config=resolved_config,
        )

    log_metric(
        "PostgreSQL load completed",
        {
            "daily_climate_id": daily_climate_id,
            "rows_processed": rows_processed,
            "mode": "idempotent upsert",
        },
    )
    return daily_climate_id, rows_processed


class PostgresDataLoader(DataLoader):
    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config

    def load(
        self,
        dataset: TransformedWeatherDataset,
        artifact: DatasetArtifact,
    ) -> tuple[int, int]:
        return load_to_postgresql(dataset, artifact, self.config)
