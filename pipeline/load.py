from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from pipeline.database import (
    DatabaseConfig,
    create_daily_climate_detail_table,
    create_daily_climate_table,
    create_database_engine,
    create_schema,
    migrate_legacy_table_names,
    resolve_database_config,
)
from pipeline.interfaces import DataLoader
from pipeline.logging import log_metric
from pipeline.model import TransformedWeatherDataset
from pipeline.utils import build_request_key, build_variable_signature

logger = logging.getLogger(__name__)

ERA5_LAND_DATABASE_COLUMNS = {
    "tp": "tp_daily_total_mm",
    "t2m": "t2m_daily_mean_c",
    "d2m": "d2m_daily_mean_c",
    "swvl1": "swvl1_daily_mean_m3_m3",
    "ssrd": "ssrd_daily_total_j_m2",
}


def _adopt_single_legacy_daily_climate(
    dataset: TransformedWeatherDataset,
    connection: Connection,
    config: DatabaseConfig,
    request_key: str,
) -> int | None:
    request = dataset.request
    legacy_rows = connection.execute(
        text(
            f"""
            SELECT id
            FROM {config.schema}.{config.daily_climate_table}
            WHERE request_key IS NULL
              AND dataset_name = :dataset_name
              AND source = :source
              AND start_date = :start_date
              AND end_date = :end_date
            ORDER BY id
            """
        ),
        {
            "dataset_name": request.dataset,
            "source": request.source.value,
            "start_date": request.start_date,
            "end_date": request.end_date,
        },
    ).scalars().all()

    if len(legacy_rows) != 1:
        return None

    dataset_id = int(legacy_rows[0])
    connection.execute(
        text(
            f"""
            UPDATE {config.schema}.{config.daily_climate_table}
            SET request_key = :request_key,
                variable_signature = :variable_signature,
                north_lat = :north_lat,
                south_lat = :south_lat,
                east_lon = :east_lon,
                west_lon = :west_lon
            WHERE id = :dataset_id
            """
        ),
        {
            "request_key": request_key,
            "variable_signature": build_variable_signature(request.variables),
            "north_lat": request.bbox.north_lat,
            "south_lat": request.bbox.south_lat,
            "east_lon": request.bbox.east_lon,
            "west_lon": request.bbox.west_lon,
            "dataset_id": dataset_id,
        },
    )
    log_metric(
        "Legacy daily climate metadata adopted",
        {"dataset_id": dataset_id, "request_key": request_key},
    )
    return dataset_id


def upsert_daily_climate_metadata(
    dataset: TransformedWeatherDataset,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """Create or reuse metadata for one logical daily-climate partition."""
    request = dataset.request
    request_key = build_request_key(request)

    existing_id = connection.execute(
        text(
            f"""
            SELECT id
            FROM {config.schema}.{config.daily_climate_table}
            WHERE request_key = :request_key
            """
        ),
        {"request_key": request_key},
    ).scalar()

    if existing_id is None:
        existing_id = _adopt_single_legacy_daily_climate(
            dataset, connection, config, request_key
        )

    if existing_id is not None:
        dataset_id = int(existing_id)
    else:
        result = connection.execute(
            text(
                f"""
                INSERT INTO {config.schema}.{config.daily_climate_table}
                (
                    request_key, dataset_name, source, start_date, end_date,
                    variable_signature, north_lat, south_lat, east_lon, west_lon,
                    transformed_at
                )
                VALUES
                (
                    :request_key, :dataset_name, :source, :start_date, :end_date,
                    :variable_signature, :north_lat, :south_lat, :east_lon, :west_lon,
                    :transformed_at
                )
                ON CONFLICT (request_key) WHERE request_key IS NOT NULL
                DO UPDATE SET request_key = EXCLUDED.request_key
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
                "transformed_at": datetime.now(timezone.utc),
            },
        )
        dataset_id = result.scalar()
        if dataset_id is None:
            raise RuntimeError("Failed to retrieve daily climate id after metadata upsert.")
        dataset_id = int(dataset_id)

    log_metric(
        "Daily climate metadata upsert",
        {
            "dataset_id": dataset_id,
            "request_key": request_key,
            "variable_signature": build_variable_signature(request.variables),
        },
    )
    return dataset_id


def _prepare_observation_dataframe(
    dataset: TransformedWeatherDataset,
    dataset_id: int,
) -> tuple[pd.DataFrame, dict[str, str]]:
    dataframe = dataset.dataset.copy()
    for time_column in ("valid_time", "valid_time_local"):
        if time_column in dataframe.columns:
            dataframe[time_column] = pd.to_datetime(dataframe[time_column]).map(
                lambda value: value.to_pydatetime()
            )

    dataframe["dataset_id"] = dataset_id
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
    dataset_id: int,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """UPSERT daily observations using partition/time/grid identity."""
    dataframe, rename_map = _prepare_observation_dataframe(dataset, dataset_id)
    if dataframe.empty:
        return 0

    columns = list(dataframe.columns)
    insert_columns = ", ".join(columns)
    bind_columns = ", ".join(f":{column}" for column in columns)
    key_columns = {"dataset_id", "valid_time", "latitude", "longitude"}
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
        ON CONFLICT (dataset_id, valid_time, latitude, longitude)
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
            "rows_processed": len(dataframe),
            "columns": columns,
            "provider_to_database_columns": rename_map,
            "conflict_key": "dataset_id + valid_time + latitude + longitude",
        },
    )
    return len(dataframe)


def load_to_postgresql(
    dataset: TransformedWeatherDataset,
    config: DatabaseConfig | None = None,
) -> tuple[int, int]:
    """Load one daily-climate partition safely and idempotently."""
    resolved_config = resolve_database_config(config)
    engine = create_database_engine(resolved_config)
    create_schema(engine, resolved_config)
    migrate_legacy_table_names(engine, resolved_config)
    create_daily_climate_table(engine, resolved_config)
    create_daily_climate_detail_table(engine, resolved_config)

    with engine.begin() as connection:
        dataset_id = upsert_daily_climate_metadata(
            dataset=dataset,
            connection=connection,
            config=resolved_config,
        )
        rows_processed = upsert_daily_climate_observations(
            dataset=dataset,
            dataset_id=dataset_id,
            connection=connection,
            config=resolved_config,
        )

    log_metric(
        "PostgreSQL load completed",
        {
            "dataset_id": dataset_id,
            "rows_processed": rows_processed,
            "mode": "idempotent upsert",
        },
    )
    return dataset_id, rows_processed


class PostgresDataLoader(DataLoader):
    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config

    def load(self, dataset: TransformedWeatherDataset) -> tuple[int, int]:
        return load_to_postgresql(dataset, self.config)
