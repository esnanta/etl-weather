from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from pipeline.database import (
    DatabaseConfig,
    create_database_engine,
    create_dataset_table,
    create_dataset_table_detail,
    create_schema,
    resolve_database_config,
)
from pipeline.interfaces import DataLoader
from pipeline.logging import log_metric
from pipeline.model import TransformedWeatherDataset

logger = logging.getLogger(__name__)

ERA5_LAND_DATABASE_COLUMNS = {
    "tp": "tp_daily_total_mm",
    "t2m": "t2m_daily_mean_c",
    "d2m": "d2m_daily_mean_c",
    "swvl1": "swvl1_daily_mean_m3_m3",
    "ssrd": "ssrd_daily_total_j_m2",
}


def insert_dataset(
    dataset: TransformedWeatherDataset,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    result = connection.execute(
        text(
            f"""
            INSERT INTO {config.schema}.{config.dataset_table}
            (dataset_name, source, start_date, end_date, transformed_at)
            VALUES (:dataset_name, :source, :start_date, :end_date, :transformed_at)
            RETURNING id
            """
        ),
        {
            "dataset_name": dataset.request.dataset,
            "source": dataset.request.source.value,
            "start_date": dataset.request.start_date,
            "end_date": dataset.request.end_date,
            "transformed_at": datetime.now(timezone.utc),
        },
    )
    dataset_id = result.scalar()
    if dataset_id is None:
        raise RuntimeError("Failed to retrieve the dataset id after inserting metadata.")
    return int(dataset_id)


def insert_observations(
    dataset: TransformedWeatherDataset,
    dataset_id: int,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """Insert transformed observations using descriptive daily DB columns."""
    dataframe = dataset.dataset.copy()

    for time_column in ("valid_time", "valid_time_local"):
        if time_column in dataframe.columns:
            dataframe[time_column] = pd.to_datetime(dataframe[time_column])

    dataframe["dataset_id"] = dataset_id

    rename_map = {
        provider_name: database_name
        for provider_name, database_name in ERA5_LAND_DATABASE_COLUMNS.items()
        if provider_name in dataframe.columns
    }
    if rename_map:
        dataframe = dataframe.rename(columns=rename_map)

    dataframe.to_sql(
        name=config.dataset_table_detail,
        con=connection,
        schema=config.schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    log_metric(
        "Details weather inserted",
        {
            "rows": len(dataframe),
            "columns": list(dataframe.columns),
            "provider_to_database_columns": rename_map,
        },
    )
    return len(dataframe)


def load_to_postgresql(
    dataset: TransformedWeatherDataset,
    config: DatabaseConfig | None = None,
) -> tuple[int, int]:
    resolved_config = resolve_database_config(config)
    engine = create_database_engine(resolved_config)
    create_schema(engine, resolved_config)
    create_dataset_table(engine, resolved_config)
    create_dataset_table_detail(engine, resolved_config)

    with engine.begin() as connection:
        dataset_id = insert_dataset(
            dataset=dataset,
            connection=connection,
            config=resolved_config,
        )
        rows_inserted = insert_observations(
            dataset=dataset,
            dataset_id=dataset_id,
            connection=connection,
            config=resolved_config,
        )

    log_metric(
        "PostgreSQL load completed",
        {"dataset_id": dataset_id, "rows": rows_inserted},
    )
    return dataset_id, rows_inserted


class PostgresDataLoader(DataLoader):
    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config

    def load(self, dataset: TransformedWeatherDataset) -> tuple[int, int]:
        return load_to_postgresql(dataset, self.config)
