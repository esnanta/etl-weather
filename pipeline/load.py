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
from pipeline.logging import log_metric
from pipeline.model import TransformedWeatherDataset
from pipeline.interfaces import DataLoader
from pipeline.utils import CANONICAL_TO_SHORT_VARIABLE_NAMES

logger = logging.getLogger(__name__)


def insert_dataset(
    dataset: TransformedWeatherDataset,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """Insert dataset metadata."""
    result = connection.execute(
        text(
            f"""
            INSERT INTO
            {config.schema}.{config.dataset_table}
            (
                dataset_name,
                source,
                start_date,
                end_date,
                transformed_at
            )
            VALUES
            (
                :dataset_name,
                :source,
                :start_date,
                :end_date,
                :transformed_at
            )
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
    if isinstance(dataset_id, int):
        return dataset_id
    return int(str(dataset_id))


def insert_observations(
    dataset: TransformedWeatherDataset,
    dataset_id: int,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """Insert weather observations."""
    dataframe = dataset.dataset.copy()

    if "valid_time" in dataframe.columns:
        dataframe["valid_time"] = pd.to_datetime(dataframe["valid_time"])

    dataframe["dataset_id"] = dataset_id

    exist_map = {
        source_name: target_name
        for source_name, target_name in CANONICAL_TO_SHORT_VARIABLE_NAMES.items()
        if source_name in dataframe.columns
    }
    if exist_map:
        dataframe = dataframe.rename(columns=exist_map)

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
        },
    )

    return len(dataframe)


def load_to_postgresql(
    dataset: TransformedWeatherDataset,
    config: DatabaseConfig | None = None,
) -> tuple[int, int]:
    """Load a transformed dataset into PostgreSQL."""
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
        {
            "dataset_id": dataset_id,
            "rows": rows_inserted,
        },
    )

    return dataset_id, rows_inserted


class PostgresDataLoader(DataLoader):
    """Handles loading transformed weather data into PostgreSQL."""

    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config

    def load(self, dataset: TransformedWeatherDataset) -> tuple[int, int]:
        return load_to_postgresql(dataset, self.config)
