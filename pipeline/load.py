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

from pipeline.logging import (
    log_step,
)

from pipeline.model import (
    TransformedWeatherDataset,
)

from pipeline.interfaces import DataLoader

logger = logging.getLogger(__name__)


@log_step
def insert_dataset(
    dataset: TransformedWeatherDataset,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """
    Insert dataset metadata.
    """
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
        raise RuntimeError(
            "Failed to retrieve the dataset id after inserting metadata."
        )

    if isinstance(dataset_id, int):
        return dataset_id

    return int(str(dataset_id))


@log_step
def insert_observations(
    dataset: TransformedWeatherDataset,
    dataset_id: int,
    connection: Connection,
    config: DatabaseConfig,
) -> int:
    """
    Insert weather observations.
    """

    dataframe = dataset.dataset.copy()

    if (
        "step" in dataframe.columns
        and pd.api.types.is_timedelta64_dtype(
            dataframe["step"]
        )
    ):
        dataframe["step"] = (
            dataframe["step"]
            .dt.total_seconds()
            .floordiv(3600)
            .astype("int16")
        )

    for column in (
        "time",
        "valid_time",
    ):
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(
                dataframe[column],
            )

    dataframe["dataset_id"] = dataset_id

    # Rename long-form variable names to short DB column names expected by
    # weather_dataset_detail. This prevents INSERT failures when the dataset
    # uses original variable names from the source (e.g. Copernicus/ARCO).
    column_mapping = {
        "total_precipitation": "tp",
        "surface_solar_radiation_downwards": "ssrd",
        # keep identity for already-short names (t2m, u10, v10, swh, msl)
    }

    # Only apply mappings for columns that actually exist in the dataframe.
    exist_map = {
        source_name: target_name
        for source_name, target_name in column_mapping.items()
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

    logger.info(
        "[METRIC] Details weather inserted | rows=%d",
        len(dataframe),
    )

    return len(
        dataframe,
    )


@log_step
def load_to_postgresql(
    dataset: TransformedWeatherDataset,
    config: DatabaseConfig | None = None,
) -> tuple[int, int]:
    """
    Load a transformed dataset into PostgreSQL.

    Returns
    -------
    tuple[int, int]
        (dataset_id, rows_inserted)
    """

    resolved_config = resolve_database_config(config)

    engine = create_database_engine(
        resolved_config,
    )

    create_schema(
        engine,
        resolved_config,
    )

    create_dataset_table(
        engine,
        resolved_config,
    )

    create_dataset_table_detail(
        engine,
        resolved_config,
    )

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

    logger.info(
        "[METRIC] PostgreSQL load completed | "
        "dataset_id=%d | "
        "rows=%d",
        dataset_id,
        rows_inserted,
    )

    return (
        dataset_id,
        rows_inserted,
    )

class PostgresDataLoader(DataLoader):
    """
    SOLID: Single Responsibility & Dependency Inversion.
    Handles loading dataset into PostgreSQL database.
    """
    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config

    def load(self, dataset: TransformedWeatherDataset) -> tuple[int, int]:
        return load_to_postgresql(dataset, self.config)
