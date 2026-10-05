"""Read canonical daily-climate observations for modelling without mutation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import TypeAlias

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.elements import TextClause

from pipeline.database import DatabaseConfig, resolve_database_config
from pipeline.logging import log_metric
from pipeline.model import BoundingBox, DataSource


MODELLING_COLUMNS = (
    "daily_climate_id",
    "valid_time",
    "valid_time_local",
    "latitude",
    "longitude",
    "tp_daily_total_mm",
    "t2m_daily_mean_c",
    "d2m_daily_mean_c",
    "swvl1_daily_mean_m3_m3",
    "ssrd_daily_total_j_m2",
)

MODELLING_OBSERVATION_KEY = (
    "valid_time_local",
    "latitude",
    "longitude",
)

SqlParamValue: TypeAlias = str | bytes | date | datetime | timedelta | bool | int | float


@dataclass(frozen=True)
class DailyClimateSelection:
    """Explicit selection of canonical daily climate observations for modelling."""

    source: DataSource
    dataset_name: str
    start_date: date
    end_date: date
    bbox: BoundingBox


def validate_daily_climate_selection(selection: DailyClimateSelection) -> None:
    """Reject ambiguous or invalid modelling selections before querying PostgreSQL."""
    if selection.start_date > selection.end_date:
        raise ValueError("start_date must be on or before end_date.")
    if not selection.dataset_name.strip():
        raise ValueError("dataset_name must not be empty.")
    if selection.bbox.north_lat < selection.bbox.south_lat:
        raise ValueError("north_lat must be greater than or equal to south_lat.")
    if selection.bbox.east_lon < selection.bbox.west_lon:
        raise ValueError("east_lon must be greater than or equal to west_lon.")


def _build_modelling_statement(config: DatabaseConfig) -> TextClause:
    daily_climate = f"{config.schema}.{config.daily_climate_table}"
    detail = f"{config.schema}.{config.daily_climate_detail_table}"
    return text(
        f"""
        SELECT
            dc.id AS daily_climate_id,
            dcd.valid_time,
            dcd.valid_time_local,
            dcd.latitude,
            dcd.longitude,
            dcd.tp_daily_total_mm,
            dcd.t2m_daily_mean_c,
            dcd.d2m_daily_mean_c,
            dcd.swvl1_daily_mean_m3_m3,
            dcd.ssrd_daily_total_j_m2
        FROM {daily_climate} AS dc
        JOIN {detail} AS dcd
          ON dcd.daily_climate_id = dc.id
        WHERE CAST(dc.source AS TEXT) = :source
          AND dc.dataset_name = :dataset_name
          AND dc.north_lat = :north_lat
          AND dc.south_lat = :south_lat
          AND dc.east_lon = :east_lon
          AND dc.west_lon = :west_lon
          AND dcd.valid_time_local >= :start_time
          AND dcd.valid_time_local < :end_time_exclusive
        ORDER BY
            dcd.valid_time_local,
            dcd.latitude,
            dcd.longitude,
            dc.id
        """
    )


def _build_query_params(selection: DailyClimateSelection) -> dict[str, SqlParamValue]:
    return {
        "source": selection.source.value,
        "dataset_name": selection.dataset_name,
        "north_lat": selection.bbox.north_lat,
        "south_lat": selection.bbox.south_lat,
        "east_lon": selection.bbox.east_lon,
        "west_lon": selection.bbox.west_lon,
        "start_time": datetime.combine(selection.start_date, time.min),
        "end_time_exclusive": datetime.combine(
            selection.end_date + timedelta(days=1), time.min
        ),
    }


def _execute_modelling_query(
    connection: Connection,
    statement: TextClause,
    params: dict[str, SqlParamValue],
) -> pd.DataFrame:
    """Execute the parameterized read through SQLAlchemy and return a DataFrame."""
    result = connection.execute(statement, params)
    columns = list(result.keys())
    return pd.DataFrame(result.mappings().all(), columns=columns)


def _validate_required_columns(dataframe: pd.DataFrame) -> None:
    missing_columns = [
        column for column in MODELLING_COLUMNS if column not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError(
            "Modelling dataset is missing required columns: "
            + ", ".join(missing_columns)
        )


def validate_modelling_dataframe(
    dataframe: pd.DataFrame,
    selection: DailyClimateSelection,
) -> None:
    """Validate the canonical database-to-modelling handoff without changing values."""
    _validate_required_columns(dataframe)
    if dataframe.empty:
        raise ValueError("No daily climate observations matched the modelling selection.")

    duplicate_mask = dataframe.duplicated(
        subset=list(MODELLING_OBSERVATION_KEY), keep=False
    )
    if duplicate_mask.any():
        duplicate_count = int(duplicate_mask.sum())
        raise ValueError(
            "Modelling dataset contains duplicate local-day grid observations: "
            f"{duplicate_count} rows."
        )

    local_time = pd.to_datetime(dataframe["valid_time_local"])
    if not local_time.dt.normalize().equals(local_time):
        raise ValueError("valid_time_local must represent local midnight.")

    local_dates = local_time.dt.date
    if local_dates.min() < selection.start_date or local_dates.max() > selection.end_date:
        raise ValueError("Modelling dataset contains observations outside the requested period.")


def load_daily_climate_for_modelling(
    selection: DailyClimateSelection,
    config: DatabaseConfig | None = None,
    *,
    engine: Engine | None = None,
) -> pd.DataFrame:
    """Read canonical PostgreSQL daily climate observations into a modelling DataFrame.

    This function is intentionally read-only. It does not create or mutate database
    objects, add modelling labels, scale values, or replace missing values.
    """
    validate_daily_climate_selection(selection)
    resolved_config = resolve_database_config(config)
    statement = _build_modelling_statement(resolved_config)
    params = _build_query_params(selection)

    owns_engine = engine is None
    resolved_engine = engine or create_engine(resolved_config.url, future=True)
    try:
        with resolved_engine.connect() as connection:
            dataframe = _execute_modelling_query(connection, statement, params)
    finally:
        if owns_engine:
            resolved_engine.dispose()

    _validate_required_columns(dataframe)
    dataframe = dataframe.loc[:, list(MODELLING_COLUMNS)].copy()
    dataframe["valid_time"] = pd.to_datetime(dataframe["valid_time"])
    dataframe["valid_time_local"] = pd.to_datetime(dataframe["valid_time_local"])
    dataframe = dataframe.sort_values(
        ["valid_time_local", "latitude", "longitude", "daily_climate_id"],
        kind="stable",
    ).reset_index(drop=True)

    validate_modelling_dataframe(dataframe, selection)

    log_metric(
        "Daily climate modelling dataset",
        {
            "source": selection.source.value,
            "dataset_name": selection.dataset_name,
            "period": f"{selection.start_date} -> {selection.end_date}",
            "rows": len(dataframe),
            "daily_climate_partitions": dataframe["daily_climate_id"].nunique(),
            "local_days": dataframe["valid_time_local"].nunique(),
            "grid_cells": dataframe[["latitude", "longitude"]]
            .drop_duplicates()
            .shape[0],
        },
    )
    return dataframe
