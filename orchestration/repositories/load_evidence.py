"""Read persisted load evidence used to validate orchestration completion."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.database import (
    DAILY_CLIMATE_DETAIL_TABLE,
    DAILY_CLIMATE_TABLE,
    SCHEMA_NAME,
)


def persisted_row_count(
    engine: Engine,
    result: Mapping[str, Any],
    path: str,
    checksum: str,
) -> int:
    """Return persisted observation count only when artifact identity matches."""
    with engine.connect() as connection:
        row = connection.execute(
            text(
                f"""
                SELECT metadata.request_key, metadata.transformed_artifact_path,
                       metadata.transformed_sha256, COUNT(observation.id) AS row_count
                FROM {SCHEMA_NAME}.{DAILY_CLIMATE_TABLE} AS metadata
                LEFT JOIN {SCHEMA_NAME}.{DAILY_CLIMATE_DETAIL_TABLE} AS observation
                  ON observation.daily_climate_id = metadata.id
                WHERE metadata.id = :daily_climate_id
                GROUP BY metadata.id
                """
            ),
            {"daily_climate_id": result["daily_climate_id"]},
        ).mappings().one_or_none()

    if (
        row is None
        or row["request_key"] != result["request_key"]
        or row["transformed_artifact_path"] != path
        or row["transformed_sha256"] != checksum
        or int(row["row_count"]) <= 0
    ):
        raise ValueError("Persisted partition evidence does not match the artifact.")
    return int(row["row_count"])
