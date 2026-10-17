"""Deterministic mapping of persisted ERA5-Land grid points to Aceh districts.

This module never changes climate observations, polygon sources or Airflow state.
Only distinct coordinates from existing climate observations are eligible.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import (
    APPLICATION_SCHEMA,
    DAILY_CLIMATE_DETAIL_TABLE,
    DISTRICT_TABLE,
    GRID_REGION_TABLE,
    PROVINCE_TABLE,
    DatabaseConfig,
    resolve_database_config,
)
from database.connection import create_database_engine

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MappingSummary:
    coordinates: int
    matched: int
    unmatched: int
    ambiguous: int


# In one statement, aggregate all district candidates before deciding whether
# any IDs can be assigned. ST_Covers includes polygon boundaries. Multiplicity
# is never resolved with LIMIT 1, MIN(id), or an arbitrary ordering.
_MAPPING_SQL = f"""
WITH points AS (
    SELECT DISTINCT latitude, longitude
    FROM {APPLICATION_SCHEMA}.{DAILY_CLIMATE_DETAIL_TABLE}
),
candidates AS (
    SELECT p.latitude, p.longitude, d.id AS district_id, d.province_id
    FROM points p
    LEFT JOIN {APPLICATION_SCHEMA}.{DISTRICT_TABLE} d
      ON climate_data.ST_Covers(
          d.boundary_geometry,
          climate_data.ST_SetSRID(
              climate_data.ST_MakePoint(p.longitude, p.latitude), 4326
          )
      )
),
classified AS (
    SELECT latitude, longitude,
           COUNT(district_id)::integer AS match_count,
           CASE WHEN COUNT(district_id) = 1 THEN MIN(district_id) END AS district_id,
           CASE WHEN COUNT(district_id) = 1 THEN MIN(province_id) END AS province_id
    FROM candidates
    GROUP BY latitude, longitude
),
written AS (
    INSERT INTO {APPLICATION_SCHEMA}.{GRID_REGION_TABLE}
        (latitude, longitude, province_id, district_id, match_status, match_count)
    SELECT latitude, longitude, province_id, district_id,
           CASE WHEN match_count = 0 THEN 'unmatched'
                WHEN match_count = 1 THEN 'matched'
                ELSE 'ambiguous' END,
           match_count
    FROM classified
    ON CONFLICT (latitude, longitude) DO UPDATE
    SET province_id = EXCLUDED.province_id,
        district_id = EXCLUDED.district_id,
        match_status = EXCLUDED.match_status,
        match_count = EXCLUDED.match_count,
        mapped_at = CASE
            WHEN ({APPLICATION_SCHEMA}.{GRID_REGION_TABLE}.province_id,
                  {APPLICATION_SCHEMA}.{GRID_REGION_TABLE}.district_id,
                  {APPLICATION_SCHEMA}.{GRID_REGION_TABLE}.match_status,
                  {APPLICATION_SCHEMA}.{GRID_REGION_TABLE}.match_count)
                 IS DISTINCT FROM
                 (EXCLUDED.province_id, EXCLUDED.district_id,
                  EXCLUDED.match_status, EXCLUDED.match_count)
            THEN CURRENT_TIMESTAMP
            ELSE {APPLICATION_SCHEMA}.{GRID_REGION_TABLE}.mapped_at
        END
    RETURNING match_status
)
SELECT COUNT(*)::integer AS coordinates,
       COUNT(*) FILTER (WHERE match_status = 'matched')::integer AS matched,
       COUNT(*) FILTER (WHERE match_status = 'unmatched')::integer AS unmatched,
       COUNT(*) FILTER (WHERE match_status = 'ambiguous')::integer AS ambiguous
FROM written
"""

_STATUS_SQL = f"""
WITH points AS (
    SELECT DISTINCT latitude, longitude
    FROM {APPLICATION_SCHEMA}.{DAILY_CLIMATE_DETAIL_TABLE}
),
candidates AS (
    SELECT p.latitude, p.longitude, d.id AS district_id
    FROM points p
    LEFT JOIN {APPLICATION_SCHEMA}.{DISTRICT_TABLE} d
      ON climate_data.ST_Covers(
          d.boundary_geometry,
          climate_data.ST_SetSRID(
              climate_data.ST_MakePoint(p.longitude, p.latitude), 4326
          )
      )
),
counts AS (
    SELECT latitude, longitude, COUNT(district_id) AS matches
    FROM candidates GROUP BY latitude, longitude
)
SELECT COUNT(*)::integer AS coordinates,
       COUNT(*) FILTER (WHERE matches = 1)::integer AS matched,
       COUNT(*) FILTER (WHERE matches = 0)::integer AS unmatched,
       COUNT(*) FILTER (WHERE matches > 1)::integer AS ambiguous
FROM counts
"""


def map_grid_regions(
    engine: Engine, config: DatabaseConfig, *, execute: bool = False,
) -> MappingSummary:
    """Preview or transactionally upsert all distinct persisted observation grids."""
    if config.schema != APPLICATION_SCHEMA:
        raise ValueError("Mapping requires the canonical climate_data schema.")
    with engine.begin() as connection:
        actual = connection.execute(text("SELECT current_database()")).scalar_one()
        if actual != config.database:
            raise RuntimeError("Unexpected application database identity.")
        # No rows are changed in preview mode. SQL does not mutate polygons.
        row = connection.execute(
            text(_MAPPING_SQL if execute else _STATUS_SQL)
        ).mappings().one()
    return MappingSummary(**dict(row))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview or persist ERA5-Land point-to-Aceh district matches."
    )
    parser.add_argument("--execute", action="store_true",
                        help="Persist classified grid points in t_grid_region.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    config = resolve_database_config()
    print(f"Grid mapping target: {config.database}.{config.schema}; execute={args.execute}")
    engine = create_database_engine(config)
    try:
        result = map_grid_regions(engine, config, execute=args.execute)
    finally:
        engine.dispose()
    print(
        f"Grid mapping {'committed' if args.execute else 'preview'} | "
        f"coordinates={result.coordinates} matched={result.matched} "
        f"unmatched={result.unmatched} ambiguous={result.ambiguous}"
    )
    if not result.coordinates:
        print("No climate coordinates are present; nothing to map.")


if __name__ == "__main__":
    main()
