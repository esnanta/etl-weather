"""Explicit geospatial DDL CLI. Canonical DDL lives in root database.geospatial."""

from __future__ import annotations

import argparse

from database.config import PROVINCE_TABLE, DISTRICT_TABLE, GRID_REGION_TABLE, resolve_database_config
from database.connection import create_database_engine
from database.geospatial import create_spatial_tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Create non-destructive spatial tables.")
    parser.add_argument("--execute", action="store_true", help="Apply CREATE TABLE/INDEX to the configured PostgreSQL database.")
    args = parser.parse_args()
    config = resolve_database_config()
    print(f"Spatial schema target: database={config.database}, schema={config.schema}")
    print(f"Tables: {PROVINCE_TABLE}, {DISTRICT_TABLE}, {GRID_REGION_TABLE}")
    if not args.execute:
        print("Dry run: no database changes. Pass --execute to create missing objects.")
        return
    engine = create_database_engine(config)
    try:
        create_spatial_tables(engine, config)
        print("Spatial tables ready; no GeoJSON imported.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
