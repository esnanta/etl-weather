"""Transactional, idempotent administrative boundary importer (no Airflow)."""
from __future__ import annotations

import argparse
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from tqdm.auto import tqdm

from sqlalchemy import text
from database.connection import create_database_engine
from sqlalchemy.engine import Engine

from geospatial.config import ACEH_DISTRICTS, GeoJSONSourceConfig
from database.bootstrap import ensure_postgis_extension
from geospatial.importers.reader import DistrictFeature, read_district_features
from database.config import (
    APPLICATION_SCHEMA,
    DISTRICT_TABLE,
    PROVINCE_TABLE,
    DatabaseConfig,
    resolve_database_config,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportSummary:
    features: int
    provinces: int
    source_sha256: str


def import_districts(
    engine: Engine,
    *,
    database: DatabaseConfig,
    source: GeoJSONSourceConfig = ACEH_DISTRICTS,
    path: Path | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> ImportSummary:
    """Validate entire source first; commit province/district UPSERTs atomically.

    Never prune missing features: a partial county input must not remove
    previously imported districts. This importer does not change grid mapping.
    """
    if database.schema != APPLICATION_SCHEMA:
        raise ValueError("Expected climate_data application schema.")
    if not source.source_dataset or not source.source_version:
        raise ValueError("Source dataset/version must be non-empty.")
    input_path = path if path is not None else source.source_path
    features = read_district_features(source, path=input_path)
    checksum = hashlib.sha256(input_path.read_bytes()).hexdigest()

    ensure_postgis_extension(engine, expected_database=database.database)
    province_sql = text(f"""
        INSERT INTO {APPLICATION_SCHEMA}.{PROVINCE_TABLE}
          (country_code, province_name, source_dataset, source_version,
           source_feature_ref, boundary_geometry)
        VALUES (
          :country, :province, :dataset, :version, NULL,
          {APPLICATION_SCHEMA}.ST_SetSRID({APPLICATION_SCHEMA}.ST_GeomFromGeoJSON(:geometry), 4326)
        )
        ON CONFLICT (country_code, province_name)
        DO UPDATE SET source_dataset = EXCLUDED.source_dataset,
                      source_version = EXCLUDED.source_version
        RETURNING id
    """)
    dissolve_sql = text(f"""
        UPDATE {APPLICATION_SCHEMA}.{PROVINCE_TABLE} p
        SET boundary_geometry = boundaries.boundary_geometry
        FROM (
          SELECT {APPLICATION_SCHEMA}.ST_Multi({APPLICATION_SCHEMA}.ST_UnaryUnion({APPLICATION_SCHEMA}.ST_Collect(d.boundary_geometry))) AS boundary_geometry
          FROM {APPLICATION_SCHEMA}.{DISTRICT_TABLE} d
          WHERE d.province_id = :province_id
        ) boundaries
        WHERE p.id = :province_id
          AND boundaries.boundary_geometry IS NOT NULL
    """)
    district_sql = text(f"""
        INSERT INTO {APPLICATION_SCHEMA}.{DISTRICT_TABLE}
          (province_id, district_name, source_dataset, source_version,
           source_feature_ref, boundary_geometry)
        VALUES (
          :province_id, :district, :dataset, :version, :ref,
          {APPLICATION_SCHEMA}.ST_SetSRID({APPLICATION_SCHEMA}.ST_GeomFromGeoJSON(:geometry), 4326)
        )
        ON CONFLICT (source_dataset, source_version, source_feature_ref)
        DO UPDATE SET province_id = EXCLUDED.province_id,
                      district_name = EXCLUDED.district_name,
                      boundary_geometry = EXCLUDED.boundary_geometry
        RETURNING id
    """)
    # A province can have multiple districts in one source; dissolve its parts.
    grouped: dict[str, list[DistrictFeature]] = {}
    for feature in features:
        grouped.setdefault(feature.province_name, []).append(feature)

    completed = 0
    with engine.begin() as connection:
        actual = connection.execute(text("SELECT current_database()")).scalar_one()
        if actual != database.database:
            raise RuntimeError("Importer connected to an unexpected database.")
        for province_name, members in sorted(grouped.items()):
            province_id = connection.execute(
                province_sql,
                {
                    "country": source.country_code,
                    "province": province_name,
                    "dataset": source.source_dataset,
                    "version": source.source_version,
                    "geometry": members[0].geometry_json,
                },
            ).scalar_one()
            for feature in members:
                connection.execute(
                    district_sql,
                    {
                        "province_id": province_id,
                        "district": feature.district_name,
                        "dataset": source.source_dataset,
                        "version": source.source_version,
                        "ref": feature.source_feature_ref,
                        "geometry": feature.geometry_json,
                    },
                ).scalar_one()
                completed += 1
                if progress is not None:
                    progress(completed, len(features))
            connection.execute(dissolve_sql, {"province_id": province_id})

    logger.info(
        "Administrative import committed | features=%s | provinces=%s | sha256=%s",
        len(features), len(grouped), checksum,
    )
    return ImportSummary(len(features), len(grouped), checksum)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import immutable GeoJSON districts.")
    parser.add_argument(
        "--execute", action="store_true",
        help="Commit validated features into the configured climate_data tables.",
    )
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    config = resolve_database_config()
    source = ACEH_DISTRICTS
    features = read_district_features(source)
    print(
        f"Target database={config.database} schema={config.schema}; "
        f"source={source.source_path}; features={len(features)}; "
        f"source_version={source.source_version}"
    )
    if not arguments.execute:
        print("Dry run: GeoJSON validated, database unchanged. Use --execute to import.")
        return
    engine = create_database_engine(config)
    try:
        with tqdm(total=len(features), desc="Importing districts", unit="district") as bar:
            last_count = 0

            def update_progress(completed: int, total: int) -> None:
                nonlocal last_count
                bar.update(completed - last_count)
                last_count = completed

            result = import_districts(
                engine, database=config, source=source, progress=update_progress
            )
        print("Transaction committed successfully.")
        print(
            f"Import complete: {result.features} features, "
            f"{result.provinces} provinces; SHA256={result.source_sha256}"
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
