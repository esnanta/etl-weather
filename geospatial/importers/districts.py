"""Transactional, idempotent administrative boundary importer (no Airflow)."""
from __future__ import annotations

import argparse
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from geospatial.config import ACEH_DISTRICTS, GeoJSONSourceConfig
from geospatial.database.bootstrap import ensure_postgis_extension
from geospatial.importers.reader import DistrictFeature, read_district_features
from pipeline.database.config import (
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
           source_feature_ref, geom)
        VALUES (
          :country, :province, :dataset, :version, NULL,
          ST_Multi(ST_UnaryUnion(ST_Collect(
            ARRAY(
              SELECT ST_SetSRID(ST_GeomFromGeoJSON(x.geometry_json), 4326)
              FROM jsonb_to_recordset(CAST(:geometries AS jsonb))
                   AS x(geometry_json text)
            )
          )))
        )
        ON CONFLICT (country_code, province_name)
        DO UPDATE SET geom = EXCLUDED.geom,
                      source_dataset = EXCLUDED.source_dataset,
                      source_version = EXCLUDED.source_version
        RETURNING id
    """)
    district_sql = text(f"""
        INSERT INTO {APPLICATION_SCHEMA}.{DISTRICT_TABLE}
          (province_id, district_name, source_dataset, source_version,
           source_feature_ref, geom)
        VALUES (
          :province_id, :district, :dataset, :version, :ref,
          ST_SetSRID(ST_GeomFromGeoJSON(:geometry), 4326)
        )
        ON CONFLICT (source_dataset, source_version, source_feature_ref)
        DO UPDATE SET province_id = EXCLUDED.province_id,
                      district_name = EXCLUDED.district_name,
                      geom = EXCLUDED.geom
        RETURNING id
    """)
    # A province can have multiple districts in one source; dissolve its parts.
    grouped: dict[str, list[DistrictFeature]] = {}
    for feature in features:
        grouped.setdefault(feature.province_name, []).append(feature)

    import json

    with engine.begin() as connection:
        actual = connection.execute(text("SELECT current_database()")).scalar_one()
        if actual != database.database:
            raise RuntimeError("Importer connected to an unexpected database.")
        for province_name, members in sorted(grouped.items()):
            geometry_rows = json.dumps(
                [{"geometry_json": f.geometry_json} for f in members]
            )
            province_id = connection.execute(
                province_sql,
                {
                    "country": source.country_code,
                    "province": province_name,
                    "dataset": source.source_dataset,
                    "version": source.source_version,
                    "geometries": geometry_rows,
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
    engine = create_engine(config.url, future=True)
    try:
        result = import_districts(engine, database=config, source=source)
        print(
            f"Import complete: {result.features} features, "
            f"{result.provinces} provinces; SHA256={result.source_sha256}"
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
