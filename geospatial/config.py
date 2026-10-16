"""Declarative GeoJSON source mapping and spatial database contracts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from database.config import (
    APPLICATION_SCHEMA,
    DISTRICT_TABLE,
    GRID_REGION_TABLE,
    PROVINCE_TABLE,
)


@dataclass(frozen=True)
class GeoJSONSourceConfig:
    """Map one immutable GeoJSON dataset to canonical administrative attributes."""

    source_path: Path
    source_dataset: str
    source_version: str
    country_code: str
    province_name_property: str
    district_name_property: str
    feature_ref_property: str
    source_crs: str = "urn:ogc:def:crs:OGC:1.3:CRS84"
    geometry_type: str = "MultiPolygon"
    geometry_dimensions: int = 3
    numeric_integer_feature_refs: bool = True


ACEH_DISTRICTS = GeoJSONSourceConfig(
    source_path=Path(__file__).resolve().parent / "sources/id/districts/Aceh.geojson",
    source_dataset="id_aceh_districts",
    # Explicitly unknown until provider release information is verified.
    source_version="unverified",
    country_code="id",
    province_name_property="WADMPR",
    district_name_property="WADMKK",
    feature_ref_property="OBJECTID",
)

SPATIAL_SCHEMA = APPLICATION_SCHEMA
SPATIAL_PROVINCE_TABLE = PROVINCE_TABLE
SPATIAL_DISTRICT_TABLE = DISTRICT_TABLE
SPATIAL_GRID_REGION_TABLE = GRID_REGION_TABLE
