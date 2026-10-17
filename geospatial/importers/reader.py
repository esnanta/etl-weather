"""Read and validate immutable administrative GeoJSON before any database writes."""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from geospatial.config import GeoJSONSourceConfig


@dataclass(frozen=True)
class DistrictFeature:
    province_name: str
    district_name: str
    source_feature_ref: str
    geometry_json: str


def normalize_feature_ref(value: object, *, numeric_integer: bool) -> str:
    """Preserve textual IDs; canonicalize only source-declared integer numbers."""
    if isinstance(value, bool) or value is None:
        raise ValueError("OBJECTID must be a non-empty string or numeric identifier.")
    if isinstance(value, str):
        result = value.strip()
    elif isinstance(value, (int, float, Decimal)):
        numeric = Decimal(str(value))
        if not numeric.is_finite():
            raise ValueError("Non-finite feature identifier.")
        if numeric_integer:
            if numeric != numeric.to_integral_value():
                raise ValueError("Expected integral numeric feature identifier.")
            result = str(int(numeric))
        else:
            result = str(numeric)
    else:
        raise ValueError("Unsupported feature identifier type.")
    if not result or len(result) > 128:
        raise ValueError("Empty or overlong source feature identifier.")
    return result


def _validate_coordinates(geometry: dict, *, dimensions: int) -> None:
    polygons = geometry.get("coordinates")
    if not isinstance(polygons, list) or not polygons:
        raise ValueError("MultiPolygon must contain polygons.")
    for polygon in polygons:
        if not isinstance(polygon, list) or not polygon:
            raise ValueError("Polygon must contain rings.")
        for ring in polygon:
            if not isinstance(ring, list) or len(ring) < 4:
                raise ValueError("Polygon ring must contain at least four points.")
            if ring[0] != ring[-1]:
                raise ValueError("Polygon ring is not closed.")
            for coords in ring:
                if not isinstance(coords, list) or len(coords) != dimensions:
                    raise ValueError("Incorrect GeoJSON coordinate dimensions.")
                if any(
                    isinstance(v, bool)
                    or not isinstance(v, (int, float, Decimal))
                    or not Decimal(str(v)).is_finite()
                    for v in coords
                ):
                    raise ValueError("GeoJSON has non-finite/non-numeric coordinate.")
                if not (-180 <= coords[0] <= 180 and -90 <= coords[1] <= 90):
                    raise ValueError("Longitude or latitude out of range.")


def read_district_features(
    source: GeoJSONSourceConfig,
    *,
    path: Path | None = None,
) -> list[DistrictFeature]:
    """Validate a complete source document without modifying its bytes."""
    input_path = path if path is not None else source.source_path
    with input_path.open("r", encoding="utf-8") as handle:
        document = json.load(handle, parse_float=Decimal)

    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise ValueError("Expected GeoJSON FeatureCollection.")

    declared = document.get("crs")
    if not isinstance(declared, dict) or declared.get("type") != "name" or (
        declared.get("properties") or {}
    ).get("name") != source.source_crs:
        raise ValueError("Source CRS does not match configured CRS84.")

    features = document.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError("GeoJSON has no features.")

    collected: list[DistrictFeature] = []
    seen_refs: set[str] = set()
    seen_names: set[tuple[str, str]] = set()
    for number, feature in enumerate(features, start=1):
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ValueError(f"Invalid Feature at index {number}.")
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            raise ValueError(f"Feature {number}: missing properties.")
        try:
            province = properties[source.province_name_property]
            district = properties[source.district_name_property]
            raw_ref = properties[source.feature_ref_property]
        except KeyError as error:
            raise ValueError(f"Feature {number}: missing mapped property {error}.") from error
        if not all(isinstance(v, str) and v.strip() for v in (province, district)):
            raise ValueError(f"Feature {number}: invalid administrative name.")
        province = province.strip()
        district = district.strip()
        if len(province) > 255 or len(district) > 255:
            raise ValueError(f"Feature {number}: administrative name too long.")
        ref = normalize_feature_ref(
            raw_ref, numeric_integer=source.numeric_integer_feature_refs
        )
        identity = (province.casefold(), district.casefold())
        if ref in seen_refs or identity in seen_names:
            raise ValueError(f"Feature {number}: duplicate source ID or district name.")
        seen_refs.add(ref)
        seen_names.add(identity)
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or geometry.get("type") != source.geometry_type:
            raise ValueError(f"Feature {number}: expected {source.geometry_type}.")
        _validate_coordinates(geometry, dimensions=source.geometry_dimensions)
        collected.append(
            DistrictFeature(
                province_name=province,
                district_name=district,
                source_feature_ref=ref,
                geometry_json=json.dumps(geometry, separators=(",", ":"), default=float),
            )
        )
    return collected
