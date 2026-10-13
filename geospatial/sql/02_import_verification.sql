-- P4.4 Administrative importer verification (read-only, weather.climate_data)
WITH district_audit AS (
  SELECT COUNT(*) AS districts,
         COUNT(DISTINCT source_feature_ref) AS refs,
         COUNT(*) FILTER (
           WHERE NOT ST_IsValid(boundary_geometry)
              OR ST_IsEmpty(boundary_geometry)
              OR ST_NDims(boundary_geometry) <> 3
              OR ST_SRID(boundary_geometry) <> 4326
              OR ST_GeometryType(boundary_geometry) <> 'ST_MultiPolygon'
         ) AS invalid_boundary_geometryetries,
         COUNT(*) FILTER (WHERE ST_Z(ST_PointN(ST_ExteriorRing(
             ST_GeometryN(boundary_geometry, 1)), 1)) IS DISTINCT FROM 0)
             AS unexpected_first_z
  FROM climate_data.t_district
  WHERE source_dataset = 'id_aceh_districts'
    AND source_version = 'unverified'
),
province_audit AS (
  SELECT COUNT(*) AS provinces,
         COUNT(*) FILTER (
           WHERE NOT ST_IsValid(boundary_geometry)
              OR ST_NDims(boundary_geometry) <> 3
              OR ST_SRID(boundary_geometry) <> 4326
         ) AS invalid_boundary_geometryetries
  FROM climate_data.t_province
  WHERE country_code = 'id' AND province_name = 'Aceh'
)
SELECT p.provinces, p.invalid_boundary_geometryetries AS invalid_provinces,
       d.districts, d.refs AS unique_refs,
       d.invalid_boundary_geometryetries AS invalid_districts,
       d.unexpected_first_z,
       CASE WHEN p.provinces = 1
             AND d.districts = 23 AND d.refs = 23
             AND p.invalid_boundary_geometryetries = 0 AND d.invalid_boundary_geometryetries = 0
             AND d.unexpected_first_z = 0
            THEN 'PASS' ELSE 'FAIL' END AS validation_status
FROM district_audit d CROSS JOIN province_audit p;
