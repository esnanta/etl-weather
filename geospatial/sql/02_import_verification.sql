-- P4.4 Administrative importer verification (read-only, weather.climate_data)
-- Explicit session path for PhpStorm; no manual SET is required.
-- Schema-qualified PostGIS functions also work when only the SELECT is executed.
SET search_path TO climate_data, public;

WITH district_audit AS (
  SELECT COUNT(*) AS districts,
         COUNT(DISTINCT source_feature_ref) AS refs,
         COUNT(*) FILTER (
           WHERE NOT public.ST_IsValid(boundary_geometry)
              OR public.ST_IsEmpty(boundary_geometry)
              OR public.ST_NDims(boundary_geometry) <> 3
              OR public.ST_SRID(boundary_geometry) <> 4326
              OR public.ST_GeometryType(boundary_geometry) <> 'ST_MultiPolygon'
         ) AS invalid_geometries,
         COUNT(*) FILTER (WHERE public.ST_Z(public.ST_PointN(public.ST_ExteriorRing(
             public.ST_GeometryN(boundary_geometry, 1)), 1)) IS DISTINCT FROM 0)
             AS unexpected_first_z
  FROM climate_data.t_district
  WHERE source_dataset = 'id_aceh_districts'
    AND source_version = 'unverified'
),
province_audit AS (
  SELECT COUNT(*) AS provinces,
         COUNT(*) FILTER (
           WHERE NOT public.ST_IsValid(boundary_geometry)
              OR public.ST_NDims(boundary_geometry) <> 3
              OR public.ST_SRID(boundary_geometry) <> 4326
         ) AS invalid_geometries
  FROM climate_data.t_province
  WHERE country_code = 'id' AND province_name = 'Aceh'
)
SELECT p.provinces, p.invalid_geometries AS invalid_provinces,
       d.districts, d.refs AS unique_refs,
       d.invalid_geometries AS invalid_districts,
       d.unexpected_first_z,
       CASE WHEN p.provinces = 1
             AND d.districts = 23 AND d.refs = 23
             AND p.invalid_geometries = 0 AND d.invalid_geometries = 0
             AND d.unexpected_first_z = 0
            THEN 'PASS' ELSE 'FAIL' END AS validation_status
FROM district_audit d CROSS JOIN province_audit p;
