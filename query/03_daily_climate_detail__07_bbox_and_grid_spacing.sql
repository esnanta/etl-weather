/*
Title: Daily Climate Detail — Bounding Box and Coordinate Alignment
Context: ERA5-Land output is on a regular 0.1-degree grid; check each row against its own request bbox, not a global fixed box.
Purpose: Bounding Box and Coordinate Alignment.
Tables: climate_data.daily_climate; climate_data.daily_climate_detail.
Grain: One row per coordinate outside the request bbox or off 0.1-degree alignment.
Scope/parameters: All persisted observations; epsilon accommodates floating-point storage.
Expected result: Zero anomaly rows if coordinates lie in the request bounds and on 0.1-degree coordinate lines.
Interpretation: Grid geometry does not prove completeness; no requirement for 1,505 populated coordinates.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT d.daily_climate_id, d.valid_time_local, d.latitude, d.longitude,
 dc.north_lat, dc.south_lat, dc.west_lon, dc.east_lon,
 CASE
  WHEN d.latitude < dc.south_lat - 1e-7 OR d.latitude > dc.north_lat + 1e-7
    OR d.longitude < dc.west_lon - 1e-7 OR d.longitude > dc.east_lon + 1e-7
    THEN 'OUTSIDE_BBOX'
  ELSE 'OFF_GRID'
 END AS issue, 'FAIL' AS result
FROM climate_data.daily_climate_detail d
JOIN climate_data.daily_climate dc ON dc.id = d.daily_climate_id
WHERE d.latitude < dc.south_lat - 1e-7 OR d.latitude > dc.north_lat + 1e-7
   OR d.longitude < dc.west_lon - 1e-7 OR d.longitude > dc.east_lon + 1e-7
   OR abs(d.latitude * 10 - round((d.latitude * 10)::numeric)) > 1e-6
   OR abs(d.longitude * 10 - round((d.longitude * 10)::numeric)) > 1e-6
ORDER BY d.daily_climate_id, d.valid_time_local, d.latitude, d.longitude
LIMIT 100;
