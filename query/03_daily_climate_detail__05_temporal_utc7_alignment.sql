/*
Title: Daily Climate Detail — UTC+7 Local-Day Alignment
Context: ERA5-Land daily local midnight is stored as local timestamp without time zone; UTC boundary is seven hours earlier.
Purpose: UTC+7 Local-Day Alignment.
Tables: climate_data.daily_climate; climate_data.daily_climate_detail.
Grain: One row per observation with an incorrect timestamp or out-of-range local day.
Scope/parameters: All persisted daily records.
Expected result: Zero anomaly rows. valid_time_local = valid_time + 7 hours; local time must be 00:00 and within partition dates.
Interpretation: FAIL rows identify a concrete mismatch with the active UTC+7 transformation contract.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT d.daily_climate_id, d.valid_time, d.valid_time_local,
 dc.start_date, dc.end_date, d.latitude, d.longitude,
 CASE
  WHEN d.valid_time_local <> d.valid_time + INTERVAL '7 hours' THEN 'UTC_OFFSET_MISMATCH'
  WHEN d.valid_time_local::time <> TIME '00:00:00' THEN 'NOT_LOCAL_MIDNIGHT'
  ELSE 'OUTSIDE_PARTITION'
 END AS issue, 'FAIL' AS result
FROM climate_data.daily_climate_detail d
JOIN climate_data.daily_climate dc ON dc.id = d.daily_climate_id
WHERE d.valid_time_local <> d.valid_time + INTERVAL '7 hours'
   OR d.valid_time_local::time <> TIME '00:00:00'
   OR d.valid_time_local::date < dc.start_date
   OR d.valid_time_local::date > dc.end_date
ORDER BY d.daily_climate_id, d.valid_time_local, d.latitude, d.longitude
LIMIT 100;
