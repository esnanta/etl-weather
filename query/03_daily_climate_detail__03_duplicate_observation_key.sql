/*
Title: Daily Climate Detail — Duplicate Observation Keys
Context: Each observation is uniquely identified within its logical partition by UTC timestamp and coordinate.
Purpose: Duplicate Observation Keys.
Tables: climate_data.daily_climate_detail.
Grain: One row per repeated composite key.
Scope/parameters: All detail records.
Expected result: Zero rows expected (PASS); any row indicates a duplicate (FAIL).
Interpretation: An empty result is healthy; overlapping distinct partitions are not duplicates by this key.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT daily_climate_id, valid_time, latitude, longitude,
 COUNT(*) AS duplicate_count, 'FAIL' AS result
FROM climate_data.daily_climate_detail
GROUP BY daily_climate_id, valid_time, latitude, longitude
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, daily_climate_id, valid_time, latitude, longitude;
