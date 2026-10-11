/*
Title: Daily Climate Detail — Recent Observation Preview
Context: Daily rows are keyed by a partition, UTC boundary and coordinate.
Purpose: Recent Observation Preview.
Tables: climate_data.daily_climate_detail; climate_data.daily_climate.
Grain: At most 30 individual observations.
Scope/parameters: Latest 30 observations ordered by local day, partition and grid coordinate.
Expected result: Up to 30 rows; an empty database returns zero rows.
Interpretation: INFO only; values may be NULL and do not imply zero.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT dc.start_date AS partition_start, dc.end_date AS partition_end,
 d.daily_climate_id, d.valid_time, d.valid_time_local, d.latitude, d.longitude,
 d.tp_daily_total_mm, d.t2m_daily_mean_c, d.d2m_daily_mean_c,
 d.swvl1_daily_mean_m3_m3, d.ssrd_daily_total_j_m2
FROM climate_data.daily_climate_detail d
JOIN climate_data.daily_climate dc ON dc.id = d.daily_climate_id
ORDER BY d.valid_time_local DESC, d.daily_climate_id, d.latitude, d.longitude
LIMIT 30;
