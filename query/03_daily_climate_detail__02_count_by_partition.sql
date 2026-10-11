/*
Title: Daily Climate Detail — Observation Counts by Partition
Context: The number of observations depends on observed daily grid membership, not the theoretical bbox maximum.
Purpose: Observation Counts by Partition.
Tables: climate_data.daily_climate; climate_data.daily_climate_detail.
Grain: One result per metadata partition.
Scope/parameters: Inclusive expected local days, observed days, distinct coordinates and row counts.
Expected result: One row per partition, including partitions with zero observations; expected days equals end_date - start_date + 1.
Interpretation: INFO; observed days fewer than expected warrant inspection. Grid count is observed and may be smaller than 1,505.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT dc.id AS daily_climate_id, dc.start_date, dc.end_date,
 (dc.end_date - dc.start_date + 1) AS expected_local_days,
 COUNT(DISTINCT d.valid_time_local::date) AS observed_local_days,
 COUNT(DISTINCT (d.latitude, d.longitude)) AS observed_grid_points,
 COUNT(d.id) AS observation_count
FROM climate_data.daily_climate dc
LEFT JOIN climate_data.daily_climate_detail d ON d.daily_climate_id = dc.id
GROUP BY dc.id, dc.start_date, dc.end_date
ORDER BY dc.start_date, dc.id;
