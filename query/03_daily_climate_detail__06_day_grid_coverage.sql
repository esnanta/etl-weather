/*
Title: Daily Climate Detail — Observed Day and Grid Coverage
Context: A partition covers inclusive local dates, but not every one of the 1,505 theoretical grid coordinates must appear.
Purpose: Observed Day and Grid Coverage.
Tables: climate_data.daily_climate; climate_data.daily_climate_detail.
Grain: One row per expected local day of each stored partition.
Scope/parameters: Entire stored partition date ranges, including days with no observations.
Expected result: Every date in a populated complete partition should have observations; zero-day counts are visible. Spatial count is observed, not 1,505.
Interpretation: FAIL for missing daily rows in an existing stored metadata partition; variation in nonzero grid counts is INFO, not automatically failure.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT dc.id AS daily_climate_id, days.local_date,
 COUNT(d.id) AS observation_count,
 COUNT(DISTINCT (d.latitude, d.longitude)) AS grid_point_count,
 CASE WHEN COUNT(d.id) = 0 THEN 'FAIL' ELSE 'INFO' END AS result
FROM climate_data.daily_climate dc
CROSS JOIN LATERAL generate_series(dc.start_date::timestamp, dc.end_date::timestamp,
 INTERVAL '1 day') AS days(local_date)
LEFT JOIN climate_data.daily_climate_detail d
 ON d.daily_climate_id = dc.id AND d.valid_time_local::date = days.local_date::date
GROUP BY dc.id, days.local_date
ORDER BY dc.id, days.local_date;
