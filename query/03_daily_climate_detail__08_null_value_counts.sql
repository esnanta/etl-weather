/*
Title: Daily Climate Detail — Null Counts per Variable
Context: Scientific missing values are persisted as SQL NULL rather than substituted with zero.
Purpose: Null Counts per Variable.
Tables: climate_data.daily_climate; climate_data.daily_climate_detail.
Grain: One row per partition.
Scope/parameters: All partitions, including empty metadata partitions.
Expected result: Count and percentage of NULL observations for each of five variables; percentages NULL for an empty partition.
Interpretation: INFO; NULL counts alone do not establish corruption or require imputation.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT dc.id AS daily_climate_id, dc.start_date, dc.end_date,
 COUNT(d.id) AS total_rows,
 COUNT(d.id) FILTER (WHERE d.tp_daily_total_mm IS NULL) AS tp_nulls,
 COUNT(d.id) FILTER (WHERE d.t2m_daily_mean_c IS NULL) AS t2m_nulls,
 COUNT(d.id) FILTER (WHERE d.d2m_daily_mean_c IS NULL) AS d2m_nulls,
 COUNT(d.id) FILTER (WHERE d.swvl1_daily_mean_m3_m3 IS NULL) AS swvl1_nulls,
 COUNT(d.id) FILTER (WHERE d.ssrd_daily_total_j_m2 IS NULL) AS ssrd_nulls,
 ROUND(100.0 * COUNT(d.id) FILTER (WHERE d.tp_daily_total_mm IS NULL) / NULLIF(COUNT(d.id), 0), 2) AS tp_null_pct,
 'INFO' AS result
FROM climate_data.daily_climate dc
LEFT JOIN climate_data.daily_climate_detail d ON dc.id = d.daily_climate_id
GROUP BY dc.id, dc.start_date, dc.end_date
ORDER BY dc.start_date, dc.id;
