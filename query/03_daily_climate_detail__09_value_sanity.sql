/*
Title: Daily Climate Detail — Nonfinite and Physical-Boundary Candidates
Context: These values are daily transformed ERA5-Land outputs. Negative accumulated outputs are retained by the pipeline with a warning, not silently fixed.
Purpose: Nonfinite and Physical-Boundary Candidates.
Tables: climate_data.daily_climate_detail.
Grain: One row per observation with an unusual numeric value.
Scope/parameters: All non-NULL observations; no arbitrary climate-specific threshold is applied.
Expected result: Zero rows ideal for nonfinite values and swvl1 outside [0,1]; negative precipitation/radiation are reported for review, not asserted as data corruption.
Interpretation: Nonfinite and swvl1 bounds are flagged as anomalies; negatives are INFO only because pipeline retains negative accumulations.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT daily_climate_id, valid_time_local, latitude, longitude,
 tp_daily_total_mm, ssrd_daily_total_j_m2, swvl1_daily_mean_m3_m3,
 CASE
  WHEN NOT isfinite(tp_daily_total_mm) OR NOT isfinite(t2m_daily_mean_c)
    OR NOT isfinite(d2m_daily_mean_c) OR NOT isfinite(swvl1_daily_mean_m3_m3)
    OR NOT isfinite(ssrd_daily_total_j_m2) THEN 'NONFINITE_VALUE'
  WHEN swvl1_daily_mean_m3_m3 < 0 OR swvl1_daily_mean_m3_m3 > 1
    THEN 'SOIL_WATER_OUTSIDE_0_1'
  ELSE 'NEGATIVE_ACCUMULATION'
 END AS issue,
 CASE WHEN NOT isfinite(tp_daily_total_mm) OR NOT isfinite(t2m_daily_mean_c)
    OR NOT isfinite(d2m_daily_mean_c) OR NOT isfinite(swvl1_daily_mean_m3_m3)
    OR NOT isfinite(ssrd_daily_total_j_m2)
    OR swvl1_daily_mean_m3_m3 < 0 OR swvl1_daily_mean_m3_m3 > 1
    THEN 'FAIL' ELSE 'INFO' END AS result
FROM climate_data.daily_climate_detail
WHERE NOT isfinite(tp_daily_total_mm) OR NOT isfinite(t2m_daily_mean_c)
 OR NOT isfinite(d2m_daily_mean_c) OR NOT isfinite(swvl1_daily_mean_m3_m3)
 OR NOT isfinite(ssrd_daily_total_j_m2)
 OR swvl1_daily_mean_m3_m3 < 0 OR swvl1_daily_mean_m3_m3 > 1
 OR tp_daily_total_mm < 0 OR ssrd_daily_total_j_m2 < 0
ORDER BY daily_climate_id, valid_time_local, latitude, longitude
LIMIT 100;
