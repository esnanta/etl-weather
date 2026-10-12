/*
Title: P2 Three-Day Standalone Pilot — Persistence Verification
Context: Read-only evidence for the 2024-05-01 to 2024-05-03 CDS ERA5-Land pilot.
Purpose: Verify the persisted partition, its local dates, stable grid membership,
    UTC+7 date alignment, duplicate observation keys and orphan details.
Tables: climate_data.t_daily_climate, climate_data.t_daily_climate_detail.
Grain: One row per local day, plus aggregate request metadata repeated per day.
Scope/parameters: Exact request_key from the logged P2 pilot. Run against the
    intended development database; verify connection context separately.
Expected result: Three rows, each with 530 observations and 530 distinct grid
    points, partition total 1590, zero duplicate keys, zero orphan rows and
    zero timestamp mismatches. The 1505 RAW bbox points are not the expected
    count of complete-variable daily observations.
Interpretation: Any discrepancy requires investigation before P2 closure.
Limitations: Counts do not independently verify filesystem SHA-256 or prove
    the absence of simultaneous Airflow execution; missing weather values
    must remain NULL and are counted separately here.
Read-only: Yes; SELECT only.
*/
WITH partition AS (
    SELECT id, request_key, start_date, end_date, transformed_artifact_path,
           transformed_sha256
    FROM climate_data.t_daily_climate
    WHERE request_key = '9adf53f8b3b0070a6c922df115c10829ca6b004561ce54cc56348ff6d30a22f0'
),
observations AS (
    SELECT d.*
    FROM climate_data.t_daily_climate_detail AS d
    JOIN partition AS p ON p.id = d.daily_climate_id
),
daily AS (
    SELECT valid_time_local::date AS local_date,
           COUNT(*) AS observation_count,
           COUNT(DISTINCT (latitude, longitude)) AS distinct_grid_points,
           COUNT(*) FILTER (WHERE valid_time_local - valid_time <> INTERVAL '7 hours'
                              OR valid_time_local::time <> TIME '00:00:00') AS time_errors,
           COUNT(*) FILTER (WHERE tp_daily_total_mm IS NULL) AS tp_nulls,
           COUNT(*) FILTER (WHERE t2m_daily_mean_c IS NULL) AS t2m_nulls,
           COUNT(*) FILTER (WHERE d2m_daily_mean_c IS NULL) AS d2m_nulls,
           COUNT(*) FILTER (WHERE swvl1_daily_mean_m3_m3 IS NULL) AS swvl1_nulls,
           COUNT(*) FILTER (WHERE ssrd_daily_total_j_m2 IS NULL) AS ssrd_nulls
    FROM observations
    GROUP BY valid_time_local::date
),
duplicate_groups AS (
    SELECT COUNT(*) AS duplicate_keys
    FROM (
        SELECT daily_climate_id, valid_time, latitude, longitude
        FROM observations
        GROUP BY daily_climate_id, valid_time, latitude, longitude
        HAVING COUNT(*) > 1
    ) AS duplicates
),
orphans AS (
    SELECT COUNT(*) AS orphan_count
    FROM climate_data.t_daily_climate_detail AS d
    LEFT JOIN climate_data.t_daily_climate AS p ON p.id = d.daily_climate_id
    WHERE p.id IS NULL
),
totals AS (
    SELECT COUNT(*) AS total_observations,
           COUNT(DISTINCT valid_time_local::date) AS total_local_days
    FROM observations
)
SELECT p.id AS daily_climate_id, p.start_date, p.end_date, p.request_key,
       p.transformed_artifact_path, p.transformed_sha256,
       t.total_observations, t.total_local_days, d.local_date, d.observation_count,
       d.distinct_grid_points, d.time_errors, d.tp_nulls, d.t2m_nulls,
       d.d2m_nulls, d.swvl1_nulls, d.ssrd_nulls,
       dup.duplicate_keys, o.orphan_count,
       CASE WHEN p.start_date = DATE '2024-05-01'
                 AND p.end_date = DATE '2024-05-03'
                 AND t.total_observations = 1590
                 AND t.total_local_days = 3
                 AND d.observation_count = 530
                 AND d.distinct_grid_points = 530
                 AND d.time_errors = 0
                 AND dup.duplicate_keys = 0
                 AND o.orphan_count = 0
            THEN 'PASS' ELSE 'FAIL' END AS validation_status
FROM partition AS p
CROSS JOIN totals AS t
CROSS JOIN duplicate_groups AS dup
CROSS JOIN orphans AS o
JOIN daily AS d ON d.local_date BETWEEN p.start_date AND p.end_date
ORDER BY d.local_date;
