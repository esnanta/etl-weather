/*
Title: Daily Climate Detail — Essential Data Quality
Context: ERA5-Land observations are uniquely identified within each request
    by UTC timestamp and grid coordinates. Local midnight uses UTC+7.
Purpose: Report partition-level counts of duplicate observation keys,
    missing local dates, invalid UTC+7 boundaries, and observations outside
    the partition's declared local date range.
Tables: climate_data.daily_climate, climate_data.daily_climate_detail.
Grain: One row per stored metadata partition.
Scope/parameters: All partitions, including partitions with no detail rows.
Expected result: An entirely populated and consistent partition has zero
    duplicate_keys, missing_days, time_errors and out_of_period_rows, and
    result = PASS. An empty partition is NOT_EVALUATED rather than PASS.
Interpretation: FAIL indicates a recorded invariant violation. PASS does not
    prove scientific validity, per-day spatial completeness, or checksum match.
Limitations: Overlapping distinct request_keys may contain shared time/grid
    coordinates legitimately; duplicates are scoped per daily_climate_id.
Read-only: Yes; SELECT only.
*/
WITH counts AS (
    SELECT
        dc.id AS daily_climate_id,
        dc.start_date,
        dc.end_date,
        COUNT(d.id) AS observation_count,
        COUNT(DISTINCT d.valid_time_local::date) FILTER (
            WHERE d.valid_time_local::date BETWEEN dc.start_date AND dc.end_date
        ) AS observed_days,
        COUNT(d.id) FILTER (
            WHERE d.valid_time_local <> d.valid_time + INTERVAL '7 hours'
               OR d.valid_time_local::time <> TIME '00:00'
        ) AS time_errors,
        COUNT(d.id) FILTER (
            WHERE d.valid_time_local::date < dc.start_date
               OR d.valid_time_local::date > dc.end_date
        ) AS out_of_period_rows
    FROM climate_data.daily_climate AS dc
    LEFT JOIN climate_data.daily_climate_detail AS d
        ON d.daily_climate_id = dc.id
    GROUP BY dc.id, dc.start_date, dc.end_date
),
duplicates AS (
    SELECT daily_climate_id, SUM(duplicate_count - 1) AS extra_rows
    FROM (
        SELECT daily_climate_id, valid_time, latitude, longitude,
               COUNT(*) AS duplicate_count
        FROM climate_data.daily_climate_detail
        GROUP BY daily_climate_id, valid_time, latitude, longitude
        HAVING COUNT(*) > 1
    ) AS duplicate_groups
    GROUP BY daily_climate_id
)
SELECT
    c.daily_climate_id, c.start_date, c.end_date, c.observation_count,
    (c.end_date - c.start_date + 1 - c.observed_days) AS missing_days,
    COALESCE(dup.extra_rows, 0) AS duplicate_keys,
    c.time_errors,
    c.out_of_period_rows,
    CASE
        WHEN c.observation_count = 0 THEN 'NOT_EVALUATED'
        WHEN c.end_date - c.start_date + 1 <> c.observed_days
          OR COALESCE(dup.extra_rows, 0) <> 0
          OR c.time_errors <> 0 OR c.out_of_period_rows <> 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS result
FROM counts AS c
LEFT JOIN duplicates AS dup ON dup.daily_climate_id = c.daily_climate_id
ORDER BY c.start_date, c.daily_climate_id;
