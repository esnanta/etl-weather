/*
Title: Daily Climate — Partition Summary
Context: Each logical request is stored once in daily_climate; its daily grid
    observations are stored in daily_climate_detail. The 2016 campaign currently
    contains monthly partitions and can grow while scheduled ETL is running.
Purpose: Summarize monthly request periods, local-day coverage, observed grid
    points and persisted observation counts in one report.
Tables: climate_data.daily_climate, climate_data.daily_climate_detail.
Grain: One row per request_key (logical partition).
Scope/parameters: All stored partitions, ordered by period. No fixed ID or
    expected grid count is assumed.
Expected result: One row for each partition. A completed monthly partition
    normally has observed_days = expected_days and positive observations.
Interpretation: MISSING_DAYS denotes incomplete calendar coverage;
    NO_OBSERVATIONS denotes metadata without detail; COVERED confirms date
    coverage, not scientific correctness or stable grid membership.
Limitations: Grid membership may be smaller than 1,505; counts alone do not
    establish checksum integrity or complete coverage within each day.
Read-only: Yes; SELECT only.
*/
SELECT
    dc.id AS daily_climate_id,
    dc.start_date,
    dc.end_date,
    dc.source::text AS source,
    dc.dataset_name,
    dc.end_date - dc.start_date + 1 AS expected_days,
    COUNT(DISTINCT d.valid_time_local::date) AS observed_days,
    COUNT(DISTINCT (d.latitude, d.longitude)) AS observed_grid_points,
    COUNT(d.id) AS observation_count,
    CASE
        WHEN COUNT(d.id) = 0 THEN 'NO_OBSERVATIONS'
        WHEN COUNT(DISTINCT d.valid_time_local::date) <> dc.end_date - dc.start_date + 1
            THEN 'MISSING_DAYS'
        ELSE 'COVERED'
    END AS coverage_status
FROM climate_data.daily_climate AS dc
LEFT JOIN climate_data.daily_climate_detail AS d
    ON d.daily_climate_id = dc.id
GROUP BY dc.id, dc.start_date, dc.end_date, dc.source, dc.dataset_name
ORDER BY dc.start_date, dc.end_date, dc.id;
