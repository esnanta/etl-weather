/*
Title: Daily Climate — Partition Counts by Start Quarter
Context: The historical campaign selects quarterly batches and loads
    monthly request partitions into daily_climate.
Purpose: Count persisted logical request partitions grouped by calendar
    year, start quarter, and source/dataset.
Tables: climate_data.daily_climate.
Grain: One row per start-year, start-quarter, source and dataset.
Scope/parameters: All recorded partitions, including manual or overlapping
    requests. This is NOT filtered to a specific campaign configuration.
Expected result: Nonnegative counts; for a strictly monthly campaign
    with three distinct loaded requests per quarter, count is normally 3.
    Other counts are legitimate for manual/overlapping request partitions.
Interpretation: INFO; do not infer quarter completion from this count.
Limitations: Grouping uses the request start_date and does not prove that
    its entire interval belongs to one quarter or that progress is valid.
Read-only: Yes; SELECT only.
*/
SELECT
    EXTRACT(YEAR FROM start_date)::integer AS start_year,
    EXTRACT(QUARTER FROM start_date)::integer AS start_quarter,
    source::text AS source,
    dataset_name,
    COUNT(*) AS partition_count,
    MIN(start_date) AS earliest_start_date,
    MAX(end_date) AS latest_end_date
FROM climate_data.daily_climate
GROUP BY
    EXTRACT(YEAR FROM start_date),
    EXTRACT(QUARTER FROM start_date),
    source,
    dataset_name
ORDER BY start_year, start_quarter, source, dataset_name;
