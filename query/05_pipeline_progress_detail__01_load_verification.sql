/*
Title: Pipeline Progress Detail — Persisted Load Verification
Context: Airflow progress details store the expected row count and transformed
    Parquet provenance for each request. daily_climate and its observations
    provide independent persisted load evidence for that request_key.
Purpose: Compare each progress partition against matching daily-climate
    metadata, artifact identity and actual observation row count.
Tables: climate_data.pipeline_progress_detail,
    climate_data.pipeline_progress, climate_data.daily_climate,
    climate_data.daily_climate_detail.
Grain: One row per progress detail partition.
Scope/parameters: All progress partitions associated with the ERA5-Land
    CDS dataset. Pending/running work is included.
Expected result: A completed and valid loaded partition should match on
    source, dataset, period, artifact path, SHA256 and observation row count,
    with verification_result = PASS. Incomplete partitions are NOT_EVALUATED;
    a mismatch in a reported complete/valid partition is FAIL.
Interpretation: This is database evidence only; it does not read a local
    Parquet file or prove that scientific transformations were correct.
Limitations: Other datasets or standalone loads without campaign progress
    do not appear in this report.
Read-only: Yes; SELECT only.
*/
WITH observations AS (
    SELECT daily_climate_id, COUNT(*) AS actual_rows
    FROM climate_data.daily_climate_detail
    GROUP BY daily_climate_id
)
SELECT
    progress.id AS pipeline_progress_id,
    detail.id AS progress_detail_id,
    detail.start_date, detail.end_date,
    detail.load_status, detail.validation_status,
    detail.row_count AS expected_rows,
    COALESCE(obs.actual_rows, 0) AS actual_rows,
    dc.id AS daily_climate_id,
    CASE
        WHEN detail.load_status <> 'complete'
          OR detail.validation_status <> 'valid' THEN 'NOT_EVALUATED'
        WHEN dc.id IS NULL
          OR dc.source::text <> progress.source
          OR dc.dataset_name <> progress.dataset_name
          OR dc.start_date <> detail.start_date
          OR dc.end_date <> detail.end_date
          OR dc.transformed_artifact_path IS DISTINCT FROM detail.transformed_artifact_path
          OR dc.transformed_sha256 IS DISTINCT FROM detail.transformed_sha256
          OR detail.row_count IS NULL OR detail.row_count <= 0
          OR COALESCE(obs.actual_rows, 0) <> detail.row_count THEN 'FAIL'
        ELSE 'PASS'
    END AS verification_result
FROM climate_data.pipeline_progress_detail AS detail
JOIN climate_data.pipeline_progress AS progress
  ON progress.id = detail.pipeline_progress_id
LEFT JOIN climate_data.daily_climate AS dc
  ON dc.request_key = detail.request_key
LEFT JOIN observations AS obs
  ON obs.daily_climate_id = dc.id
WHERE progress.dataset_name = 'reanalysis-era5-land'
  AND progress.source = 'cds'
ORDER BY detail.start_date, detail.end_date, detail.id;
