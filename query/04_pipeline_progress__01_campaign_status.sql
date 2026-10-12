/*
Title: Pipeline Progress — Campaign Quarter Status
Context: Each scheduled campaign quarter expects three monthly partitions;
    operational progress is persisted independently of Airflow task history.
Purpose: Show recorded batch completion, validation and failure counts.
Tables: climate_data.t_pipeline_progress.
Grain: One row per persisted progress batch (manual batches may also appear).
Scope/parameters: All progress batches; filtered to ERA5-Land CDS dataset.
Expected result: Completed valid quarters have completed = expected,
    failed = 0, execution_status = complete, validation_status = valid.
Interpretation: COMPLETE indicates recorded completion; INCOMPLETE or FAILED
    warrants inspection of pipeline_progress_detail and persisted observations.
Limitations: Progress counts are operational evidence, not a direct independent
    recount of daily_climate_detail. The load-verification query checks that.
Read-only: Yes; SELECT only.
*/
SELECT
    id AS pipeline_progress_id,
    start_date, end_date,
    expected_partition_count AS expected_months,
    completed_partition_count AS completed_months,
    failed_partition_count AS failed_months,
    execution_status, validation_status, completed_at,
    CASE
        WHEN execution_status = 'complete' AND validation_status = 'valid'
             AND completed_partition_count = expected_partition_count
             AND failed_partition_count = 0 THEN 'COMPLETE'
        WHEN execution_status = 'failed' OR validation_status = 'failed'
             OR failed_partition_count > 0 THEN 'FAILED'
        ELSE 'INCOMPLETE'
    END AS result
FROM climate_data.t_pipeline_progress
WHERE dataset_name = 'reanalysis-era5-land' AND source = 'cds'
ORDER BY start_date, end_date, id;
