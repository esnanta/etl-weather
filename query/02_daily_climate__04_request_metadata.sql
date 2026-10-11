/*
Title: Daily Climate — Request Metadata and Artifact Provenance
Context: A partition's request identity includes source, dataset, dates,
    variables, and bbox. The transformed Parquet path and SHA256 record
    the persisted artifact identity used in load evidence reconciliation.
Purpose: Inspect request metadata and provenance for a selected period.
Tables: climate_data.daily_climate.
Grain: One row per persisted request_key overlapping the selected dates.
Scope/parameters: Edit start_date/end_date in params (inclusive), without
    assuming any daily_climate_id. Default is the observed 2016 campaign year.
Expected result: All matching partitions with nonempty artifact path and
    SHA256 (both NOT NULL by schema), and their exact request metadata.
    Zero rows is informational if this date scope contains no partitions.
Interpretation: INFO. Bbox and variables may legitimately vary between
    requests; compare against their intended request contract.
Limitations: SQL cannot verify file existence or calculate a filesystem
    checksum. Path/checksum equality with progress is a later cross-table task.
Read-only: Yes; SELECT only.
*/
WITH params AS (
    SELECT DATE '2016-01-01' AS start_date,
           DATE '2016-12-31' AS end_date
)
SELECT
    dc.id AS daily_climate_id,
    dc.request_key,
    dc.source::text AS source,
    dc.dataset_name,
    dc.start_date,
    dc.end_date,
    dc.variable_signature,
    dc.north_lat,
    dc.south_lat,
    dc.west_lon,
    dc.east_lon,
    dc.downloaded_at,
    dc.transformed_at,
    dc.transformed_artifact_path,
    dc.transformed_sha256
FROM climate_data.daily_climate AS dc
CROSS JOIN params AS p
WHERE dc.start_date <= p.end_date
  AND dc.end_date >= p.start_date
ORDER BY dc.start_date, dc.end_date, dc.id;
