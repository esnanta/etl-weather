/*
Title: Daily Climate — Recent Partition Preview
Context: Each request_key identifies one logical daily-climate partition.
    A repeated identical request should reuse its existing metadata row.
Purpose: Display a bounded sample of recently transformed partitions.
Tables: climate_data.daily_climate.
Grain: One result row per logical request partition.
Scope/parameters: The 20 most recently transformed partitions; edit LIMIT.
Expected result: Up to 20 rows ordered by transformed_at descending.
    Empty table legitimately returns zero rows.
Interpretation: INFO only; check periods, dataset, source and variable
    signatures. A row does not alone prove observation completeness.
Limitations: Metadata preview does not validate the transformed artifact.
Read-only: Yes; SELECT only.
*/
SELECT
    id AS daily_climate_id,
    start_date,
    end_date,
    source::text AS source,
    dataset_name,
    variable_signature,
    request_key,
    transformed_at
FROM climate_data.daily_climate
ORDER BY transformed_at DESC, id DESC
LIMIT 20;
