/*
Title: Daily Climate — Duplicate Request Identity Check
Context: A unique index enforces one daily_climate metadata row per
    deterministic request_key, regardless of repeated ETL execution.
Purpose: Find request keys appearing in more than one metadata row.
Tables: climate_data.daily_climate.
Grain: One result row per duplicated request_key.
Scope/parameters: Entire daily_climate table.
Expected result: Zero rows means PASS. Any returned request_key has
    duplicate_count > 1 and means FAIL.
Interpretation: An empty result is expected with the unique index intact.
Limitations: Does not detect different request keys with overlapping
    spatial/temporal coverage, nor duplicate detail observations.
Read-only: Yes; SELECT only.
*/
SELECT
    request_key,
    COUNT(*) AS duplicate_count,
    MIN(id) AS first_daily_climate_id,
    MAX(id) AS last_daily_climate_id,
    'FAIL' AS result
FROM climate_data.daily_climate
GROUP BY request_key
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, request_key;
