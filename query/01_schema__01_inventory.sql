/*
Title: Application Schema — Table Inventory
Context: The ERA5-Land ETL and Airflow progress subsystem share the
    climate_data schema, which currently defines five application tables.
Purpose: List the expected application tables and whether each exists.
Tables: pg_catalog.pg_namespace, pg_catalog.pg_class (system catalogs).
Grain: One row per expected application table.
Scope/parameters: The fixed application schema climate_data.
Expected result: Five rows; relation_exists = true and relation_kind = 'r'
    for each table in a normally initialized database.
Interpretation: PASS means that a base table is present; FAIL means the
    expected name is missing or does not resolve to a regular table.
Limitations: Presence does not prove expected columns, constraints, or rows.
Read-only: Yes; SELECT only.
*/
WITH expected_tables (table_name) AS (
    VALUES
        ('t_daily_climate'),
        ('t_daily_climate_detail'),
        ('t_pipeline_progress'),
        ('t_pipeline_progress_detail'),
        ('t_submission_pacing')
)
SELECT
    'climate_data' AS schema_name,
    expected.table_name,
    actual.relkind AS relation_kind,
    (actual.oid IS NOT NULL) AS relation_exists,
    CASE WHEN actual.relkind = 'r' THEN 'PASS' ELSE 'FAIL' END AS result
FROM expected_tables AS expected
LEFT JOIN pg_catalog.pg_namespace AS ns
    ON ns.nspname = 'climate_data'
LEFT JOIN pg_catalog.pg_class AS actual
    ON actual.relnamespace = ns.oid
   AND actual.relname = expected.table_name
ORDER BY expected.table_name;
