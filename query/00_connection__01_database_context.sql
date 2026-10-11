/*
Title: Database Connection — Target Context
Context: Check the active connection before any climate_data audit.
Purpose: Target Context.
Tables: pg_catalog current database and session settings.
Grain: One row for the current connection.
Scope/parameters: Database name, user, PostgreSQL version, read-only setting and target match.
Expected result: database_name should be weather for the operator's current development host; target_matches = true. Transaction read-only may be off in an IDE session; use a read-only role/transaction for audits.
Interpretation: A wrong target is FAIL; transaction_read_only=false is a safety warning, not proof any writes occurred.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
SELECT current_database() AS database_name, current_user AS database_user,
       current_setting('server_version') AS postgres_version,
       current_setting('transaction_read_only') AS transaction_read_only,
       CASE WHEN current_database() = 'weather' THEN 'PASS' ELSE 'FAIL' END AS target_result,
       CASE WHEN current_setting('transaction_read_only') = 'on' THEN 'PASS'
            ELSE 'INFO' END AS transaction_safety_result;
