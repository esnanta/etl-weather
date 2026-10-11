# PostgreSQL Query Catalog — Standards and Execution Guide

This directory contains **read-only, one-purpose-per-file** PostgreSQL
diagnostics for the ERA5-Land ETL application database. The authoritative
application schema is `climate_data`. These queries inspect database state;
they do not replace production validation, Airflow reconciliation, or tests.

## Data context

The five active application tables are:

| Table | Purpose | Grain |
| --- | --- | --- |
| `climate_data.daily_climate` | Request metadata, periods, bounding boxes, variables, transformed artifact provenance | One logical request key |
| `climate_data.daily_climate_detail` | Canonical daily values for `tp`, `t2m`, `d2m`, `swvl1`, and `ssrd` | One partition / UTC daily boundary / grid coordinate |
| `climate_data.pipeline_progress` | Durable batch/quarter execution and validation state | One batch identity |
| `climate_data.pipeline_progress_detail` | Per-request extract/transform/load status and load evidence | One parent batch / request key |
| `climate_data.submission_pacing` | Most recent globally reserved CDS POST instant | At most one singleton row |

The FK relationships are `daily_climate_detail.daily_climate_id` →
`daily_climate.id` and `pipeline_progress_detail.pipeline_progress_id` →
`pipeline_progress.id`. Progress details are reconciled to daily climate
metadata by matching request keys **plus** dataset, source, period, and
persisted artifact evidence; they do not have a foreign key to daily metadata.

Dates use local-day semantics for `Asia/Jakarta` (UTC+7). The stored
`valid_time` and `valid_time_local` are timestamp *without time zone*.
The current Aceh bounding box covers 43 × 35 = 1,505 possible coordinate
pairs at 0.1° grid spacing; that is **not** a guaranteed observation count.
Null climate values may be legitimate missing observations and must not be
coalesced to zero by diagnostics.

## Filename taxonomy

`<category-number>_<table-or-domain>__<task-number>_<action>.sql`

- `01_schema`: schema, columns, keys, constraints and indexes.
- `02_daily_climate`: request-partition metadata.
- `03_daily_climate_detail`: persisted daily observations.
- `04_pipeline_progress`: batch/campaign progress.
- `05_pipeline_progress_detail`: per-partition progress and evidence.
- `06_submission_pacing`: global submission clock.
- `07_cross_table`: reconciled evidence across table boundaries.

Files sort in intended reading order. **Each file must perform one
diagnostic task**, even if its single result requires joins or CTEs.

## Required file-level SQL documentation

SQL does not support Python docstrings. Each SQL file must start with an
English block comment with the following fields:

```sql
/*
Title: Daily climate — count partitions by period
Context: The ETL stores one metadata row per request_key, even on rerun.
Purpose: Count persisted logical partitions within an explicit date scope.
Tables: climate_data.daily_climate
Grain: One result row per requested period (or group).
Scope/parameters: Inclusive local start/end dates; describe how to edit them.
Expected result: Nonnegative counts; one row per distinct logical request.
Interpretation: Explain normal output and what warrants investigation.
Limitations: A count alone does not prove transformed artifact correctness.
Read-only: Yes; SELECT only.
*/
```

The **Expected result** must describe concrete output shape and the expected
pass condition, not invent a fixed row count for an unknown database. A check
with no anomalies usually returns zero findings; an informational count may
legitimately return zero or more rows. Describe empty-table cases, scope,
assumptions, and ambiguity (including overlapping request partitions).

For programmatic diagnostic status, use:

| Status | Meaning |
| --- | --- |
| `PASS` | The scoped check ran and its explicitly defined invariant holds. |
| `FAIL` | The check ran and produced evidence violating that invariant. |
| `INFO` | Descriptive measurement; no binary assertion is warranted. |
| `NOT_EVALUATED` | Scope/input or sufficient evidence is absent; do not claim a pass. |

Do not label unscoped counts or scientific plausibility observations
`FAIL` without an explicit invariant. Use descriptive output aliases,
stable ordering and bounded previews; avoid `SELECT *`. Never rely on a
hard-coded surrogate ID or fixed row count. Use a clearly documented
`params` CTE when a query needs input values.

## Safe execution

**Read queries against the configured target database only after checking
its name and access permissions.** Prefer a PostgreSQL user with SELECT-only
permissions. For `psql`, connect through an existing secure local connection
method (e.g., a configured libpq service or environment variables); do not
paste passwords into SQL files, command history, or documentation.

Suggested interactive session:

```sql
SELECT current_database() AS database_name,
       current_user AS connected_user,
       current_setting('server_version') AS postgres_version,
       current_setting('transaction_read_only') AS transaction_read_only;

BEGIN READ ONLY;
-- In psql: \i query/02_daily_climate__01_preview.sql
ROLLBACK;
```

The above is an **operator instruction**, not a script that modifies data.
A read-only transaction limits accidental writes; a SELECT-only database
role adds a stronger permission boundary. PostgreSQL version and live schema
must be verified on the target host; no database connection was made when
this guide was prepared.

The legacy `daily_climate_verification.sql` predates this taxonomy and is
**not a generic integrity test**: it assumes ID 1 and 747 observations for
a specific former sanity request. Do not use its fixed expectations for
the current campaign. It will be replaced or removed as the per-task
scripts are implemented.

## Source ownership

Verified against the `dev` source definitions on 2026-10-11:

- `pipeline/database/schema.py`
- `pipeline/loaders/daily_climate_postgres.py`
- `orchestration/repositories/pipeline_progress.py`
- `orchestration/repositories/submission_pacing.py`
- `orchestration/repositories/load_evidence.py`
- root `config.py`

This is documentation only. No database version, data counts, or query
execution results have yet been verified against a live PostgreSQL instance.
