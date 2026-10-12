# PostgreSQL Operational Audit Queries

This catalog provides **seven read-only checks** for the active ERA5-Land ETL.
Run each file as a separate task, in numeric order, on the intended database.
The code and examples target the `climate_data` schema in PostgreSQL 16.

| Order | File | Question answered |
| --- | --- | --- |
| 00 | `00_connection__01_database_context.sql` | Am I connected to the intended PostgreSQL database? |
| 01 | `01_schema__01_inventory.sql` | Do all five application tables exist? |
| 02 | `02_daily_climate__01_partition_summary.sql` | Which daily-climate request partitions exist, and how many days/grid observations do they contain? |
| 03 | `03_daily_climate_detail__01_data_quality.sql` | Are there missing local dates, duplicate observation keys, or invalid UTC+7 timestamps? |
| 04 | `04_pipeline_progress__01_campaign_status.sql` | Which campaign batches are complete, incomplete, or failed? |
| 05 | `05_pipeline_progress_detail__01_load_verification.sql` | Do completed load records match the persisted daily data and Parquet metadata? |
| 06 | `06_p2__01_standalone_pilot_verification.sql` | Did the three-day May 2024 standalone pilot persist 530 grids per day without duplicate keys, orphans, or UTC+7 time errors? |

## How to run

In PhpStorm, select the **weather** database for the current development
host, then execute each file independently. Start with query 00; it confirms
the selected database and reports PostgreSQL version, user, and read-only mode.
Its expected database name is `weather`, which must be adjusted if the
deployment intentionally uses another database.

Prefer a SELECT-only database user or explicitly read-only transactions.
The existing connection may report `transaction_read_only=off` even though
these files contain SELECT statements only. Never run them against the
administrative `postgres` database by mistake.

## Large-dataset result limits

**Use `LIMIT 1000` for any query that displays individual observation
records (a SELECT-all-style preview).** Apply an explicit deterministic
`ORDER BY` before `LIMIT`; use a selective date/request scope and indexed
keys for efficient inspection. Never use an unrestricted `SELECT *` over
`daily_climate_detail` as the campaign grows to millions of rows.

**Do not limit the input rows of aggregate or integrity checks.** Partition
counts, duplicate detection, missing-day checks, load reconciliation and
campaign totals must examine all relevant records so a problem after the first
1,000 rows is not concealed. These six operational queries return compact
summaries, rather than individual observations; a blanket `LIMIT 1000`
on them would hide later partitions when the campaign grows.

For any future detailed inspection query, use `ORDER BY ... LIMIT 1000`
at the final result stage and document that the result is a **sample**, not
a complete validation. Large aggregate checks should be reviewed with
`EXPLAIN` and appropriate indexes as data volume increases.

## Output interpretation

- `PASS`: the query's stated invariant holds in its declared scope.
- `FAIL`: the query found an explicit integrity mismatch.
- `INFO`: a descriptive result, not proof of correctness.
- `NOT_EVALUATED`: evidence is absent or the batch is not yet complete.

The partition summary counts **observed** grid coordinates and must not demand
all 1,505 possible coordinate pairs per day. Missing dates are checked,
but scientific completeness and stable per-day grid membership require
additional investigation if a problem is suspected.

The data quality report combines a few core checks into one row per partition:
duplicate observation keys, missing local dates, UTC+7 offset/midnight
violations and dates outside the declared request period. A PASS does not
prove all scientific values are plausible or non-NULL.

The load verification query checks the persisted rows, request metadata,
period, transformed artifact path and SHA256 against operational progress.
An incomplete or pending partition returns NOT_EVALUATED. This is not a
filesystem checksum verification.

## SQL documentation rules

Every SQL file begins with an English block comment containing **Title,
Context, Purpose, Tables, Grain, Scope/parameters, Expected result,
Interpretation, Limitations, Read-only**. SQL has no Python docstring syntax;
this comment serves the equivalent documentation purpose.

All six files contain SELECT statements only and avoid hard-coded IDs and
fixed observation counts. They do not create monthly tables, run EDA, or modify
Airflow or climate observations.

## Source definitions

The audit maps to `pipeline/database/schema.py`,
`pipeline/loaders/daily_climate_postgres.py`,
`orchestration/repositories/pipeline_progress.py`, and
`orchestration/repositories/submission_pacing.py` on branch `dev`.

The operator previously confirmed nine persisted monthly partitions for
January–September 2016 and a healthy UTC+7 offset check. **The four newly
consolidated files have not yet been executed against the database**; their
results should be reviewed before closing the verification checklist.

The P2 pilot query (06) is deliberately scoped to the recorded request key and returns three daily rows with NULL counts. An empty result means that the specified request key is missing; it does not mean PASS. It does not replace a fresh-database bootstrap check or independent Airflow-state verification.
