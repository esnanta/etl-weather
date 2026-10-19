# PostgreSQL Operational Audit Queries

This directory contains seven read-only SQL checks for the active ERA5-Land ETL.

For database setup and project-level commands, see [`/README.md`](../README.md).

## Query Catalog

| Order | File | Purpose |
| --- | --- | --- |
| 00 | `00_connection__01_database_context.sql` | Confirm the intended PostgreSQL database and connection context |
| 01 | `01_schema__01_inventory.sql` | Check the five core daily/operational audit tables |
| 02 | `02_daily_climate__01_partition_summary.sql` | Summarize persisted daily request partitions |
| 03 | `03_daily_climate_detail__01_data_quality.sql` | Check dates, duplicate keys, and UTC+7 timestamp rules |
| 04 | `04_pipeline_progress__01_campaign_status.sql` | Inspect campaign-quarter status |
| 05 | `05_pipeline_progress_detail__01_load_verification.sql` | Reconcile completed load evidence with persisted climate data |
| 06 | `06_p2__01_standalone_pilot_verification.sql` | Verify the scoped standalone pilot request |

The five tables checked by query 01 are the core daily/operational audit scope, not the complete bootstrapped application schema.

## How to Run

Execute each SQL file independently, in numeric order, against the intended application database.

Start with query 00 so the selected database, user, PostgreSQL version, and read-only state are visible before interpreting later results.

Prefer a SELECT-only database user or an explicitly read-only transaction.

Never run the audit against the administrative `postgres` database by mistake.

## Result Limits

Use `ORDER BY ... LIMIT 1000` when displaying individual observation records.

Do not limit the input rows of aggregate or integrity checks. Counts, duplicate checks, missing-day checks, campaign totals, and reconciliation must inspect the full relevant scope.

A limited detail query is a sample, not complete validation.

## Interpretation

- `PASS` — the declared invariant holds for the query scope;
- `FAIL` — an explicit mismatch was found;
- `INFO` — descriptive output only;
- `NOT_EVALUATED` — required evidence is absent or incomplete.

A PASS does not prove every scientific value is plausible or non-null.

## Important Limitations

Partition summaries count observed grid coordinates and must not assume that every theoretical coordinate pair exists every day.

Load verification checks database metadata and recorded artifact identity; it does not independently recompute filesystem checksums.

Query 06 is intentionally scoped to its recorded pilot request. An empty result means that request is absent, not that the check passed.

## SQL Documentation Rules

Every SQL file begins with an English block comment documenting:

- Title
- Context
- Purpose
- Tables
- Grain
- Scope/parameters
- Expected result
- Interpretation
- Limitations
- Read-only status

All seven audit files are SELECT-only and must not modify Airflow state, application tables, or climate observations.
