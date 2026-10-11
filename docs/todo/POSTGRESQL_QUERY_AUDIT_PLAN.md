# PostgreSQL Read-Only Query Audit — Plan and Checklist

Date: 2026-10-11 (Asia/Jakarta)  
Branch: `dev` only  
Status: **Q1 SQL implementation completed; PostgreSQL execution/integration validation pending.** Q0 live PostgreSQL version/permissions remain unverified.

## 1. Purpose and limits

Build a maintainable collection of **read-only PostgreSQL SQL scripts** to
inspect what the active ERA5-Land ETL persists and validate storage integrity.
Each SQL file performs **one named task** and has an English documentation
comment (SQL has no Python-style docstring syntax), explaining purpose,
tables, assumptions, input scope, expected result, and interpretation.

Current work is SQL diagnostics, not a database migration, ETL refactor, EDA,
new monthly persistence layer, or live CDS run. Do not change application code,
data, indexes, or schema. All reads target branch `dev` only.

## 2. Verified schema and data mapping (from current `dev` DDL)

There is **one application schema, `climate_data`**, with five tables.

| Table | Data inserted by | Contents and row grain | Key / link |
| --- | --- | --- | --- |
| `daily_climate` | `pipeline/loaders/daily_climate_postgres.py` | One metadata row per logical request partition: `request_key`, source/dataset, inclusive date range, variable signature, bbox, download/transform timestamps, transformed Parquet path/SHA256. | PK `id`; unique `request_key`. |
| `daily_climate_detail` | same loader | One observation per **partition, UTC instant, grid point**, including `valid_time`, `valid_time_local`, latitude/longitude, precipitation, temperature, dewpoint, soil water and solar radiation daily values. All five climate columns can be NULL. | PK `id`; FK `daily_climate_id -> daily_climate.id` (cascade); unique (`daily_climate_id`, `valid_time`, `latitude`, `longitude`). |
| `pipeline_progress` | `orchestration/repositories/pipeline_progress.py` | One logical batch/campaign-quarter progress row with period, configuration identity, expected/completed/failed partition counts, execution/validation state, owner/claim, timestamps and last error. | PK `id`; unique (pipeline name, source, dataset, dates, configuration identity). |
| `pipeline_progress_detail` | same progress repository; updated by orchestration | One monthly/request partition within a parent batch, with request key, period, RAW/Parquet paths and checksums, spatial/temporal dimensions, row count, extract/transform/load/validation statuses, errors and JSONB metrics. | PK `id`; FK `pipeline_progress_id -> pipeline_progress.id` (cascade); unique (`pipeline_progress_id`, `request_key`). |
| `submission_pacing` | `orchestration/repositories/submission_pacing.py` | Global CDS submission-clock state, not one log row per POST; contains last reserved submission instant and update time. | Singleton PK `singleton_id = 1`. |

Relevant DDL sources:
`pipeline/database/schema.py`,
`orchestration/repositories/pipeline_progress.py`,
`orchestration/repositories/submission_pacing.py`.
Reconciliation logic also joins `pipeline_progress_detail.request_key` with
`daily_climate.request_key` and compares source, dataset, date bounds,
Parquet path/SHA256, and **actual detail row counts**.

### Exact scientific observation columns

- `tp_daily_total_mm` — daily precipitation total (mm);
- `t2m_daily_mean_c` — daily mean temperature (°C);
- `d2m_daily_mean_c` — daily mean dewpoint (°C);
- `swvl1_daily_mean_m3_m3` — daily mean volumetric soil water (m³/m³);
- `ssrd_daily_total_j_m2` — daily solar radiation total (J/m²).

The application uses `Asia/Jakarta` (UTC+7) local-day semantics.
Both `valid_time` and `valid_time_local` are PostgreSQL `TIMESTAMP`
columns, **without** timezone; audits must verify the documented UTC/local
relationship rather than assume implicit PostgreSQL timezone conversion.

Current bbox: north 6.1, south 1.9, west 94.9, east 98.3; regular
0.1° grid with 43 latitude coordinates × 35 longitude coordinates =
**1,505 possible coordinate pairs**. This is a geometric maximum, **not**
an expected populated detail count per day; missing/sea-masked points
must not be treated as corruption or zero.

## 3. Naming taxonomy and SQL file format

Implement under the existing root `query/` directory. One task = one file.

```text
query/
├── daily_climate_verification.sql       # existing legacy script: review/replace
├── 01_schema__01_inventory.sql
├── 01_schema__02_constraints.sql
├── 02_daily_climate__01_preview.sql
├── 02_daily_climate__02_count_by_period.sql
├── 02_daily_climate__03_duplicate_request_key.sql
├── 02_daily_climate__04_request_metadata.sql
├── 03_daily_climate_detail__01_preview.sql
├── 03_daily_climate_detail__02_count_by_partition.sql
├── 03_daily_climate_detail__03_duplicate_observation_key.sql
├── 03_daily_climate_detail__04_orphan_or_missing_parent.sql
├── 03_daily_climate_detail__05_temporal_utc7_alignment.sql
├── 03_daily_climate_detail__06_day_grid_coverage.sql
├── 03_daily_climate_detail__07_bbox_and_grid_spacing.sql
├── 03_daily_climate_detail__08_null_value_counts.sql
├── 03_daily_climate_detail__09_value_sanity.sql
├── 04_pipeline_progress__01_preview.sql
├── 04_pipeline_progress__02_quarter_counts_and_status.sql
├── 04_pipeline_progress__03_partition_count_consistency.sql
├── 05_pipeline_progress_detail__01_preview.sql
├── 05_pipeline_progress_detail__02_status_and_failures.sql
├── 05_pipeline_progress_detail__03_partition_vs_daily_evidence.sql
├── 05_pipeline_progress_detail__04_artifact_metadata_consistency.sql
├── 06_submission_pacing__01_state.sql
└── 07_cross_table__01_campaign_reconciliation_summary.sql
```

Format: `<category-order>_<table-or-domain>__<task-order>_<task>.sql`.
Prefix orders categories by data lineage: schema → partition metadata →
observations → batch progress → partition progress → submission gate →
cross-table checks. The number after `__` orders tasks within the table.

At the top of **every SQL file**, use a structured English SQL block comment
as its docstring-equivalent:

```sql
/*
Purpose: ...
Tables: climate_data.<table>
Grain: ...
Scope/parameters: ...
Expected output: ...
Pass/fail interpretation: ...
Assumptions and caveats: ...
Read-only: SELECT only.
*/
```

Use descriptive result column aliases and deterministic `ORDER BY` when
listing rows. Avoid `SELECT *` for durable outputs; use bounded `LIMIT`
for previews. Do not assume an `id` starts at 1 or that every run has
exactly one partition. Query scopes should be explicit and safely editable
(e.g., a `params` CTE for selected dates/request keys), with no passwords
embedded in scripts. Avoid server-side artifact-file access.

## 4. Ordered implementation checklist

### Q0 — Confirm coverage and conventions

- [x] Recheck current `dev` DDL and repository reads/writes before coding (2026-10-11; confirmed five application tables, two foreign-key relationships, unique identities, per-request load evidence, and singleton pacing).
- [x] Inspect the existing `query/daily_climate_verification.sql`: it assumes
      `daily_climate_id = 1` and a fixed **747** observations for an old
      sanity run. This legacy script is unsuitable for a general campaign; its
      replacement/removal remains part of Q1–Q2 implementation.
- [ ] Confirm **live target PostgreSQL version** (requires operator database
      access; not inferred from Python code). A read-only transaction and
      SELECT-only role are documented in `query/README.md`; verify them on the
      target database before actual query execution.
- [x] Define the one-task-per-file naming and English SQL documentation
      contract (see `query/README.md`); verify each implementation against it.
- [x] Define the consistent meaning of `PASS`, `FAIL`, `INFO` and
      `NOT_EVALUATED` for diagnostic results in `query/README.md`.

**Q0 remaining verification:** Obtain `SHOW server_version;` and verify the
actual PostgreSQL connection target/read-only permissions when the SQL work is
run by the operator. Q0 did not execute SQL and cannot certify the live host.

### Q1 — Schema and daily-climate metadata

- [x] Build schema/table inventory and PK/FK/unique/check index inventory (`01_schema__01_inventory.sql`, `01_schema__02_constraints.sql`).
- [x] Preview partition metadata and count requests grouped by start-year/quarter/source/dataset (`02_daily_climate__01_preview.sql`, `02_daily_climate__02_count_by_period.sql`).
- [x] Check duplicate `request_key` and inspect bbox/variable/provenance
      metadata without imposing one global request shape on all records (`02_daily_climate__03_duplicate_request_key.sql`, `02_daily_climate__04_request_metadata.sql`).

**Q1 verification still pending:** Run all six SQL scripts on the intended
PostgreSQL database and capture observed results. Q1 completion above denotes
file implementation and source review, not proven database execution; Q4 owns
isolated test database verification. No live database has been queried by the
assistant.

### Q2 — Daily observation integrity

- [ ] Preview a bounded range of daily observations.
- [ ] Count by `daily_climate_id` and date; distinguish **expected** from
      **observed** grid coverage.
- [ ] Check duplicate observation composite keys.
- [ ] Check orphaned children and metadata partitions with zero details.
- [ ] Check UTC versus UTC+7 timestamps, period boundaries and local-day counts.
- [ ] Check latitude/longitude within **each partition's bbox** and regular
      grid coordinates, without demanding that all 1,505 cells exist.
- [ ] Count NULLs for each variable; do not convert NULL to zero.
- [ ] Report suspicious/nonfinite/impossible ranges as diagnostic candidates.
      Scientific plausibility thresholds need explicit approval and must not
      silently reject valid ERA5-Land outputs.

### Q3 — Progress and pacing evidence

- [ ] Preview progress parent and monthly child rows.
- [ ] Summarize execution/validation statuses and failed partitions.
- [ ] Compare parent expected/completed/failed counts with child evidence.
- [ ] Match child request keys, periods, Parquet paths/SHA256 and recorded row
      counts with canonical daily-climate metadata and observations.
- [ ] Inspect the singleton submission clock; explain that it **cannot**
      reconstruct historical POST spacing without an event log.
- [ ] Provide one campaign-wide read-only reconciliation summary without
      updating progress state.

### Q4 — Verification and documentation

- [ ] Verify every file runs against an isolated test database seeded with
      known-good small fixtures.
- [ ] Include intentionally corrupted/edge fixtures for duplicate/missing,
      timestamp mismatch, wrong checksum and partial progress as applicable;
      validate expected diagnostic results.
- [ ] Run `EXPLAIN` on expensive aggregation queries where relevant, with
      no `EXPLAIN ANALYZE` against production without review.
- [ ] Confirm every script is read-only; no `INSERT`, `UPDATE`, `DELETE`,
      `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, or data-mutating functions.
- [ ] Document recommended execution order and `psql` usage in
      `query/README.md`, including target database verification and how to
      interpret zero rows.
- [ ] Run relevant existing offline pytest and isolated PostgreSQL integration
      suites; record actual results, not assumed pass counts.
- [ ] Mark completed work here, and remove `docs/todo/` content once all
      planned queries and verification are complete.

## 5. Acceptance criteria

The scripts independently help an operator answer: **what tables exist, what
was loaded, how many rows per partition/day, what is missing/duplicated, do
timestamps/grid/values look consistent, do progress records match persisted
observations, and what does the submission clock say?** No query changes data.

Only Q0–Q4 are authorized by this plan. No implementation of monthly tables,
EDA, or historical campaign execution is included.
