# Six-Query PostgreSQL Audit — Verification Checklist

Date: 2026-10-11 (Asia/Jakarta)  
Branch: `dev` only  
Status: **Six-query implementation completed; execution and acceptance pending.**

## Scope

Maintain only six focused read-only queries for operational inspection of
ERA5-Land data persisted to PostgreSQL. No EDA, monthly persistence, schema
modification, historical campaign execution or ETL code changes are included.

The current catalog is documented in `query/README.md`:

- [x] `00_connection__01_database_context.sql`: database name, user, version,
      transaction safety context.
- [x] `01_schema__01_inventory.sql`: five application tables.
- [x] `02_daily_climate__01_partition_summary.sql`: request partitions,
      observed dates, grid points and observation counts.
- [x] `03_daily_climate_detail__01_data_quality.sql`: missing dates, duplicates,
      UTC+7 timestamp and period-boundary integrity.
- [x] `04_pipeline_progress__01_campaign_status.sql`: batch/quarter completion.
- [x] `05_pipeline_progress_detail__01_load_verification.sql`: compare completed
      load evidence with persisted partition metadata and observation counts.
- [x] Remove obsolete/superseded detailed query files and the legacy
      `daily_climate_verification.sql`.
- [x] Keep English file-level documentation of context, purpose, expected
      result, interpretation and limitations.

## Verified operator evidence from earlier queries

- The `weather` database runs PostgreSQL **16.15**, as shown by the operator.
- The five `climate_data` application tables are present.
- Nine January–September 2016 request partitions appeared, three in each
  of Q1, Q2 and Q3.
- The earlier duplicate-request-key query returned zero rows.
- The earlier UTC+7 offset anomaly query returned zero rows.
- The earlier numeric sanity query found three very small negative rainfall
  values retained by the ETL; this is not an automatic failure.

Those observations do **not** certify the newly consolidated queries.

## Remaining acceptance checks

- [ ] Execute the six-file catalog on the intended `weather` database and
      record the output of each query.
- [ ] Confirm the expected schema inventory and connection context.
- [ ] Review summary counts against the actual campaign stage; do not assume
      fixed grid-point or observation counts.
- [ ] Confirm all complete partitions return PASS in the combined quality and
      load-verification queries. Examine FAIL or NOT_EVALUATED without changing
      data automatically.
- [ ] Run the new SQL against an isolated PostgreSQL test fixture where
      possible; test missing-day, duplicate, bad-time and metadata-mismatch
      detection.
- [ ] Review query performance as the 2016–2025 database grows.
- [ ] Once these checks pass, remove this finished TODO file rather than keeping
      a history of completed work.

The application code and PostgreSQL data have not been changed by this query
catalog refactor.
