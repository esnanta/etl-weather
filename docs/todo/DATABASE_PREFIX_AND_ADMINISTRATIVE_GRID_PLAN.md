# ETL Weather — Outstanding Work

Branch: `dev`  
Updated: 2026-10-18

Only unfinished work appears here. Completed work and its test evidence remain in Git history and the relevant README files.

## P6 — Final regression, SQL acceptance, and documentation

### Six read-only PostgreSQL queries

The six implemented query files are described in [`query/README.md`](../../query/README.md). Implementation is complete, but execution and acceptance have **not** been reported.

- [ ] Run all six SQL files against the intended `weather` database; retain outputs and verify database/connection identity, physical identifiers and schema inventory (currently seven climate/operational tables, three geospatial tables and two views).
- [ ] Compare partition and campaign summaries to *actual* persisted data; verify daily completeness, duplicates, UTC+7 date boundaries and completed-load evidence. Investigate `FAIL`/`NOT_EVALUATED` without altering data automatically.
- [ ] Where practical, test that the read-only checks detect missing days, duplicates, invalid timestamps and mismatched metadata in isolated fixtures.

### Final documentation and regression checks

- [x] Reconcile root `README.md` with daily-only runtime; daily CLI renamed from `scripts/run_pipeline_cds.py` to `scripts/run_pipeline.py`. A separate experimental `scripts/run_monthly_climate.py` is now present; it is not invoked by Airflow or daily ETL.
- [ ] Review remaining operations tests for destructive side effects and run applicable non-destructive regression checks. Previously accepted: `tests/unit` **377 passed** and `tests/integration` **17 passed**; repeat only if changes warrant it.
- [x] Review root/test/Airflow/geospatial READMEs and AGENTS for daily/monthly boundary claims and update them to reflect the separate CLI.
- [x] Refactor daily/monthly view builders, spatial DDL, district importer and grid mapping to use canonical schema/table/view identifiers; preserve default SQL snapshots for existing contract tests. Add offline SQL-builder regression tests. Static `geospatial/sql/*.sql` files remain operator examples for current default names, not dynamically generated scripts.
- [x] Local offline unit regression reported by operator (2026-10-19): `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` **380 passed in 8.69s**; focused `tests/unit/database tests/unit/geospatial` **46 passed in 1.02s**. These runs validate current default identifiers, not database prefix migration.
- [ ] Verify changed SQL against isolated `TEST_POSTGRES_*` PostgreSQL with applicable integration tests; no acceptance of changed prefixes or migration on existing `weather` is implied.
- [ ] Test standalone monthly aggregation in isolated fixtures (full calendar-day/grid coverage, missing values, duplicates, rerun refusal, atomic rollback, PostgreSQL schema compatibility). No acceptance or execution against live `weather` yet.
- [ ] Record residual limitations and obtain explicit approval before closing P6. Once accepted, remove this TODO file rather than retaining completed checklists.

## Separately authorized future work — not P6 acceptance blockers

- [ ] Conduct a controlled scheduled Airflow → CDS pilot before claiming end-to-end operational scheduler acceptance. P3's contract/discovery and single-slot `cds_api` pool are verified, but the current SQLite metadata had no DagRun.
- [ ] Review scientific monthly aggregation semantics and complete isolated testing/acceptance of the separately implemented experimental monthly CLI. Do not treat the new, untested write path as production-ready.
- [ ] Rebenchmark regional joins and six-query SQL performance after representative historical data have accumulated (current regional baseline: 1,590 pilot observations).
- [ ] Optionally verify destructive reset/cleanup using only disposable isolated fixtures and explicit approval, never the live `weather` database.

## Safety boundary

Edit only branch `dev`. No changes to `pipeline/` or `orchestration/` behavior without separate approval. Do not retrieve CDS data, unpause/trigger Airflow, reset application data or change schema/prefixes as part of TODO maintenance.
