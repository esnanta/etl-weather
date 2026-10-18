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
- [ ] Resolve **prefix audit finding**: `database/vw_monthly_climate_region.py` uses a literal `climate_data.vw_monthly_climate_region` definition and literal `climate_data.t_*` joins despite importing configurable identifiers. Inspect other SQL/view definitions before declaring prefix semantics verified; changing prefixes needs coordinated migration.
- [ ] Test standalone monthly aggregation in isolated fixtures (full calendar-day/grid coverage, missing values, duplicates, rerun refusal, atomic rollback, PostgreSQL schema compatibility). No acceptance or execution against live `weather` yet.
- [ ] Record residual limitations and obtain explicit approval before closing P6. Once accepted, remove this TODO file rather than retaining completed checklists.

## Separately authorized future work — not P6 acceptance blockers

- [ ] Conduct a controlled scheduled Airflow → CDS pilot before claiming end-to-end operational scheduler acceptance. P3's contract/discovery and single-slot `cds_api` pool are verified, but the current SQLite metadata had no DagRun.
- [ ] Review scientific monthly aggregation semantics and complete isolated testing/acceptance of the separately implemented experimental monthly CLI. Do not treat the new, untested write path as production-ready.
- [ ] Rebenchmark regional joins and six-query SQL performance after representative historical data have accumulated (current regional baseline: 1,590 pilot observations).
- [ ] Optionally verify destructive reset/cleanup using only disposable isolated fixtures and explicit approval, never the live `weather` database.

## Safety boundary

Edit only branch `dev`. No changes to `pipeline/` or `orchestration/` behavior without separate approval. Do not retrieve CDS data, unpause/trigger Airflow, reset application data or change schema/prefixes as part of TODO maintenance.
