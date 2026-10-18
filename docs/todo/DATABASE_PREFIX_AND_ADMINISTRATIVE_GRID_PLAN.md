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

- [x] Reconcile root `README.md` descriptions of monthly aggregation and persistence with current implementation. Verified `pipeline/runtime.py` executes daily ETL in calendar-month request partitions; `pipeline/monthly.py`, `scripts/run_monthly_climate.py` and `scripts/run_pipeline.py` do not exist on `dev`. Updated README in commit `33ddbc8`; no scientific or runtime changes.
- [ ] Review remaining operations tests for destructive side effects and run applicable non-destructive regression checks. Previously accepted: `tests/unit` **377 passed** and `tests/integration` **17 passed**; repeat only if changes warrant it.
- [ ] Review root/test/Airflow/geospatial READMEs and AGENTS for stale implementation claims, duplicated guidance and configurable table/view prefix semantics. Physical PostgreSQL names derive from `database/config.py` (`APPLICATION_SCHEMA`, `TABLE_PREFIX`, `VIEW_PREFIX`); SQL containing literal names requires reassessment when prefixes change.
- [ ] Record residual limitations and obtain explicit approval before closing P6. Once accepted, remove this TODO file rather than retaining completed checklists.

## Separately authorized future work — not P6 acceptance blockers

- [ ] Conduct a controlled scheduled Airflow → CDS pilot before claiming end-to-end operational scheduler acceptance. P3's contract/discovery and single-slot `cds_api` pool are verified, but the current SQLite metadata had no DagRun.
- [ ] Define and test scientific monthly aggregation and idempotent monthly loading if the project elects to implement them; existing empty monthly tables/view are insufficient.
- [ ] Rebenchmark regional joins and six-query SQL performance after representative historical data have accumulated (current regional baseline: 1,590 pilot observations).
- [ ] Optionally verify destructive reset/cleanup using only disposable isolated fixtures and explicit approval, never the live `weather` database.

## Safety boundary

Edit only branch `dev`. No changes to `pipeline/` or `orchestration/` behavior without separate approval. Do not retrieve CDS data, unpause/trigger Airflow, reset application data or change schema/prefixes as part of TODO maintenance.
