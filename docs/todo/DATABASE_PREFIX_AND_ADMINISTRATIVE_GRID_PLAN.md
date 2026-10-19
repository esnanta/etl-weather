# ETL Weather — Outstanding Work

Branch: `dev`  
Updated: 2026-10-19

Only unfinished work appears here. Completed work and its test evidence remain in Git history and the relevant README files.

## P6 — Final regression, SQL acceptance, and documentation

### Seven read-only PostgreSQL queries

The seven implemented query files (00–06) are described in [`query/README.md`](../../query/README.md). Implementation is complete. Historical SQL execution was reported by the operator, but complete current-run outputs have not been attached to this acceptance record.

- [ ] Reconcile historical operator-reported SQL PASS results with the **seven** current query files and retain available outputs; if current database identity/schema inventory needs renewed acceptance, run read-only queries 00–06 against the intended `weather` connection. Inventory expectation: seven climate/operational tables, three geospatial tables and two views.
- [ ] Compare partition and campaign summaries to *actual* persisted data; verify daily completeness, duplicates, UTC+7 date boundaries and completed-load evidence. Investigate `FAIL`/`NOT_EVALUATED` without altering data automatically.
- [ ] Where practical, test that the read-only checks detect missing days, duplicates, invalid timestamps and mismatched metadata in isolated fixtures.

### Final documentation and regression checks

- [x] Reconcile root `README.md` with daily-only runtime; daily CLI renamed from `scripts/run_pipeline_cds.py` to `scripts/run_pipeline.py`. A separate experimental `scripts/run_monthly_climate.py` is now present; it is not invoked by Airflow or daily ETL.
- [x] Review operations tests for destructive side effects. Inspected representative destroy/reset/database-recreation unit tests: destructive boundaries are mocked and guarded by preflight/confirmation; no destructive commands executed. Operator-reported full unit regression **380 passed** on 2026-10-19; the previous all-integration baseline **17 passed** was not rerun as part of this review.
- [x] Review root/test/Airflow/geospatial READMEs and AGENTS for daily/monthly boundary claims and update them to reflect the separate CLI.
- [x] Refactor daily/monthly view builders, spatial DDL, district importer and grid mapping to use canonical schema/table/view identifiers; preserve default SQL snapshots for existing contract tests. Add offline SQL-builder regression tests. Static `geospatial/sql/*.sql` files remain operator examples for current default names, not dynamically generated scripts.
- [x] Local offline unit regression reported by operator (2026-10-19): `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` **380 passed in 8.69s**; focused `tests/unit/database tests/unit/geospatial` **46 passed in 1.02s**. These runs validate current default identifiers, not database prefix migration.
- [x] Operator-reported isolated geospatial PostgreSQL regression (2026-10-19): `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration/geospatial -q` **6 passed in 1.43s**. Together with 380 unit tests (46 focused database/geospatial), P6.1 default-identifier regression is accepted; prefix migration on an existing database remains out of scope.
- [ ] Future separate monthly CLI acceptance: test full calendar-day/grid coverage, missing values, duplicates, rerun refusal, rollback, PostgreSQL compatibility and scientific definitions in isolated fixtures. This work is **not a P6 acceptance blocker** and must not run against live `weather` yet.
- [ ] Record residual limitations and obtain explicit approval before closing P6. Once accepted, remove this TODO file rather than retaining completed checklists.

## P6 acceptance summary (2026-10-19)

- P6.1 identifiers: operator-reported unit **380 passed**, focused database/geospatial **46 passed**, isolated geospatial integration **6 passed**.
- Documentation: daily/monthly separation aligned; seven-query inventory wording corrected in `query/README.md`.
- SQL acceptance: historical checks reported, but current outputs and negative-fixture evidence remain unverified in this record. Do not mark these PASS solely on recollection.
- Residual limitations: monthly CLI is experimental; scheduled Airflow → CDS pilot is not accepted; static SQL targets current physical names; prefix migration is out of scope.
- **Final P6 acceptance is pending explicit operator approval and closure decision; do not delete this TODO yet.**

## Separately authorized future work — not P6 acceptance blockers

- [ ] Conduct a controlled scheduled Airflow → CDS pilot before claiming end-to-end operational scheduler acceptance. P3's contract/discovery and single-slot `cds_api` pool are verified, but the current SQLite metadata had no DagRun.
- [ ] Review scientific monthly aggregation semantics and complete isolated testing/acceptance of the separately implemented experimental monthly CLI. Do not treat the new, untested write path as production-ready.
- [ ] Rebenchmark regional joins and six-query SQL performance after representative historical data have accumulated (current regional baseline: 1,590 pilot observations).
- [ ] Optionally verify destructive reset/cleanup using only disposable isolated fixtures and explicit approval, never the live `weather` database.

## Safety boundary

Edit only branch `dev`. No changes to `pipeline/` or `orchestration/` behavior without separate approval. Do not retrieve CDS data, unpause/trigger Airflow, reset application data or change schema/prefixes as part of TODO maintenance.
