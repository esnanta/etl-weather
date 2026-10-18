# Outstanding Work — ETL Weather

Branch: `dev`  
Updated: 2026-10-18

This file tracks **only work not yet accepted**. Completed P0–P5 tasks, P3 scheduling-contract checks, database/geospatial implementation history, and successful regression checklists have been removed from the TODO. Historical decisions and evidence remain in Git commits and maintained project documentation.

## P6 — Final acceptance

- [ ] Complete the six-query PostgreSQL catalog execution/acceptance in [POSTGRESQL_QUERY_AUDIT_PLAN.md](POSTGRESQL_QUERY_AUDIT_PLAN.md). Do not equate query implementation with successful execution.
- [ ] Reconcile root `README.md` monthly-processing/persistence claims with the actual current code. The active `pipeline/runtime.py` runs daily ETL in calendar-month request partitions, while the monthly parent/detail tables and monthly regional view were accepted as schema-only. Confirm any separately implemented monthly modules before describing monthly output as operational; do not change scientific methodology in this documentation task.
- [ ] Review remaining operations tests for non-destructive execution and run applicable regression coverage. Full `tests/unit` (**377 passed**) and `tests/integration` (**17 passed**) are already accepted and should not be repeated without a reason.
- [ ] Perform final cross-document consistency review (root/test/airflow/geospatial READMEs and AGENTS), including configurable PostgreSQL table/view prefixes; correct residual stale statements without changing code execution.
- [ ] Record final limitations and obtain explicit sign-off before closing P6.

## Separate future work (not blockers for P6 contract-only validation)

- [ ] Plan and authorize a controlled scheduled Airflow pilot before claiming end-to-end scheduler → CDS operational acceptance. P3 verified scheduling contract, DAG discovery, paused state and one-slot `cds_api` pool; no DagRun was present on current SQLite metadata.
- [ ] Define, implement, and separately validate scientific monthly aggregation and idempotent monthly loading **if desired**. Empty monthly database tables/view are not evidence of a working monthly ETL.
- [ ] Repeat regional query benchmarks when representative historical climate volume has accumulated; existing measurements cover the limited 1,590-observation pilot.
- [ ] Consider optional destructive-reset/cleanup fixture tests only in an isolated disposable environment, with explicit approval; never run on `weather` as part of documentation cleanup.

## Boundaries

Only branch `dev` may be edited. Keep `pipeline/` and `orchestration/` behavior unchanged without explicit approval. No CDS retrieval, DAG unpause, destructive database actions, or automatic prefix migration are authorized by this TODO.
