# ETL Weather — Future Work

Branch: `dev`  
Updated: 2026-10-19

P6 was **closed with explicit operator approval on 2026-10-19**. P6 closure covers documentation and default-identifier regression. It does **not** certify renewed SQL acceptance, production monthly loading, or scheduled Airflow-to-CDS operation.

## Independent workstreams

### Monthly aggregation and persistence
- [ ] Review and approve the scientific rules for local-calendar monthly aggregates from UTC+7 daily observations.
- [ ] Test the experimental `scripts/run_monthly_climate.py` with isolated fixtures: complete month/grid coverage, missing values, duplicate keys, idempotency/rerun behavior, atomic rollback, and PostgreSQL table/schema compatibility.
- [ ] Accept the separate monthly read/aggregate/write path before any `--execute` against the live `weather` database.
- [ ] Keep monthly aggregation separate from `scripts/run_pipeline.py` and the current daily ETL DAG. `pipeline/monthly.py` is not required.

### Scheduled Airflow → CDS pilot
- [ ] Conduct an explicitly authorized controlled scheduled pilot, with checks for pacing, concurrency, retries, checkpoints, and persisted campaign evidence.
- [ ] Do not claim end-to-end scheduler acceptance based only on DAG discovery and a single-slot `cds_api` pool.

### Deferred SQL acceptance
- [ ] If renewed SQL acceptance is needed, retain current outputs for all seven read-only query files `00`–`06` from `query/`, verify connection/database identity, physical table/view inventory, partition integrity, UTC+7 boundaries, and completed-load evidence.
- [ ] Where practical, check missing days, duplicates, invalid timestamps, and mismatched metadata using isolated negative fixtures.
- [ ] Historical operator-reported SQL checks were acknowledged for P6 closure, **without** asserting that all seven current queries were re-executed or reaccepted.

### Deferred performance and maintenance verification
- [ ] Rebenchmark regional joins and query performance once representative historical observations exist (earlier regional pilot: 1,590 observations).
- [ ] Optionally test destructive cleanup only in explicitly approved disposable fixtures; never reset the live `weather` database merely for verification.
- [ ] Treat non-default schema/table/view prefixes as requiring coordinated migration and revalidation of static operator SQL.

## P6 closure evidence

- 2026-10-19 operator report: `tests/unit` **380 passed** (8.69s).
- 2026-10-19 operator report: focused `tests/unit/database tests/unit/geospatial` **46 passed** (1.02s), included within the 380.
- 2026-10-19 operator report: `tests/integration/geospatial` **6 passed** (1.43s).
- P6 documentation and identifier updates are recorded in the `dev` Git history.
- Full integration suite was not rerun for P6 closure; previously reported 17 passed is historical evidence only.
