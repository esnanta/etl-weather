# ETL Weather — Future Work

Branch: `dev`  
Updated: 2026-10-28

P6 was **closed with explicit operator approval on 2026-10-19**. P6 closure covers documentation and default-identifier regression. It does **not** certify renewed SQL acceptance, production monthly loading, or scheduled Airflow-to-CDS operation.

## Planned workstream — Database refactor and readiness verification

Status: **closed after final code review (2026-10-28)**. Operator-reported tests: 397 unit passed after fixture correction; 17 integration passed after production changes and before fixture-only correction. Scope is intentionally narrow.
Decision: monthly parent/detail tables are required database schema objects for future use; their presence does not imply that monthly aggregation has been accepted for production.

### Baseline and safety checks
- [x] Read `AGENTS.md`, `tests/AGENTS.md`, `docs/app_description.md`, and `docs/python_se_principle.md` for audit. Reconfirm before implementation if the branch changes.
- [x] Inspect current callers, maintenance dependencies, and existing database/orchestration/operations tests on `dev` (static/source audit).
- [x] Record locally executed baseline tests reported by the operator on 2026-10-28 (results below). These runs were not executed in the assistant environment.
- [x] Final source review confirms existing seven-table creation order, table/view identifiers, monthly SQL text, constraints, indexes, PostGIS checks, transactional DDL boundary, and independent climate/geospatial initialization were preserved.
- [x] No database recreation/drop, persisted-data changes, or live schema initialization was performed by the assistant; operator-reported integration tests are the verification evidence (local target not independently inspected).

### Audit notes (2026-10-28, static review only)
- `database/schema.py::initialize_climate_schema()` creates daily parent/detail, monthly parent/detail, progress parent/detail, and submission pacing (seven tables), in that order; spatial tables/views use a separate initializer.
- Pre-refactor, `database/verification.py::CLIMATE_TABLES` contained five objects; after implementation, readiness requires seven.
- `scripts/maintenance/destroy_all.py` computes its expected inventory from `CLIMATE_TABLES | GEOSPATIAL_TABLES`; adding monthly tables changes **verification**, not its explicit confirmation/preflight or recreation commands.
- Pre-refactor, `runtime_database_engine()` duplicated engine creation/disposal; it now delegates to `managed_database_engine()`, with updated tests for success and failure cleanup.
- Pre-refactor, `database/bootstrap.py` duplicated the `climate_data` literal; the `POSTGIS_SCHEMA` alias now references `APPLICATION_SCHEMA`.
- Existing related tests examined: `tests/unit/database/test_setup.py`, `tests/unit/orchestration/test_database.py`, `tests/unit/geospatial/test_spatial_schema.py`, `tests/unit/geospatial/test_region_views.py`, `tests/unit/geospatial/test_monthly_region_view.py`, `tests/unit/operations/test_destroy_all.py`. Focused readiness negative/positive tests have since been added.
- Historical P6 results at the end of this document remain distinct from the 2026-10-28 baseline. No pytest or PostgreSQL integration commands were executed in the assistant environment.
- Repository tests are organized under `tests/unit/orchestration/`; do not assume legacy `tests/airflow_orchestration/` exists.

### Local baseline results (operator-reported, 2026-10-28)
- `python -m pytest tests/unit/database tests/unit/geospatial -q` → **46 passed in 0.88s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/orchestration tests/unit/operations -q` → **215 passed in 5.60s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **392 passed in 6.80s**.
- These are **pre-refactor** outcomes; the 46 and 215 tests are subsets of the 392 total and must not be added together.
- `python -m pytest tests/integration -q` → **17 passed in 2.35s** (operator-reported pre-refactor integration baseline).
- All four commands were run locally by the operator; the assistant did not run them. Re-run relevant tests after implementation.

### Implementation
- [x] **Consolidate monthly DDL:** move `create_monthly_climate_tables()` unchanged from `database/monthly_climate.py` into `database/climate.py`; update imports in `database/schema.py` and tests/callers, then delete the obsolete module only after confirming no references remain. Preserve exact CREATE TABLE statements, execution order, constraints, transaction boundary, function name, and independent monthly workflow.
- [x] **Database readiness:** include `MONTHLY_CLIMATE_TABLE` and `MONTHLY_CLIMATE_DETAIL_TABLE` in `database/verification.py::CLIMATE_TABLES`. Keep the existing inspection result keys and public function signatures. Confirm `inspect_database()` and `require_schema_ready()` now report a missing monthly table and accept all seven climate/operational tables.
- [x] **Maintenance dependency audit (static):** `scripts/maintenance/destroy_all.py` imports `CLIMATE_TABLES`, unions it with `GEOSPATIAL_TABLES` into `EXPECTED_APPLICATION_TABLES`, and uses that in post-recreation readiness verification. The additional two monthly tables will intentionally become required. No destructive action was executed.
- [x] Update non-destructive maintenance table-inventory tests to expect ten application tables; unit regression passed (397 tests).
- [x] **Engine lifecycle:** make `orchestration/repositories/database.py::runtime_database_engine()` delegate to `database.connection.managed_database_engine()` without changing target selection, yielded engine lifetime, or disposal behavior.
- [x] **PostGIS schema constant:** replace the duplicate literal `POSTGIS_SCHEMA = "climate_data"` in `database/bootstrap.py` with the canonical `APPLICATION_SCHEMA` reference while preserving the public constant and resulting SQL/error messages.
- [x] **Requirement documentation:** update the owning database-readiness requirement in `docs/app_description.md` to distinguish required monthly database structures from the not-yet-accepted monthly aggregation workflow. Reconcile any related descriptive references without expanding the implementation scope.

### Implementation evidence
- The monthly DDL function was moved without modifying its SQL statements; `database/monthly_climate.py` was deleted and callers/tests updated.
- `CLIMATE_TABLES` now lists seven required tables. `destroy_all.py` final inventory consequently expects ten tables (seven climate/operational plus three geospatial); its explicit safety gates were not modified.
- Runtime engine lifecycle now delegates to `managed_database_engine` and keeps the context-manager interface.
- PostGIS schema alias uses `APPLICATION_SCHEMA` while retaining the existing exported name.
- No live PostgreSQL, Airflow, CDS, or destructive maintenance command was executed by the assistant. Post-refactor unit suite passed (397 tests) and integration suite passed before the test fixture correction (17 tests).

### Post-refactor regression evidence (operator-reported, 2026-10-28)
- Focused suite: **78 passed, 1 failed** (1.33s). Failure: `test_geospatial_readiness_remains_independent`.
- All unit tests: **396 passed, 1 failed** (7.38s), same failure.
- Integration tests: **17 passed** (2.23s).
- Root cause: the new test fake matched `pg_namespace` before `pg_extension`, although the extension query joins both catalog tables. The test fake was corrected in commit `f9757efa`; production code was not changed for this correction.
- [x] Re-run complete unit suite after the test-only correction: `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **397 passed in 8.01s** (operator-reported, 2026-10-28).
- [x] Complete unit suite passed after the fixture-only correction (397 tests); a separate focused re-run is unnecessary for closure.
- Integration passed before the test-fixture fix (17 passed in 2.23s); no production code changed in that fix.

### Regression and acceptance
- [x] Add/update focused unit tests proving missing monthly parent/detail tables fail readiness, complete seven-table schema passes, and inspection stays read-only. Include the maintenance dependency contract.
- [x] Add/update focused tests for delegated engine creation/disposal (including failures); existing PostGIS unit guards remain unchanged and must be rerun.
- [x] Run the wider unit suite (397 passed after correction). Focused suite was run before fixture correction (78 passed, 1 failed), with the sole failing fixture covered in the passing complete unit run.
- [x] Operator-reported complete integration suite after production changes: 17 passed (2.23s), before a subsequent test-only fixture fix. Its target environment was not independently inspected.
- [x] Final diff review against pre-implementation commit `2205ff7`: 15 affected files, including tests/docs; no unapproved SQL or persistence-logic edits found. Full source-level review complete; no independent CI run performed.

### Final review notes (2026-10-28)
- Compared `2205ff7..dev` (19 commits; 15 changed files). Monthly function body was verified text-identical to its old definition, and module removal is reflected in the current repository tree.
- The only intentional behavior change is database readiness requiring both monthly tables, propagating to the ten-table final inventory in `destroy_all.py`.
- `runtime_database_engine()` preserves context-manager lifetime and disposal; engine creation now uses the canonical SQLAlchemy URL object instead of its rendered string. The resolved PostgreSQL target remains the same; connection-formatting equivalence is supported by the passing tests but was not separately measured against a live runtime.
- The temporary unit-test failure was a fake catalog-query ordering bug; the corrected 397-test unit suite passed. The 17 integration tests were run before that test-only correction.
- No unrelated implementation changes or blocking review findings identified. This review does not substitute for a production deployment/pilot.

### Deferred (out of scope)
- Changing custom schema support, moving PostGIS bootstrap into a new module, reorganizing `schema.py`, altering monthly scientific aggregation, or enabling monthly processing in the daily Airflow DAG.

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
