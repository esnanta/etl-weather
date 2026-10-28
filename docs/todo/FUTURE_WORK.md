# ETL Weather — Future Work

Branch: `dev`  
Updated: 2026-10-28

P6 was **closed with explicit operator approval on 2026-10-19**. P6 closure covers documentation and default-identifier regression. It does **not** certify renewed SQL acceptance, production monthly loading, or scheduled Airflow-to-CDS operation.

## Planned workstream — Database refactor and readiness verification

Status: **dependency/source audit and operator-reported local unit/integration baselines complete; implementation not started**. Scope is intentionally narrow.
Decision: monthly parent/detail tables are required database schema objects for future use; their presence does not imply that monthly aggregation has been accepted for production.

### Baseline and safety checks
- [x] Read `AGENTS.md`, `tests/AGENTS.md`, `docs/app_description.md`, and `docs/python_se_principle.md` for audit. Reconfirm before implementation if the branch changes.
- [x] Inspect current callers, maintenance dependencies, and existing database/orchestration/operations tests on `dev` (static/source audit).
- [x] Record locally executed baseline tests reported by the operator on 2026-10-28 (results below). These runs were not executed in the assistant environment.
- [ ] Preserve the existing seven-table creation order, table/view identifiers, SQL definitions, constraints, indexes, PostGIS checks, transactional writes, idempotency, and independent climate/geospatial initialization.
- [ ] Do not recreate/drop databases, alter persisted records, or run schema initialization on a live database solely for this refactor. All PostgreSQL integration checks must use isolated `TEST_POSTGRES_*` targets.

### Audit notes (2026-10-28, static review only)
- `database/schema.py::initialize_climate_schema()` creates daily parent/detail, monthly parent/detail, progress parent/detail, and submission pacing (seven tables), in that order; spatial tables/views use a separate initializer.
- `database/verification.py::CLIMATE_TABLES` currently contains only five objects; this is the exact readiness change approved for implementation.
- `scripts/maintenance/destroy_all.py` computes its expected inventory from `CLIMATE_TABLES | GEOSPATIAL_TABLES`; adding monthly tables changes **verification**, not its explicit confirmation/preflight or recreation commands.
- `orchestration/repositories/database.py::runtime_database_engine()` duplicates engine creation/disposal already owned by `database/connection.py::managed_database_engine()`. The existing `tests/unit/orchestration/test_database.py` patches `create_engine` at the orchestration module boundary; this test must be updated to patch the delegated context manager and exercise failure cleanup.
- `database/bootstrap.py` duplicates the `climate_data` literal in `POSTGIS_SCHEMA`; keep that exported name stable when referencing `APPLICATION_SCHEMA`. Retain exact SQL and extension-location guards.
- Existing related tests examined: `tests/unit/database/test_setup.py`, `tests/unit/orchestration/test_database.py`, `tests/unit/geospatial/test_spatial_schema.py`, `tests/unit/geospatial/test_region_views.py`, `tests/unit/geospatial/test_monthly_region_view.py`, `tests/unit/operations/test_destroy_all.py`. Focused readiness negative/positive tests remain to be added.
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
- [ ] **Database readiness:** include `MONTHLY_CLIMATE_TABLE` and `MONTHLY_CLIMATE_DETAIL_TABLE` in `database/verification.py::CLIMATE_TABLES`. Keep the existing inspection result keys and public function signatures. Confirm `inspect_database()` and `require_schema_ready()` now report a missing monthly table and accept all seven climate/operational tables.
- [x] **Maintenance dependency audit (static):** `scripts/maintenance/destroy_all.py` imports `CLIMATE_TABLES`, unions it with `GEOSPATIAL_TABLES` into `EXPECTED_APPLICATION_TABLES`, and uses that in post-recreation readiness verification. The additional two monthly tables will intentionally become required. No destructive action was executed.
- [ ] Validate this changed readiness expectation with focused non-destructive maintenance tests.
- [ ] **Engine lifecycle:** make `orchestration/repositories/database.py::runtime_database_engine()` delegate to `database.connection.managed_database_engine()` without changing target selection, yielded engine lifetime, or disposal behavior.
- [ ] **PostGIS schema constant:** replace the duplicate literal `POSTGIS_SCHEMA = "climate_data"` in `database/bootstrap.py` with the canonical `APPLICATION_SCHEMA` reference while preserving the public constant and resulting SQL/error messages.
- [ ] **Requirement documentation:** update the owning database-readiness requirement in `docs/app_description.md` to distinguish required monthly database structures from the not-yet-accepted monthly aggregation workflow. Reconcile any related descriptive references without expanding the implementation scope.

### Regression and acceptance
- [ ] Add/update focused unit tests proving missing monthly parent/detail tables fail readiness, complete seven-table schema passes, and inspection stays read-only. Include the maintenance dependency contract.
- [ ] Add/update focused tests for delegated engine creation/disposal (including failures) and unchanged PostGIS schema/version guards.
- [ ] Run `python -m pytest tests/unit/database tests/unit/geospatial -q`, then the wider unit/orchestration suite applicable to the current repository layout (quiet mode).
- [ ] Run relevant PostgreSQL integration tests against isolated `TEST_POSTGRES_*` credentials; verify normal and repeat-execution/failure paths where relevant. Do not assume historical test counts are current.
- [ ] Inspect the final diff for unrelated behavior changes; report files changed, commands/results, unrun checks, and remaining risks before considering the refactor complete.

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
