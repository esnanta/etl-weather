# ETL Weather — Future Work

Branch: `dev`  
Updated: 2026-10-28

P6 was **closed with explicit operator approval on 2026-10-19**. P6 closure covers documentation and default-identifier regression. It does **not** certify renewed SQL acceptance, production monthly loading, or scheduled Airflow-to-CDS operation.

## Planned workstream — Database refactor and readiness verification

Status: **planned; implementation not started**. Scope is intentionally narrow.
Decision: monthly parent/detail tables are required database schema objects for future use; their presence does not imply that monthly aggregation has been accepted for production.

### Baseline and safety checks
- [ ] Re-read `AGENTS.md`, `tests/AGENTS.md`, `docs/app_description.md`, and `docs/python_se_principle.md` before implementation.
- [ ] Inspect current callers, maintenance dependencies, and database tests; record the baseline on `dev`. Run focused unit tests first and report the actual results.
- [ ] Preserve the existing seven-table creation order, table/view identifiers, SQL definitions, constraints, indexes, PostGIS checks, transactional writes, idempotency, and independent climate/geospatial initialization.
- [ ] Do not recreate/drop databases, alter persisted records, or run schema initialization on a live database solely for this refactor. All PostgreSQL integration checks must use isolated `TEST_POSTGRES_*` targets.

### Implementation
- [ ] **Database readiness:** include `MONTHLY_CLIMATE_TABLE` and `MONTHLY_CLIMATE_DETAIL_TABLE` in `database/verification.py::CLIMATE_TABLES`. Keep the existing inspection result keys and public function signatures. Confirm `inspect_database()` and `require_schema_ready()` now report a missing monthly table and accept all seven climate/operational tables.
- [ ] **Maintenance dependency audit:** inspect consumers of `CLIMATE_TABLES` (including `scripts/maintenance/destroy_all.py`) before updating its contents; confirm no unexpected maintenance behavior or destructive operations occur.
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

### Database refactor and readiness contract (planned; not started)
- [ ] **Baseline / inspect.** Reconfirm `AGENTS.md`, `docs/app_description.md`, `tests/AGENTS.md`, database DDL, orchestration callers, and maintenance consumers on `dev`. Record existing behavior before edits. Do not change database objects or data during planning.
- [ ] **Monthly readiness.** Include `MONTHLY_CLIMATE_TABLE` and `MONTHLY_CLIMATE_DETAIL_TABLE` in `database.verification.CLIMATE_TABLES`, so all seven climate/operational tables are required by `inspect_database()` and `require_schema_ready()`. Monthly tables are planned application infrastructure, **not optional experimental database objects**. This changes the readiness check only; it must not activate monthly ETL, alter DDL, or modify existing data.
- [ ] **Consumers / maintenance safety.** Audit every consumer of `CLIMATE_TABLES`, especially `scripts/maintenance/destroy_all.py`, and update focused tests and any table-inventory expectations accordingly. Preserve all explicit destructive-action guards; no destructive commands against the live database.
- [ ] **Centralize engine lifecycle.** Make `orchestration/repositories/database.py::runtime_database_engine()` delegate to `database.connection.managed_database_engine(DatabaseConfig.from_env())`. Preserve the same target, context-manager API, connection semantics, and engine disposal on success/failure; do not introduce database creation at runtime.
- [ ] **Single PostGIS schema identifier.** Replace the duplicate hard-coded `POSTGIS_SCHEMA = "climate_data"` in `database/bootstrap.py` with the existing `APPLICATION_SCHEMA` constant, preserving generated SQL and extension validation.
- [ ] **Regression coverage.** Add/adjust focused tests for monthly tables missing/present in readiness, schema inspection, setup/maintenance consumers, engine disposal on success/failure, and unchanged PostGIS schema/SQL behavior. Include a negative fixture with a missing monthly table; tests must use isolated `TEST_POSTGRES_*` configuration when connecting to PostgreSQL.
- [ ] **Validate in order.** Run `python -m pytest tests/unit/database tests/unit/geospatial -q`, then `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit tests/airflow_orchestration -q` if that path exists (otherwise use the current repository test paths), then relevant PostgreSQL integration suites against the isolated test database. Record actual results; never infer success from historical P6 tests.
- [ ] **Documentation alignment.** Update the owning `docs/app_description.md` language to distinguish **future-use monthly persistence infrastructure** from **monthly aggregation scientific/operational acceptance still pending**. Adjust setup/readiness documentation and CLI table counts only where needed; do not silently claim that the monthly ETL is production-ready.
- [ ] **Close out.** Verify diff contains only the agreed changes, no DDL/schema/data mutation, no contract change beyond the explicitly approved readiness rule, and no new Airflow/scientific coupling. Record modified files, passing/failing/unrun checks, and remaining risks before marking this workstream complete.

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
