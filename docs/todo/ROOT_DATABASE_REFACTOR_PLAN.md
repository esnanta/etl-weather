# Root Database Package Refactor Plan

**Status:** DB-R2 pipeline, geospatial, and orchestration import/DDL migration completed at unit-test level (355 passed, user-reported 2026-10-16). Legacy package cleanup pending; PostgreSQL integration and DB-R1 live checks remain open.  
**Target branch:** `dev` exclusively.  
**Current priority:** Database refactor before P5 grid-to-region mapping; Airflow orchestration remains deferred.

## Objective and approved layout

Consolidate PostgreSQL database ownership from `pipeline/database/`, `geospatial/database/`, and database-creation logic distributed elsewhere into **one root Python package**. Avoid parallel implementations, hidden database creation and environment-specific schema drift.

```text
etl-weather/
├── database/
│   ├── __init__.py
│   ├── config.py
│   ├── connection.py
│   ├── bootstrap.py
│   ├── schema.py
│   ├── climate.py
│   ├── geospatial.py
│   └── verification.py
├── pipeline/               # Scientific ETL, loaders and read repositories
├── geospatial/              # Immutable sources, importers and spatial mapping
├── orchestration/           # Scheduling, operational repositories and tasks
├── scripts/                 # Thin guarded CLI entrypoints
└── tests/
```

The files above are the agreed **target**, not existing files. Do not create separate root `database/schema/` subpackages instead. Once migration is complete, remove `pipeline/database/` and `geospatial/database/` and update all references, not just add compatibility re-exports.

## Approved climate/geospatial separation (2026-10-16)

One flat root `database/` package has two independent DDL entrypoints:

- `initialize_climate_schema(engine, config)` owns **only** `t_daily_climate`, `t_daily_climate_detail`, `t_pipeline_progress`, `t_pipeline_progress_detail`, and `t_submission_pacing`.
- `initialize_geospatial_schema(engine, config)` owns **only** `t_province`, `t_district`, and `t_grid_region`, including PostGIS dependency.
- Airflow, DAG tasks, and orchestration repositories must not import, invoke, or initialize geospatial code or tables. Geospatial remains independent of Airflow.
- An explicit development database setup may invoke both paths to prepare the eight-table database. This is not permission for orchestration to invoke the geospatial path.

## Ownership and behavior boundaries

| Target module | Responsibility |
| --- | --- |
| `database/config.py` | Canonical schema/table/view names; explicit `POSTGRES_*` and `TEST_POSTGRES_*` configuration; no implicit production fallback for tests. |
| `database/connection.py` | Connection/engine lifecycle, maintenance connections and safe database-existence checks; normal runtime must not create databases implicitly. |
| `database/bootstrap.py` | Initialize application **or explicitly selected test** database; create `climate_data` and enable/verify PostGIS in **that schema**. |
| `database/schema.py` | Compose complete DDL initialization in dependency order; no duplicate SQL. |
| `database/climate.py` | Canonical weather table DDL, indexes and constraints; preserve existing scientific contracts. |
| `database/geospatial.py` | PostGIS-bound `t_province`, `t_district`, `t_grid_region` DDL and indexes; `boundary_geometry` stays XYZ. |
| `database/verification.py` | Read-only PostGIS placement, table/constraint/index and readiness checks; clear messages by database. |

The operational `t_pipeline_progress`, `t_pipeline_progress_detail`, and `t_submission_pacing` DDL must also be owned or composed from `database/schema.py` (choose a simple arrangement within the approved flat package, without recreating SQL in multiple modules). Operational **repository CRUD and pacing/selection behavior** remain under `orchestration/repositories/`. `pipeline/loaders/` and `geospatial/importers/` remain responsible for domain writes and import the shared database package; neither owns CREATE TABLE logic. The orchestration database module should not remain the global application-schema bootstrap owner.

## Database lifecycle contract

Development setup prepares **both** `weather` and `weather_test` (resolved from explicit environment settings, not hard-coded names) with the same required schema, PostGIS extension location and canonical table definitions. Empty test databases contain **structure only**; do not copy production observation or Aceh boundary rows into tests. Production setup must not touch test databases.

- **Initialize:** non-destructive, idempotent; create missing databases/extension/tables and verify them. Existing incompatible installations fail with context; do not silently relocate PostGIS or migrate incompatible layouts.
- **Recreate:** guarded, explicit destructive action with confirmation and no fallback to unexpected databases; call the **same** initialization function afterward.
- **`destroy_all`:** retain its full scope (application DB, test DB after approval, generated NetCDF/Parquet, Airflow logs/runtime and recognized processes). Preserve tracked configuration, source files including `Aceh.geojson`, and project code. Preflight **both** databases' connections, identities, roles, Airflow state and expected targets **before the first deletion**. Show both targets and maintain the exact `DESTROY` confirmation. Report partial failure honestly; never claim cross-database atomic rollback.
- **PostGIS:** server package is a prerequisite. Enable extension in `climate_data` per database; reference `climate_data.geometry` and `climate_data.ST_*` explicitly. Do not move an existing extension or execute `DROP EXTENSION ... CASCADE` automatically.
- **Isolation:** tests use only explicit `TEST_POSTGRES_*`; schemas/fixtures must not alter `weather`, other shared databases, or immutable sources. The test fixture must be redesigned to honor the production `climate_data` contract rather than patching a different application schema while DDL and functions hardcode `climate_data`.
- **Airflow metadata:** remains SQLite. Database refactor must not enable schedules, change DAG graphs, deferrals, pacing, retry or scientific processing.

## Confirmed inventory at planning time

- `pipeline/database/config.py`: `DatabaseConfig`, `APPLICATION_SCHEMA`, `t_*` / `vw_*` names, and engine/database-creation helpers.
- `pipeline/database/schema.py`: weather DDL.
- `geospatial/database/bootstrap.py`: PostGIS lifecycle; `geospatial/database/schema.py`: administrative/grid DDL.
- `orchestration/repositories/database.py`: currently composes application DDL; `orchestration/repositories/pipeline_progress.py` and `submission_pacing.py` also own operational table creation.
- `scripts/recreate_postgres_database.py` currently recreates **only** configured application database; `scripts/destroy_all.py` composes the full reset but currently recreates **only** that database and checks its five canonical tables.
- `tests/integration/geospatial/test_import_idempotency.py` assumes an existing `TEST_POSTGRES_*` database; the earlier `weather_test` was dropped by the user, so the current test suite cannot be assumed runnable until provisioning is rebuilt.
- Root `AGENTS.md` currently instructs `pipeline/database/` ownership and describes geospatial persistence under `geospatial/`. Update that guidance **during the refactor** so docs and code agree. Review `README.md`, `tests/AGENTS.md`, domain READMEs, CLI docs, SQL files, tests and CI references for stale import paths.

## Implementation checklist

### DB-R1 — Complete dependency audit and baseline
- [ ] Inventory every import and call to `pipeline.database`, `geospatial.database`, `orchestration.repositories.database`, create-table helpers, `CREATE EXTENSION`, and setup/reset scripts across code, tests, docs and CI.
- [ ] Record current public Python API, import-cycle risks, create-table ownership, `weather` state, `weather_test` absence and baseline tests runnable without a test database.
- [ ] Confirm `POSTGRES_*` and `TEST_POSTGRES_*` database identities and whether setup/recreation will require elevated PostgreSQL permissions. Never expose credentials in logs.
- [ ] Confirm destructive operation target/confirmation policy for *both* databases before implementing it.

### DB-R2 — Root package and import migration
- [x] Stage the approved flat `/database` package and new configuration/connection utilities (not yet adopted by callers; regression pending).
- [x] Stage copies of canonical weather DDL, geospatial DDL and PostGIS bootstrap into target modules; original owners remain until migration and validation.
- [x] Stage `database/schema.py` with **two independent** entrypoints (five climate/operational tables and three spatial tables); runtime adoption and SQL regression still pending.
- [x] Move operational CREATE TABLE ownership out of orchestration repositories without moving operational business logic (unit regression: 122 orchestration / 355 total passed; integration outstanding).
- [ ] Update imports across runtime, scripts, tests and CI; remove old `pipeline/database` and `geospatial/database` only after no callers remain.
- [ ] Keep code and docstrings in English; avoid stale compatibility aliases and duplicate bootstrap paths.

### Step 1 audit evidence — dev Git tree scan (2026-10-16)

A GitHub **dev** recursive tree request returned 271 entries (`truncated=false`). Source-level checks confirmed the following active legacy references; this is a working inventory, **not yet proof of a full `git grep` pass**:

| Group | Verified remaining references | Migration target |
| --- | --- | --- |
| Maintenance/monitoring scripts | `scripts/destroy_all.py`, `inspect_campaign_capacity.py`, `recreate_postgres_database.py`, `report_campaign_progress.py`, `reset_all.py` | `database.config`, `database.connection` and appropriate root schema functions |
| Orchestration domain reads | `orchestration/repositories/load_evidence.py` | `database.config` / `database.climate` identifiers only |
| Integration tests | `tests/integration/geospatial/test_import_idempotency.py`, `tests/integration/pipeline/test_postgresql_idempotency.py`, `tests/integration/orchestration/test_orchestration_database_bootstrap.py`, `tests/integration/orchestration/test_pipeline_progress_reconciliation.py` | Root database imports; geospatial fixture requires DB-R3 isolation redesign |
| Unit tests | `tests/unit/geospatial/test_importer.py`; `tests/unit/operations/test_destroy_all.py`, `test_recreate_postgres_database.py`, `test_reset_all.py` | Root `DatabaseConfig` |
| CLI and documentation | `README.md`, `geospatial/README.md` use `python -m geospatial.database.bootstrap` / `schema`; `docs/todo/DATABASE_PREFIX_AND_ADMINISTRATIVE_GRID_PLAN.md` has historical references | New CLI entrypoints plus clearly labeled historical notes |
| Legacy implementation | `pipeline/database/{__init__,config,schema}.py`, `geospatial/database/{__init__,bootstrap,schema}.py` | Remove only after migration and CLI verification |

**Additional documentation discrepancy:** root `README.md` currently describes PostGIS installed in `public`, conflicting with the approved `climate_data` placement. Correct during Step 6; verify actual PostgreSQL extension placement separately.

**Next audit gate:** run on local `dev`:

```bash
git grep -n -E 'pipeline[./]database|geospatial[./]database|create_database_engine|ensure_database_exists' -- '*.py' '*.md' '*.sql' '*.sh' '*.yml' '*.yaml'
```

Exclude clearly marked historical records when deciding code removal, but do not silently delete past implementation evidence. Step 1 stays unchecked until the full reference inventory and import/CLI impact review is confirmed.

### Step 1 completed — local git grep evidence (user report, 2026-10-16)

The user executed both targeted `git grep` checks on their local `dev` checkout and supplied full terminal output. Active legacy Python imports were identified in `geospatial/config.py`, `orchestration/campaign/reporting.py`, `orchestration/repositories/load_evidence.py`, maintenance scripts (`destroy_all.py`, `recreate_postgres_database.py`, `report_campaign_progress.py`, `reset_all.py`), geospatial/pipeline/orchestration integration tests, geospatial importer unit test, and three operations unit test modules. The remaining internal imports in `pipeline/database/` are expected until package deletion. Separately audited `scripts/inspect_campaign_capacity.py`, which contains function-local legacy imports not matched by the start-of-line-only grep pattern.

CLI/documentation references remain in `README.md`, `geospatial/README.md`, `query/README.md`, and the historical `docs/todo/DATABASE_PREFIX_AND_ADMINISTRATIVE_GRID_PLAN.md`. References in this plan are migration tracking/history, not runtime imports. Root `database.connection.create_database_engine` is canonical, not a legacy symbol to delete.

**Step 1 acceptance:** complete for the agreed grep scope; inventory is known. Step 2 remains pending until migrated paths and test isolation are checked.

### DB-R2 final cleanup — approved execution order (2026-10-16)

**Scope:** `dev` only. Retain one canonical root `database/` package. Domain reads remain in `pipeline/repositories/`, ETL writes in `pipeline/loaders/`, and progress/pacing operations in `orchestration/repositories/`. No DAG or scientific behavior change. Geospatial must remain independent from orchestration. No database drops, extension relocation, or source-data deletion.

**Step 1 — Audit every remaining legacy reference**
- [x] Inventory `pipeline.database`, `geospatial.database`, path-based CLI invocations, imports of old DDL/engine helpers, and stale documentation in `pipeline/`, `geospatial/`, `orchestration/`, `scripts/`, `tests/`, `.github/`, and Markdown/SQL files on **dev**.
- [x] Record path + symbol + replacement/retirement decision for every hit. Include potential import cycles and commands documented as `python -m geospatial.database.bootstrap` / `schema`.
- [x] Confirm references with a full working-tree `git grep` on `dev`; GitHub code search on default branch alone is not acceptable evidence.

**Step 2 — Migrate all remaining callers to root `database/`**
- [ ] Replace runtime, maintenance-script, test, and documentation references with canonical `database.config`, `database.connection`, `database.climate`, `database.geospatial`, `database.schema`, or `database.bootstrap`, as appropriate.
- [ ] Keep loader / repository behavior and operational SQL unchanged, with no silent DB creation and no permanent re-export compatibility aliases.
- [ ] Verify application and test DB configuration remain distinct; never fall back from `TEST_POSTGRES_*` to `POSTGRES_*`.

**Step 3 — Preserve geospatial CLI capabilities**
- [ ] Decide/document new supported `python -m database.bootstrap` / `python -m database.geospatial` entrypoints or thin CLI scripts before deleting old module paths; preserve dry-run default, `--execute` guard, intended DB identity, and no automatic GeoJSON import.
- [x] Verify help/dry-run output and non-destructive operation of new commands. Do not activate PostGIS or create tables merely to check help/dry-run.

**Step 4 — Remove old database packages**
- [ ] Delete `pipeline/database/` and `geospatial/database/` **only after** Steps 1–3 are verified.
- [ ] Confirm no import, CLI, test, CI, or documentation reference requires deleted modules. Root `database/` must be the sole owner of DDL/config/connection/PostGIS bootstrap.

**Step 5 — Verify after removal**
- [ ] Run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` (pre-cleanup baseline **355 passed in 9.16s**, as reported by user).
- [ ] Run focused unit suites (`tests/unit/pipeline`, `tests/unit/geospatial`, `tests/unit/orchestration`) and validate maintained CLI help/dry-run behavior.
- [ ] Run isolated `TEST_POSTGRES_*` integration tests **when test DB provisioning is ready**; never target the application `weather` database. Report skipped/missing DB as unverified, not passed.

**Step 6 — Align instructions and documentation**
- [ ] Update `AGENTS.md`, `README.md`, `tests/AGENTS.md`, `tests/README.md`, related domain READMEs, SQL/docs and workflow references to reflect root database ownership and current CLI.
- [ ] Verify no duplicated or contradictory architecture instructions and no stale `pipeline/database` or `geospatial/database` references (except clearly labeled historical migration notes).
- [ ] Record actual commands, observed outputs, changed files, and evidence here before marking DB-R2 cleanup complete.

**Acceptance gate:** All legacy Python imports and module CLI paths removed/replaced; CLI dry-run works; the 355-unit-test baseline is preserved; no geospatial dependency from orchestration; integration status explicitly documented. Do not mark DB-R2 complete merely because deleted folders disappear.

## DB-R3 setup/bootstrap responsibility decision and additional guards (2026-10-17)

**Decision retained:** `database/setup.py` provisions **two explicit application/test PostgreSQL targets**; `database/bootstrap.py` initializes **one existing target** by composing the independently callable five-table climate and three-table spatial schema paths. The explicit `database.climate` and `database.geospatial` CLIs remain available. Airflow does not use provisioning or the combined eight-table bootstrap.

- [x] Hardened `database.setup.initialize_development_databases(application, testing)` so **direct Python calls** enforce the same `APP_ENV=dev`, canonical schema, system-database refusal, test-name marker and distinct target checks as the CLI before opening connections. CLI's no-connection dry-run is preserved.
- [x] Added unit coverage for production environment refusal, same-target refusal, unmarked test target refusal, and one-database bootstrap composition/disposal. The preflight-order test now explicitly sets `APP_ENV=dev`.
- [ ] Local verification: `python -m database.setup --help`, `python -m database.setup` (both dry-run) and `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/database -q` followed by full unit regression. Results are **not reported** yet.
- [ ] Provision and verify isolated `TEST_POSTGRES_*` only with explicit operator approval to run `--execute`. No PostgreSQL connections or writes were performed by these source changes.

## DB-R3 dotenv-isolation test correction (user report, 2026-10-17)

- CLI `python -m database.setup --help` and default dry-run succeeded: distinct `localhost:5432/weather` and `localhost:5432/weather_test` in `climate_data`; no DB connection or changes in dry-run.
- Local `tests/unit/database -q`: **12 passed, 1 failed**; all units: **362 passed, 1 failed in 8.16s**. Both failures are the same `test_missing_test_configuration_never_uses_application_settings` case.
- Cause: deleting `TEST_POSTGRES_*` from `os.environ` allows `DatabaseConfig.from_env()` to repopulate these values from the user's existing `.env` via `load_dotenv()`, so the test's missing-config scenario was not actually isolated.
- Fixed the **unit test fixture** to pass an explicit environment mapping without `TEST_POSTGRES_*` directly to `resolve_test_database_config(env)`, bypassing dotenv; this still validates that test config never falls back to `POSTGRES_*`. Production dotenv behavior remains unchanged.
- **Awaiting rerun after correction**; do not record a passing 363-test baseline until user reports it. No PostgreSQL integration or setup `--execute` has been run.

## DB-R3 regression verification (user report, 2026-10-17)

- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/database -q` → **13 passed in 0.59s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **363 passed in 7.65s** (the supplied command text displayed `RA5_CAMPAIGN_SCHEDULE_ENABLED`, but pytest completed successfully; recheck the environment-variable spelling in future commands).
- The isolated dotenv fixture correction is verified. These results do not verify PostgreSQL provisioning or transactional integration.
- Next DB-R3 gate: review two-target preflight, roles/permissions, and existing database state before any explicit `database.setup --execute`.

## DB-R3 successful dual-database provisioning (user report, 2026-10-17)

- Read-only inspection of existing `weather`: `schema_exists=True`, `postgis_schema=climate_data`, `climate_tables_missing=()`, `geospatial_tables_missing=()`.
- Executed `python -m database.setup --execute` locally under `APP_ENV=dev`: existing `weather` was initialized idempotently; missing `weather_test` was created, initialized and verified.
- Both targets logged PostGIS **3.4.2** in `climate_data`; setup finished `Application and isolated test database schemas verified.`.
- **Still to verify:** test database contains no copied observations or imported administrative rows; live integration suites pass; cleanup and cross-database failure behavior remain review items. Do not mark integration coverage verified based on schema readiness alone.

## DB-R3 live integration verification (user report, 2026-10-17)

- The user inspected all eight `climate_data` tables in newly provisioned `weather_test` and reported **0 rows in every table before integration execution**: `t_daily_climate`, `t_daily_climate_detail`, `t_pipeline_progress`, `t_pipeline_progress_detail`, `t_submission_pacing`, `t_province`, `t_district`, `t_grid_region`.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration -q` → **15 passed in 1.69s**.
- Existing unit regression: **363 passed in 7.65s**; dual-target setup previously reported success, PostGIS 3.4.2 in both databases, all eight tables present.
- This establishes successful fresh provisioning and live isolated integration behavior. Unit and integration counts are separate test suites.
- Remaining DB-R3 review: repeated `setup --execute` behavior and error handling on partial cross-database failures; no destructive recreation is authorized by this evidence.

## DB-R3 repeat setup verification (user report, 2026-10-17)

- Re-ran `python -m database.setup --execute` against **existing** `weather` and `weather_test`.
- Both reported schema `climate_data` ready, climate table readiness, PostGIS version **3.4.2** in `climate_data`; command completed with `Application and isolated test database schemas verified.`.
- No `Creating missing PostgreSQL database` message appeared in the repeat run. This verifies successful repeated initialization on the tested state, not byte-for-byte data/index preservation.
- DB-R3 acceptance evidence: **363 unit passed**, **15 PostgreSQL integration passed**, initial test database empty across eight tables, dual-target first provisioning successful, and repeat setup successful. DB-R3 functional acceptance complete; partial-failure compensation and destructive operations are separate hardening / DB-R4 concerns.

## DB-R4 initial destructive-maintenance alignment (2026-10-17)

- [x] Audited `scripts/recreate_postgres_database.py` and `scripts/destroy_all.py` against root `database/` ownership. Existing recreation already had `APP_ENV=dev`, inactive campaign/Airflow checks, PostgreSQL-session checks, interactive confirmation and safely quoted DROP/CREATE identifiers; destroy additionally requires exact `DESTROY` and stops recognized Airflow processes before database recreation.
- [x] Replaced the obsolete **climate-only** `orchestration.repositories.database.initialize_application_storage` call in `_recreate_database` with root `database.bootstrap.bootstrap(config)`. A recreated application database now uses the same five climate plus three spatial DDL paths as DB-R3; PostGIS server installation and permissions are prerequisites.
- [x] Updated `destroy_all` final database verification to require all **eight** canonical tables from `database.verification` and the PostGIS extension in `climate_data` (formerly checked five tables only).
- [x] Added a targeted unit contract assertion for the eight-table verification list.
- [ ] **Do not execute destructive scripts yet.** Existing `destroy_all` still covers the application database, generated artifacts and Airflow runtime, but **does not drop/recreate `weather_test`**. Expanding destructive scope is a separately approved policy decision; no implicit test database deletion.
- [ ] Examine the remaining preflight gap: `destroy_all` checks external DB sessions after stopping Airflow; it should check database-specific privileges, readiness and both relevant targets **before the first destructive step**, then recheck sessions immediately before DROP. Likewise, the recreation CLI should validate root-bootstrap prerequisites prior to DROP.
- [ ] Run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/operations -q` then full unit suite. No local tests or destructive execution have been performed by this source change.
- [ ] Update CLI/maintenance documentation after the full DB-R4 safety design is resolved.

## DB-R4 safety gate correction (2026-10-17)

- User-confirmed regression before this change: `tests/unit/operations -q` **91 passed in 1.73s**; full `tests/unit -q` **364 passed in 9.94s**.
- [x] Added read-only `_validate_bootstrap_prerequisites(config)` to `scripts/recreate_postgres_database.py`, checking canonical schema, target database existence, owner membership, database creation privileges, PostGIS server availability and superuser authorization for PostGIS activation **before DROP**. This check is a conservative prerequisite, not a guarantee of future DDL success.
- [x] The standalone recreation CLI runs privilege/PostGIS preflight together with runtime preflight before confirmation, then repeats them immediately before `_recreate_database`.
- [x] `scripts/destroy_all.execute_destroy` now runs privilege/PostGIS preflight and checks external database sessions **before terminating Airflow**. It retains the second session check directly before DROP to catch new connections.
- [x] Added targeted tests ensuring failing PostgreSQL/PostGIS preflight cannot terminate Airflow, and updated order assertions to include early session preflight.
- [ ] **Regression pending** after these modifications. No destructive commands were executed.
- [ ] Existing tests and operational behavior should be reviewed for roles that can create PostGIS without superuser; conservative policy deliberately rejects such roles rather than risk data loss. PostgreSQL permissions and server state can change after preflight: a successful preflight is not transactional protection across DROP/CREATE.

## DB-R4 repeated-preflight test correction (user report, 2026-10-17)

- After early-preflight change, local `tests/unit/operations -q`: **92 passed, 1 failed**; full `tests/unit -q`: **365 passed, 1 failed in 8.83s**.
- The same test failed in both suites: `test_execute_yes_recreates_only_after_preflight` still expected `preflight → confirm → recreate`, while the intentional guarded runtime order is `preflight → confirm → preflight → recreate`.
- Updated only the expected assertion in `tests/unit/operations/test_recreate_postgres_database.py` to match and protect the second preflight; no destructive behavior was changed.
- Awaiting rerun of unit suites. Do not run destructive `--execute` commands for test verification.

## DB-R3 initial implementation (2026-10-17)

- [x] Recorded DB-R2 final user regression: `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 8.01s**.
- [x] Added `database/setup.py`, a separate guarded development/test setup CLI. Default mode only resolves and displays distinct `POSTGRES_*` and `TEST_POSTGRES_*` targets; no database connection or DDL. `--execute` is allowed only under `APP_ENV=dev` and preflights both targets' maintenance access, PostGIS server availability, target database identity and existing extension-schema placement before the first write.
- [x] Explicit execute creates missing targets through the PostgreSQL maintenance database with safely quoted identifiers, delegates eight-table initialization to the same combined `database.bootstrap.bootstrap(config)` function for each, and uses read-only schema readiness verification afterward. No data copying and no automatic DROP/recreate.
- [x] Added `tests/unit/database/test_setup.py` for env isolation, distinct identities and preflight ordering.
- [x] Verify dry-run help and configured targets locally; run unit regression. Baseline **355** unit tests before new tests; the number may increase with added test cases.
- [ ] Test live provisioning against explicitly isolated `TEST_POSTGRES_*` under operator control; integration tests have not been executed by the assistant.
- [ ] Review remaining lifecycle/setup parity and failure behavior before marking DB-R3 fully complete. Database provisioning across two PostgreSQL DBs is not atomic.

### DB-R3 — Unified initialize and test provisioning
- [ ] Implement explicit, idempotent initialize for the application database; verify existing schemas, PostGIS and all expected tables.
- [ ] Implement guarded development setup that prepares both application and test databases using dedicated `POSTGRES_*` and `TEST_POSTGRES_*` settings; test DB missing is **created**, not silently skipped.
- [ ] Use common schema/table initializer for both; ensure test data are not copied from development.
- [ ] Ensure normal loaders, readers, Airflow tasks and importers **never auto-create** a missing database.
- [ ] Repair test-fixture isolation under `climate_data` without patching production code into contradictory schema identities.

### DB-R4 — Maintenance command integration
- [ ] Update `scripts/recreate_postgres_database.py` and `scripts/destroy_all.py` to call common initialization logic, with existing guards, full delete scope and interactive confirmation preserved.
- [ ] Display both database targets and verify **both** are eligible; block on active sessions in either database *before* destructive steps.
- [ ] Keep generated data, Airflow logs/runtime and recognized-process cleanup unchanged; never wipe `geospatial/sources/`.
- [ ] Maintain readable step headers and actionable error context; show verification status for each database.
- [ ] Handle partial failures safely and explicitly (PostgreSQL database drop/create is not one atomic two-database transaction).

### DB-R5 — Automated regression and actual PostgreSQL verification
- [ ] Unit tests: config separation, bootstrap delegation, DDL shape, no unexpected DB creation, permissions/preflight failures, operations guards and no mutation during inspect.
- [ ] Integration tests: provision fresh test database with PostGIS under `climate_data`; verify all tables/indexes/FKs; repeat initialize with zero schema/data loss.
- [ ] Integration tests: geospatial repeated import, partial input, forced rollback and failed retry (existing four scenarios); weather load idempotency and transaction rollback; operational pacing/progress tests.
- [ ] Negative tests: incorrect PostGIS schema, missing package/privileges, unexpected database name, test DB missing, open sessions and interruption after one of two database operations.
- [ ] Run relevant unit, isolated PostgreSQL integration and Airflow orchestration regression suites with `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false`; preserve DAG behavior.

### DB-R6 — Documentation, cleanup and acceptance
- [ ] Update root `AGENTS.md`, `README.md`, `tests/AGENTS.md`, relevant domain READMEs and SQL references without duplicating generic database instructions in geospatial docs.
- [ ] Retire old database package directories and verify no stale references in code, tests, docs or GitHub workflows.
- [ ] Verify the development setup creates `weather` and `weather_test`, both with PostGIS in `climate_data` and all required tables.
- [ ] Verify `destroy_all` retains full clean-slate scope and works only with explicit confirmation; verify preserved sources/config and clean test-database state.
- [ ] Record real test output and SQL verification evidence in this checklist; **do not mark done based solely on code inspection**.
- [ ] Only after database refactor acceptance, resume P4 integration verification and P5 spatial mapping. Airflow remains deferred.

## 2026-10-16 DB-R1 / DB-R2 execution evidence

- Inspected the identified source files in the `dev` branch and confirmed scattered CREATE TABLE ownership, implicit database creation in `pipeline.database.config.create_database_engine`, geospatial temporary-schema monkeypatching and one-database maintenance preflight.
- Source audit is **not complete**: repository-wide callers, all tests, CI and SQL references require a full working-tree scan. Do not mark DB-R1 completed until that scan is checked.
- Live connection identity, database existence, PostgreSQL roles/permissions, and the actual PostGIS schema are **not verified** in this GitHub-only environment. `weather_test` absence is historical user-provided context, not a fresh SQL observation.
- New files staged on `dev`: `database/__init__.py`, `config.py`, `connection.py`, `climate.py`, `geospatial.py`, `schema.py`, `bootstrap.py`, `verification.py`; plus `tests/unit/database/test_root_config.py`.
- **No callers were migrated**, old packages remain, no PostgreSQL DDL/destructive commands were executed, and pytest was **not executed**. New unit tests and schema composition must be run and reviewed before any old module is removed.
- Next steps: run local read-only DB identity/privilege queries and existing baseline tests; migrate callers in bounded groups while preserving domain separation; remove duplicate DDL only after successful regression.

## Verified local unit-test baseline (user report, 2026-10-16)

- `python -m pytest tests/unit/database -q` → **5 passed in 0.13s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 9.37s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/orchestration -q` → **122 passed in 4.85s** (included in the 355 overall tests, not additive).
- These were executed and reported by the user on their workstation, not by the GitHub integration. They establish the pre-import-migration unit regression baseline only; PostgreSQL integration and live database inventory remain outstanding.

## DB-R2 pipeline import migration (2026-10-16)

- Migrated `pipeline/loaders/daily_climate_postgres.py` to `database.config`, `database.connection`, and `database.climate`.
- Migrated `pipeline/repositories/daily_climate_reader.py` to `database.config`.
- Redirected `tests/unit/pipeline/test_database.py`, `test_load.py`, and `test_daily_climate_repository.py` to root database modules.
- The new `database.connection.create_database_engine` does **not** create a missing database implicitly. This is an intentional lifecycle boundary change and must be covered by regression tests. The loader still invokes weather-table DDL at runtime; ownership/removal of implicit table creation remains an open design/implementation item.
- No `geospatial/`, `orchestration/`, database maintenance scripts or old database packages were modified in this step. Existing callers in those domains still use old paths.
- **Verification pending:** focused pipeline unit pytest and complete unit regression on user's workstation. The GitHub connector cannot execute those tests or access local PostgreSQL. Do not mark this migration accepted until evidence is supplied.

## DB-R2 pipeline migration regression evidence (user report, 2026-10-16)

The user executed these commands on the updated local `dev` working tree:

- `python -m pytest tests/unit/pipeline/test_database.py tests/unit/pipeline/test_load.py tests/unit/pipeline/test_daily_climate_repository.py -q` → **19 passed in 0.65s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/pipeline -q` → **119 passed in 2.05s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 8.97s**.

The focused tests and pipeline tests are included in the full unit count; do not add totals. This validates the pipeline import migration at unit level. It does not establish actual PostgreSQL integration, lack of runtime DDL, or correctness of the future geospatial/orchestration migrations. Next: migrate geospatial callers while keeping the Airflow boundary unchanged.

## DB-R2 geospatial import migration (2026-10-16)

- `geospatial/importers/districts.py` now imports PostgreSQL configuration, PostGIS bootstrap and engine construction from root `database/`.
- `geospatial/database/schema.py` and `geospatial/database/bootstrap.py` are temporary **CLI-only wrappers** that invoke root DDL/bootstrap. The old package cannot be deleted until the documented module CLI invocations are migrated and all old callers are removed.
- No geospatial SQL write/upsert, GeoJSON source files, geometry XYZ definition, or orchestration DAGs were modified. Root `database.schema` still provides separate climate (five tables) and geospatial (three tables) initialization paths.
- Known integration-test blocker: `tests/integration/geospatial/test_import_idempotency.py` monkeypatches `geospatial.database.schema.APPLICATION_SCHEMA` and uses a temporary schema instead of canonical `climate_data`. It must be refactored as part of isolated test DB provisioning; do not run it against `weather`.
- **Tests pending:** focused geospatial units and full unit regression must be executed on the user's workstation before accepting DB-R2 geospatial migration. No local pytest or PostgreSQL access is available through the GitHub connector.

## DB-R2 geospatial test correction (2026-10-16)

- User ran `tests/unit/geospatial -q` → **18 passed, 1 failed**; full `tests/unit -q` → **354 passed, 1 failed**. Both failures came from `test_spatial_schema.py` patching `geospatial.database.schema.ensure_postgis_extension`, which moved to `database.geospatial.ensure_postgis_extension`.
- Updated the schema test to import `create_spatial_tables` and `DatabaseConfig` from root `database/`, and patch the actual new dependency target `database.geospatial.ensure_postgis_extension`. No production SQL, data, or Airflow code was modified in this correction.
- Re-run geospatial and complete unit suites on local `dev` before marking accepted. This GitHub-only environment has not executed pytest.

## DB-R2 geospatial regression passed (user report, 2026-10-16)

- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q` → **19 passed in 0.58s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 9.03s**.
- These runs were executed on the user's local `dev` checkout after updating the stale test patch target. Geospatial migration is accepted at the **unit regression** level, but isolated PostgreSQL integration remains pending.
- Next DB-R2 phase: migrate orchestration database imports and operational DDL ownership to the root package without adding any geospatial dependency to Airflow.

## DB-R2 orchestration migration — partial (2026-10-16)

- `orchestration/repositories/database.py` now delegates its five-table bootstrap calls to DDL constructors imported from root `database.climate` and `database.schema`. No geospatial initializer is called by orchestration.
- `orchestration/repositories/pipeline_progress.py` and `submission_pacing.py` now import configuration identifiers from root `database.config`.
- **Not yet complete:** Duplicate CREATE TABLE functions still exist in the two operational repositories and their remaining references/tests require migration. The requested removal of DDL ownership is not accepted. An attempt to apply a large edit to the progress module was blocked by tooling; no claim of completion is made.
- **Regression not run:** Local `tests/unit/orchestration -q` and `tests/unit -q` need to be executed after these changes. The pre-change baseline was 122 and 355 passing tests respectively. PostgreSQL integration remains outstanding.
- No DAG scheduling/retry/deferral or geospatial code was changed in this partial orchestration step.

## DB-R2 orchestration partial migration regression (user report, 2026-10-16)

- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/orchestration -q` → **122 passed in 4.87s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 8.42s**.
- This verifies the **partial** orchestration import/bootstrap delegation migration at unit-test level. The 122 tests are included in the total 355, not additive.
- Duplicate DDL still exists in `orchestration/repositories/pipeline_progress.py` and `submission_pacing.py`; removing it and redirecting callers remains required before DB-R2 can close.
- No live PostgreSQL integration verification has been executed in this stage.

## DB-R2 operational DDL deduplication (2026-10-16)

- Removed the duplicate `create_pipeline_progress_tables` implementation from `orchestration/repositories/pipeline_progress.py`; `database/schema.py` retains the canonical definition. The repository retains progress CRUD, reconciliation and selection behavior.
- Removed the duplicate `create_submission_pacing_table` implementation from `orchestration/repositories/submission_pacing.py`; `database/schema.py` retains the canonical definition. The repository retains reservation/locking behavior.
- Updated operational repository public exports, progress DDL unit tests and PostgreSQL progress/pacing integration-test imports to use the root owner.
- `database/schema.py` now loads `database.geospatial` only inside explicit `initialize_geospatial_schema`; importing the orchestration climate DDL path does not import spatial DDL code.
- **Unit regression is pending after these edits**. Latest verified baseline before DDL removal: orchestration **122 passed** and all unit tests **355 passed**. Integration PostgreSQL remains outstanding.
- Legacy `pipeline/database/` and `geospatial/database/` packages remain in place. Do not remove them until full reference search and passing regression.

## DB-R2 operational DDL regression and legacy package decision (user report, 2026-10-16)

- After removing duplicated operational DDL, `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/orchestration -q` → **122 passed in 4.95s**.
- Full `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 9.16s**.
- Agreed target: remove legacy `pipeline/database/` and `geospatial/database/` once every remaining caller, scripts, tests and documentation have migrated; do **not** move infrastructure/DDL into `pipeline/repositories/`.
- `pipeline/repositories/` continues to own domain-specific climate reads; `pipeline/loaders/` retains transactional climate writes. Root `database/` owns shared connection, config, bootstrap, verification and DDL; `orchestration/repositories/` retains progress/pacing operations.
- Deletion is **not yet done**; audit of remaining references and post-removal regressions is required. PostgreSQL integration tests remain outstanding.

## DB-R2 Step 1 local grep evidence (user report)

The user ran `git grep -n -E 'pipeline[./]database|geospatial[./]database|create_database_engine|ensure_database_exists' -- '*.py' '*.md' '*.sql' '*.sh' '*.yml' '*.yaml'` on their working tree. The pasted output ended after a leading colon and is **incomplete**; do not treat the list as an exhaustive zero-stale-import result.

Verified visible hits: `README.md` still uses legacy spatial CLI and incorrectly describes PostGIS under `public`; `docs/todo/DATABASE_PREFIX_AND_ADMINISTRATIVE_GRID_PLAN.md` contains historical legacy paths; `database/bootstrap.py` and `database/connection.py` correctly use the **new** `create_database_engine` function. These new canonical symbols are not cleanup targets. Step 1 checklist remains unchecked pending complete inventory and cross-check with the dev tree scan above.

## DB-R2 Step 2 partial migration (2026-10-16)

- After user-provided complete active-import grep evidence, updated root `database.config` imports in `geospatial/config.py`, `orchestration/campaign/reporting.py`, `orchestration/repositories/load_evidence.py`, `scripts/destroy_all.py`, `scripts/recreate_postgres_database.py`, `scripts/report_campaign_progress.py`, `scripts/reset_all.py`, and function-local references in `scripts/inspect_campaign_capacity.py`.
- Updated direct `DatabaseConfig` imports in `tests/integration/pipeline/test_postgresql_idempotency.py`, `tests/unit/geospatial/test_importer.py`, and three `tests/unit/operations/` modules.
- **Not yet completed:** orchestration integration test imports, geospatial integration fixture replacement, geospatial CLI preservation, package removal, and documentation cleanup. No database operations were performed. Unit regression after these edits **not yet run**; previous 355 passed baseline predates these edits.
- Do not delete the legacy packages until all active-import grep matches are eliminated and safe replacement for temporary-schema geospatial integration test is designed (DB-R3).

## DB-R2 Step 2 intermediate unit regression (user report, 2026-10-16)

- After migrating thirteen remaining application/script/test configuration imports to root `database.config`, the user executed `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q`: **355 passed in 8.70s**.
- This verifies unit regression only, not live PostgreSQL or geospatial integration fixture correctness.
- Remaining integration references and CLI migration are tracked as open; do not remove the legacy packages yet.

## DB-R2 Step 2 additional integration import migration (user report, 2026-10-16)

- Latest local unit regression following thirteen caller migrations: **355 passed in 8.70s**.
- Updated `tests/integration/orchestration/test_orchestration_database_bootstrap.py` and `tests/integration/orchestration/test_pipeline_progress_reconciliation.py` to import config and climate DDL from root `database/`.
- **Unverified after latest edits:** no new local pytest/integration results yet. `tests/integration/geospatial/test_import_idempotency.py` still needs isolated test database fixture redesign; do not delete old package or use application database to run it.

## DB-R2 integration collection evidence (user report, 2026-10-16)

- `python -m pytest tests/integration/orchestration/test_orchestration_database_bootstrap.py tests/integration/orchestration/test_pipeline_progress_reconciliation.py --collect-only -q` → **5 tests collected in 0.60s**.
- Collection succeeds with migrated root imports; test execution and PostgreSQL behavior are not verified by this result.
- Remaining priority: geospatial integration fixture isolation under canonical `climate_data`, then new spatial CLI and safe legacy-package retirement.

## DB-R2 geospatial integration fixture migration (2026-10-16)

- Migrated `tests/integration/geospatial/test_import_idempotency.py` to canonical root `database.geospatial.create_spatial_tables` and explicit `database.config.resolve_test_database_config`.
- Removed temporary schema creation, monkeypatching of `APPLICATION_SCHEMA`, and destructive `DROP SCHEMA ... CASCADE`. The fixture now uses `climate_data` only within a dedicated `TEST_POSTGRES_*` database.
- Fixture rejects an unexpected database identity, misplaced PostGIS, or pre-existing rows in any of the three spatial tables. It cleans up the test importer dataset's district and province rows rather than dropping schemas. SQL uses explicitly qualified `climate_data.ST_*` calls.
- **Caveat:** it requires an empty and isolated spatial test environment and does not support concurrent suites writing to these three tables. Broader test-database provisioning and isolation remain DB-R3. No integration tests have been executed against live PostgreSQL by this change.
- **Validation pending:** geospatial integration `--collect-only`, focused geospatial unit regression and complete unit regression on user workstation, followed by isolated integration only after confirming `TEST_POSTGRES_*` points at a disposable test database (never `weather`).
- Legacy geospatial CLI replacement and removal of old packages remain separate unchecked steps.

## DB-R2 geospatial fixture verification (user report, 2026-10-16)

- `python -m pytest tests/integration/geospatial/test_import_idempotency.py --collect-only -q` → **4 tests collected in 0.15s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q` → **19 passed in 0.17s**.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 8.44s**.
- These outcomes validate collection and unit regression only; no live PostgreSQL integration behavior has been verified.

## DB-R2 Step 3 root geospatial CLI implementation (2026-10-16)

- Root `python -m database.bootstrap` now defaults to read-only target preview and requires `--execute` to initialize or verify PostGIS (the legacy wrapper previously executed immediately; this deliberate new guard must be reflected in operator instructions).
- Root `python -m database.geospatial` already previews by default and requires `--execute` for DDL; it now uses `database.connection.create_database_engine` instead of a direct SQLAlchemy engine factory.
- Updated `README.md` and `geospatial/README.md` for the new CLI and corrected the documented PostGIS location to `climate_data`.
- **Pending user verification:** both `--help` and default dry runs (no `--execute`) and full unit regression. Do not delete legacy wrappers yet; no DB mutation has been executed by this work.
- Historical notes in `docs/todo/DATABASE_PREFIX_AND_ADMINISTRATIVE_GRID_PLAN.md` should retain original citations and be explicitly marked as historical, not silently rewritten.

## DB-R2 root spatial CLI verified (user report, 2026-10-16)

- `python -m database.bootstrap --help` and `python -m database.geospatial --help`: both succeeded.
- `python -m database.bootstrap`: `PostGIS target: database=weather, schema=climate_data`, followed by `Dry run: database unchanged`.
- `python -m database.geospatial`: lists `t_province`, `t_district`, `t_grid_region` with target `weather.climate_data` and `Dry run: no database changes`.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q`: **355 passed in 8.56s**.
- No `--execute` was used. CLI execution against a live database and PostgreSQL integration remain unverified.
- Step 3 CLI `--help`/dry-run acceptance is satisfied. Step 4 package deletion requires final active-import audit.

## DB-R2 legacy package removal (2026-10-16)

- Deleted all six tracked files from `pipeline/database/` and `geospatial/database/` on `dev`, after auditing active known import sites and confirming the replacement spatial CLI help/dry-run commands.
- Final deletion commit: `581bfdd15928d380cd35ee2ee868910d641f0a2c`. Git does not track empty directories; the old package paths should disappear on a clean checkout. Local `__pycache__` files can remain untracked and should not be interpreted as source modules.
- **Crucial verification gate:** the user-reported **355 passed in 8.56s** was **before** deletion. Rerun complete units and all test collection on the new revision; do not mark Step 4/5 accepted before confirming no missing import or broken CLI.
- Historical documentation references may remain in archival notes. Root `AGENTS.md`, root `README.md`, and other guidance still require Step 6 alignment. Isolated PostgreSQL execution remains a DB-R3/DB-R5 gate.

## DB-R2 post-removal regression defect and fix (user report, 2026-10-16)

- Local grep for top-level old-package imports returned no hits, but this missed a **string-based subprocess import** in `tests/unit/orchestration/test_campaign_config.py`.
- Actual post-deletion unit test: **354 passed, 1 failed in 7.51s**; failure was `ModuleNotFoundError: pipeline.database.config` in that subprocess test, not application code.
- Integration collection succeeded: **15 tests collected in 0.66s**; no PostgreSQL integration tests executed.
- Changed subprocess import string to `from database.config import APPLICATION_SCHEMA` (commit `095d15c`). New complete unit regression **pending**, do not claim 355 passed post-removal yet.
- User has **not** rerun root bootstrap/geospatial dry-run after package removal; Step 4/5 acceptance remains pending.
- Next audit must include string-based references, not just statements anchored to `^from` or `^import`.

## DB-R2 CLI contract refinement — three entrypoints (2026-10-16)

The user clarified the intended design: **`python -m database.climate`** independently initializes the five climate/operational tables; **`python -m database.geospatial`** independently initializes the three geospatial tables and required PostGIS extension; **`python -m database.bootstrap`** coordinates both, for a total of eight application tables. Each defaults to read-only dry-run, and requires `--execute` for DDL. The combined CLI calls the separate `initialize_climate_schema` and `initialize_geospatial_schema` in order; Airflow must continue using its climate-only path and must never bootstrap geospatial implicitly. Root `database.bootstrap.ensure_postgis_extension` stays independently callable for importer bootstrap. The combined bootstrap does **not** create databases, remove objects, or import GeoJSON.

- [x] Implement independent climate CLI and combined bootstrap coordinator on `dev`.
- [x] Update root and geospatial README CLI guidance.
- [x] Verify `--help` and dry-run output of all three CLIs after these changes.
- [x] Re-run complete unit suite (**355 passed in 7.74s**) after the implementation.
- [ ] Execute against the isolated test database for full initialization verification when DB-R3 provisioning is ready; do not test destructive operations on `weather`.

## DB-R2 three-CLI verification (user report, 2026-10-16)

- `python -m database.climate --help` and default `python -m database.climate` succeeded. Dry-run target `weather.climate_data`, listing exactly five climate/operational tables.
- `python -m database.geospatial --help` and default `python -m database.geospatial` succeeded. Dry-run lists exactly three spatial tables in `weather.climate_data`.
- `python -m database.bootstrap --help` and default `python -m database.bootstrap` succeeded. Dry-run reports five climate/operational + three geospatial tables and unchanged database.
- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` → **355 passed in 7.74s** after CLI changes and legacy-package removal.
- All six CLI commands were help/dry-run only. No `--execute` or live PostgreSQL integration results have been reported.

## DB-R2 Step 6 documentation synchronization (2026-10-17)

- [x] Root `AGENTS.md`: root `database/` owns connection/config/DDL/PostGIS; scientific loader/read repository, operational repository, and spatial importer responsibilities remain separate.
- [x] Root `README.md`: actual root database project tree, three explicitly guarded CLIs, five climate + three spatial tables, current PostGIS `climate_data` contract; removed obsolete description of missing importer and historical extension-in-public claim.
- [x] `tests/AGENTS.md` and `tests/README.md`: explicit isolated `TEST_POSTGRES_*`, canonical schema constraints, geospatial integration command, and collection-vs-execution distinction.
- [x] `orchestration/README.md`: climate-only application bootstrap versus independent spatial initialization, metadata remains SQLite.
- [x] `query/README.md`: canonical climate DDL source references moved to root `database/climate.py` and `database/schema.py`.
- [x] `geospatial/README.md`: previously updated to describe all three CLIs, PostGIS and importer, and test safety.
- [x] Verified no old `pipeline/database`, `geospatial/database`, `pipeline.database`, or `geospatial.database` references in the seven reviewed live instructional documents (geospatial README mentions PostGIS in `public` solely as a legacy-test-db warning).
- [ ] Final repo-wide documentation search, excluding explicitly historical migration notes, and subsequent local unit regression after documentation edits.
- [ ] Live isolated PostgreSQL integration tests and unified development/test provisioning remain DB-R3/DB-R5 work, **not** proved by documentation-only changes.

## Current evidence and boundaries

The user previously reported **19 passed** geospatial unit tests, **22 passed** destroy-all unit tests, successful PostGIS 3.4.2 bootstrap at `weather.climate_data`, creation of the three spatial tables, and one committed import of 23 Aceh districts/one province. These results predate this planned refactor and do not establish its regression success. The user has since **dropped `weather_test`**, while `weather` must remain untouched until an explicitly authorized maintenance operation. No source files or PostgreSQL databases have been changed by creating this plan.
