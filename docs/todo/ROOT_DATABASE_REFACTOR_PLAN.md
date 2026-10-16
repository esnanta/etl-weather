# Root Database Package Refactor Plan

**Status:** DB-R1 source audit partially completed; DB-R2 root package staged (not yet migrated into runtime). No live PostgreSQL or local pytest verification has been performed.  
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
- [ ] Move operational CREATE TABLE ownership out of orchestration repositories without moving operational business logic.
- [ ] Update imports across runtime, scripts, tests and CI; remove old `pipeline/database` and `geospatial/database` only after no callers remain.
- [ ] Keep code and docstrings in English; avoid stale compatibility aliases and duplicate bootstrap paths.

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

## Current evidence and boundaries

The user previously reported **19 passed** geospatial unit tests, **22 passed** destroy-all unit tests, successful PostGIS 3.4.2 bootstrap at `weather.climate_data`, creation of the three spatial tables, and one committed import of 23 Aceh districts/one province. These results predate this planned refactor and do not establish its regression success. The user has since **dropped `weather_test`**, while `weather` must remain untouched until an explicitly authorized maintenance operation. No source files or PostgreSQL databases have been changed by creating this plan.
