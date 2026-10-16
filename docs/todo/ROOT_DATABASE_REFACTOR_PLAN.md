# Root Database Package Refactor Plan

**Status:** Approved architecture; planning only — implementation not started.  
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
- [ ] Introduce the approved flat `/database` package and centralize configuration/connection utilities.
- [ ] Relocate canonical weather DDL, geospatial DDL and PostGIS bootstrap into the target modules while preserving definitions.
- [ ] Establish one `database/schema.py` composition entrypoint for **all eight currently identified** application/operational/spatial tables, including correct FK order and indexes.
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

## Current evidence and boundaries

The user previously reported **19 passed** geospatial unit tests, **22 passed** destroy-all unit tests, successful PostGIS 3.4.2 bootstrap at `weather.climate_data`, creation of the three spatial tables, and one committed import of 23 Aceh districts/one province. These results predate this planned refactor and do not establish its regression success. The user has since **dropped `weather_test`**, while `weather` must remain untouched until an explicitly authorized maintenance operation. No source files or PostgreSQL databases have been changed by creating this plan.
