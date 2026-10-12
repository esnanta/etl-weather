# TODO — Global Database Prefix and Administrative Grid Mapping

Status: **PLANNING / IMPLEMENTATION PAUSED**
Date: 2026-10-12
Branch: **dev only**
Repository: `esnanta/etl-weather`

> This is a plan and checklist, not approval to refactor code, reset a database, or load GeoJSON.
> No production code, SQL schema, Airflow DAG, or data should be modified until the plan is reviewed.

## 1. Scope and constraints

- One PostgreSQL application database: `weather` (configured through `POSTGRES_DATABASE`).
- Exactly one application schema: `climate_data`. Do **not** recreate a separate `climate` schema.
- Global table prefix from root `/config.py`: `TABLE_PREFIX = "t_"` (changeable to `"sk_"` only as a separately approved naming decision).
- Global view prefix: `VIEW_PREFIX = "vw_"`.
- **Planned standalone period rename:** replace `CDS_START_DATE` / `CDS_END_DATE` with provider-neutral `PIPELINE_START_DATE` / `PIPELINE_END_DATE` in configuration, environment examples, standalone entrypoints, tests and documentation. This is a naming-only change; do not change request date semantics or Airflow campaign years.
- No `adm_` prefix. Examples: `t_province`, `t_district`.
- New field names should be explicit: `province_name`, `district_name`; inspect genuinely problematic SQL identifiers before renaming existing columns.
- Database and application data will be recreated, **not migrated** from the old naming convention. Destructive execution requires separate controlled preflight on the intended development database; this plan does not run deletion.
- Pipeline and Airflow changes must be limited to database object names/references and absolutely necessary column-reference changes. Preserve scientific transformations, CDS requests, scheduling enable behavior, interval cadence, deferral, retries, pacing, XCom, idempotency, and logging behavior.
- `/AGENTS.md` remains authoritative. It establishes single active CDS ERA5-Land path, immutable RAW, UTC+7 daily rules, independent pipeline logic, read-only repository behavior, and deterministic persistence.
- Change only branch `dev`; do not touch other branches.
- **Strict code boundary:** inside `/pipeline` and `/orchestration`, only edit physical database table/view naming and directly required SQL references (including index/constraint/FK names or SQL column identifiers verified as conflicting). No alterations to control flow, ETL stages, CDS requests, transformations, Airflow DAG graphs, task dependencies, schedules, retry, deferrals, pacing, or request construction. If a non-naming modification appears necessary, stop and request a new design decision first.

## 2. Confirmed administrative table naming and key convention

**Approved:** the grid-point-to-administrative-area mapping table is `climate_data.t_grid_region`. Do not use `t_region` or `t_lat_lon_region`. All new administrative tables use a uniform primary key column named `id`; relationship columns remain explicit foreign keys (`province_id`, `district_id`). GeoJSON source feature identifiers use `source_feature_ref`, not `source_object_id`, and are **not** foreign keys. This convention applies to `t_province`, `t_district`, and `t_grid_region`. Existing canonical climate table primary keys remain `id`; existing FK contracts such as `daily_climate_id` are preserved.

Approved config entry (not yet implemented):

```python
GRID_REGION_TABLE = table_name("grid_region")
```

Proposed columns (subject to verification of climate coordinate types):

```text
id (PK)
latitude
longitude
province_id (nullable FK)
district_id (nullable FK)
created_at
updated_at
UNIQUE(latitude, longitude)  # revisit if multiple distinct grid definitions are supported
```

A point with no region match remains nullable; ambiguous matches must be reported and not silently assigned. A district's province must agree with `province_id`; enforce at DB level. For fixed ERA5-Land grid points, classify via PostGIS point-in-polygon only once, then use indexed relational joins.

## 3. Target objects (proposals, not existing-object inventory)

| Category | Target |
| --- | --- |
| Province | `climate_data.t_province` |
| Kabupaten/kota | `climate_data.t_district` |
| Grid-to-region mapping | `climate_data.t_grid_region` (approved) |
| Daily partitions | `climate_data.t_daily_climate` |
| Daily observations | `climate_data.t_daily_climate_detail` |
| Monthly tables | `climate_data.t_monthly_climate` and `t_monthly_climate_detail` (verify actual definitions) |
| Progress | `climate_data.t_pipeline_progress` |
| Progress details | `climate_data.t_pipeline_progress_detail` |
| Submission pacing | `climate_data.t_submission_pacing` |
| Daily region view | `climate_data.vw_daily_climate_region` |
| Monthly region view | `climate_data.vw_monthly_climate_region` |

`t_province` and `t_district` use `id` as primary key, explicit `province_name` and `district_name`, and `province_id` only as a foreign key in `t_district`. Official codes remain nullable until reliable codes are available; store the provider feature identity under `source_feature_ref` (not a database FK or official administrative code), with source dataset/version context, geometry in SRID 4326 and spatial indexes. For dissolved province geometry, `source_feature_ref` may be NULL or omitted because a single upstream feature identifier may not exist. `district` means kabupaten/kota in this provider, **not kecamatan**.

**Repository source of truth for district boundary import:** `/indonesia/districts/Aceh.geojson` (verified as a path on branch `dev` on 2026-10-12). This is the input to the future independent administrative loader, not a source for the existing climate ETL or Airflow DAG. The current repository copy has not yet undergone feature-count, property, CRS, and geometry-validity inspection; validate it during P4. The previously uploaded files are background references, not the import path.

GeoJSON observations from earlier inspection: `Aceh.geojson` comprises Aceh kabupaten/kota records with `WADMPR`, `WADMKK`, `OBJECTID`; `Aceh_Barat.geojson` is a single kabupaten/kota feature. Neither establishes kecamatan-level coverage. Revalidate geometry CRS/validity and feature count when implementing. Map GeoJSON `OBJECTID` to a normalized string `source_feature_ref` for district imports, keeping the associated source dataset/version; do not treat `OBJECTID` as an official government code or a foreign key.

No direct administrative FK is required on canonical daily/monthly records initially; first test views joining the normalized grid mapping using consistent coordinate types. Do not assume the coordinate-bearing records are the partition-parent tables: current `pipeline/database/schema.py` places latitude and longitude in `daily_climate_detail`. Review monthly schema accordingly.

## 4. Changes already made before this planning pause

**These are historical changes already pushed to `dev`, not new tasks completed by this document:**

- Commit `a9047ec`: root `config.py` gained `TABLE_PREFIX = "t_"`, `VIEW_PREFIX = "vw_"`, `table_name()`, `view_name()`, and derived table/view constants. Existing progress/pacing names now derive from `table_name()`.
- Commit `61e0300`: `pipeline/database/config.py` now imports global `DAILY_CLIMATE_TABLE` and `DAILY_CLIMATE_DETAIL_TABLE` instead of defining unprefixed literals.
- **Incomplete:** root `config.py` still contains `REGION_TABLE = table_name("region")`; it has not yet been changed to the newly proposed `GRID_REGION_TABLE`.
- **Not verified:** other hard-coded SQL references, constraints/index names, operational scripts, tests, documentation, monthly schema and Airflow persistence remain unaudited as a complete set; no full regression/integration pass has been reported for these two commits.
- **No data reset, GeoJSON import, PostGIS creation, or administrative table creation was performed.**

Root `/config.py` (not `scripts/config.py`) is the project's actual shared lightweight configuration module according to `README.md`.

## 5. Checklist — complete in order, after plan approval

## 5A. P0 read-only audit findings (2026-10-12; implementation not started)

**Verified on the `dev` branch by reading repository files:**

| Area | File(s) / current behavior | P1 / test implication |
| --- | --- | --- |
| Project boundary | `/AGENTS.md` | No changes to CDS, scientific conversion, RAW, idempotent persistence, or orchestration flow. |
| Shared naming | `/config.py` | Already includes `TABLE_PREFIX="t_"`, `VIEW_PREFIX="vw_"` and derived constants; obsolete `REGION_TABLE` still exists. |
| Climate DB config | `pipeline/database/config.py` | Daily table names imported from root config; schema `climate_data`. |
| Actual daily schema | `pipeline/database/schema.py` | `daily_climate` stores partition metadata; `daily_climate_detail` stores dated grid-point observations, including latitude/longitude. Keep `daily_climate_id` FK. |
| Daily load/read | `pipeline/loaders/daily_climate_postgres.py`, `pipeline/repositories/daily_climate_reader.py` | References use configured `daily_climate_table`/detail names; inspect all SQL call sites and test overrides. |
| Progress/pacing | `orchestration/repositories/pipeline_progress.py`, `submission_pacing.py`, `load_evidence.py` | Table names are imported from shared constants; keep progress and pacing semantics exactly unchanged. |
| Database operations | `scripts/destroy_all.py` | `EXPECTED_APPLICATION_TABLES` still contains unprefixed daily names; adjust inventory expectation during P1. Do **not** run destructive scripts during P0. |
| Standalone period | `config.py`, `pipeline/requests/factory.py`, `env-example` | Old `CDS_START_DATE` and `CDS_END_DATE` still active; plan rename to `PIPELINE_START_DATE`/`PIPELINE_END_DATE` without altering inclusive range logic. |
| Actual terminal entrypoint | `scripts/run_pipeline_cds.py` | Use `python -m scripts.run_pipeline_cds` for P2; old documentation examples using `scripts.run_pipeline` must be corrected. |
| Monthly scope | `pipeline/database/schema.py`, `pipeline/runtime.py` | No monthly database schema creator found in database module. `run_monthly_batch` means monthly partition execution, **not proof that monthly climate statistics are persisted**. Treat monthly-view work as blocked pending confirmed schema. |
| Schedule flag | `orchestration/scheduling/schedule.py` | Retain flag-controlled `schedule=None` versus fixed interval. |
| Boundary dataset | `indonesia/districts/Aceh.geojson` | File exists on dev; contents/geometry were not fully validated in P0. |

**Important caveat:** This is a *source-code audit*, not a completed P0 pass. No runtime tests were run, no PostgreSQL database was contacted, no dataset was downloaded, and file-by-file inventory remains incomplete. Prior commits `a9047ec`/`61e0300` remain partially integrated.

**P0 continuation before P1:** inventory all `pipeline/`, `orchestration/`, `scripts/`, `query/`, `tests/` references, especially raw SQL and reset safety; record exact current pytest results on local checkout; confirm application prefix is `t_`. Only then check P0 complete.

### P0 test log (12 October 2026)

User baseline: `tests/unit -q` -> 315 passed, 7 failed, 5 errors; `tests/integration -q` -> 7 passed, 3 failed. P0 is not green.

- Five DAG fixture errors and one schedule test failure: expected `cds_era5_land_etl`, detected `cds-era5-land`. The tracked DAG source still defines `cds_era5_land_etl`. Investigate local DAG discovery without modifying the DAG.
- Six pipeline unit failures: assertions still reference unprefixed daily table names; configured SQL now uses `t_daily_climate` and `t_daily_climate_detail`.
- Three integration failures: `tests/integration/orchestration/test_pipeline_progress_reconciliation.py` uses literal unprefixed daily table names, leading to PostgreSQL UndefinedTable and cleanup failures.
- Investigation-only during P0. Fix stale table assertions and SQL in P1; retain logic, DAG topology, and schedule behavior.
- [ ] Determine source of unexpected DAG ID and why startup log reports schedule enabled true while test invocation sets false.
- [ ] Rerun both suites after approved P1 naming fixes.

### Verified passing local test baseline — 2026-10-12

User-reported results on the local `dev` checkout:

- `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q`: **327 passed in 7.54s**.
- `python -m pytest tests/integration -q`: **10 passed in 1.53s**.
- Activate with `source activate-airflow.sh -q` for all future command examples.

**Test quality is a separate acceptance gate from a green test count.** Read and enforce `/tests/AGENTS.md` before changes: test the real production implementation, do not duplicate or recreate the algorithm or SQL behavior under test, avoid mocks of the subject under test, use production config for config-contract tests, and keep minimal explicit fixtures for isolated logic. PostgreSQL transactional/idempotency guarantees require real isolated `TEST_POSTGRES_*` integration tests. Include meaningful negative cases, schema/FK and DAG invariants, and protect unchanged scientific and orchestration behavior. A green suite confirms only the contracts actually asserted; P0 inventory and behavioral test quality review remain separate pending work.

### P0 test coverage and quality audit — 2026-10-12 (read-only)

Reviewed `tests/AGENTS.md` plus representative current test modules: `tests/unit/pipeline/test_database.py`, `test_load.py`, `test_daily_climate_repository.py`, `tests/integration/pipeline/test_postgresql_idempotency.py`, `tests/integration/orchestration/test_pipeline_progress_reconciliation.py`, `tests/unit/orchestration/test_dag.py`, `test_schedule.py`, and `tests/unit/operations/test_recreate_postgres_database.py`.

**Strengths confirmed in source:** schema unit tests invoke production DDL builders and inspect resulting SQL; loader unit tests exercise production preparation and UPSERT construction with an isolated SQL recording boundary; read-repository tests cover invalid selection, empty/missing/duplicate observations and generated query parameters; PostgreSQL integration exercises real persistence, idempotency and transaction rollback; DAG policy tests parse the actual DAG and assert task graph, retries and pools; maintenance tests assert safety guards and no-execution outcomes.

**Limitations and outstanding checks:**
- Capturing an emitted SQL string is an appropriate unit assertion, but does not prove that a PostgreSQL constraint, foreign key or index actually works. Ensure specific negative-case integration coverage before accepting P1.
- Current tests often assert concrete `t_*` strings. Audit whether central `TABLE_PREFIX` configuration and all consumers remain consistent without rewriting production behavior in the test.
- `tests/AGENTS.md` still documents unprefixed `climate_data.daily_climate`, `daily_climate_detail`, `pipeline_progress`, `pipeline_progress_detail`, and `submission_pacing`; update these *documentation contracts* during P1 without altering the testing principles.
- A fully passing `pytest -q` result does not itself quantify mutation sensitivity or exhaustive code coverage; review negative assertions and real-DB integration around changed identifiers and operational safety.
- This is a representative module review, not an exhaustive audit of all test cases. Do not claim full coverage or a verified production run.

**Implementation restriction:** P0 records observations only. Any strengthened tests or updated testing documentation should be scoped to the approved P1 database naming/configuration change; do not modify scientific algorithm or DAG control flow.

### P0 — Audit, freeze and baseline

- [x] Read `/AGENTS.md` on `dev` (2026-10-12).
- [x] Review actual root `/config.py` and `pipeline/database/config.py` status on `dev`.
- [x] Record prior prefix commits and their incomplete state above.
- [x] Confirm grid mapping table name: `t_grid_region` and primary key `id` on all new administrative tables.
- [x] Confirm GeoJSON provenance column `source_feature_ref` instead of `source_object_id` (non-FK, not an official area code).
- [x] Confirm the repository source file path `/indonesia/districts/Aceh.geojson` exists on `dev` (content validation pending P4).
- [ ] Confirm `t_` versus `sk_` final global table prefix before any reset (currently `t_` in config).
- [ ] Finish exhaustive reference audit of `/README.md`, `tests/AGENTS.md`, Airflow documentation, all SQL scripts, and tests on `dev` (core database definitions and several operational modules inspected; not exhaustive).
- [ ] Record all existing table/view/column names and all call sites, including test fixtures (partial inventory below).
- [x] Locate primary standalone date readers: root `config.py` defines `CDS_START_DATE_ENV`/`CDS_END_DATE_ENV`, `resolve_cds_period()`; `pipeline/requests/factory.py` calls the resolver; `env-example` contains both keys; actual entrypoint is `scripts/run_pipeline_cds.py`. Full references across tests/docs still pending.
- [x] Audit current database module for monthly tables: no `create_monthly_climate*` function in `pipeline/database/schema.py`; `pipeline/runtime.py` calls `run_monthly_batch` for monthly *partitioning*, not monthly-statistic database persistence. A separate future monthly schema is **not yet verified/implemented**. Recheck full codebase before any monthly view.
- [x] Record the passing user-executed baseline: unit 327 passed (7.54s), integration 10 passed (1.53s); no assistant-side test execution.
- [ ] Verify all references to `ERA5_CAMPAIGN_SCHEDULE_ENABLED` and `ENABLED_ENV` stay intact in root config, scheduling module, env-example, startup scripts, reset tooling, docs and tests.
- [ ] Freeze implementation scope: naming-only for existing pipeline/orchestration.
- [x] Re-read `/tests/AGENTS.md`; apply its test construction and integration-boundary requirements.
- [x] Review representative unit/integration coverage quality and boundaries; results documented above.
- [ ] Complete targeted verification that real PostgreSQL negative cases and prefix consumers cover the naming changes before closing P0.

### P1 — Global naming refactor (only after P0 approval)

- [ ] Update central constants in root `config.py` to use the approved `GRID_REGION_TABLE = table_name("grid_region")`; remove obsolete `REGION_TABLE`.
- [ ] Ensure all existing climate, progress, and pacing SQL uses centralized table identifiers, **without changing any ETL or Airflow execution/control flow**.
- [ ] Update index, constraint, FK, reset/destroy/recreate and operational queries that reference physical table names.
- [ ] Review SQL keywords/identifier naming; rename only documented conflicts or newly designed fields, preserving scientific data contracts.
- [ ] Review `DatabaseConfig`, imports, fixture overrides, tests and documentation for consistent names.
- [ ] Rename standalone period settings to `PIPELINE_START_DATE` and `PIPELINE_END_DATE`; update root `config.py` env-key definitions and resolver names as appropriate, all read sites, tests, `env-example` and documentation. Preserve the inclusive period, UTC+7 daily boundaries, and existing standalone behavior. Remove obsolete `CDS_*DATE` aliases only once all references are updated; do not add fallback compatibility for discarded development config.
- [ ] Do not change DAG topology, schedule enable switch, schedule interval, retry/pacing behavior, climate formulas, loader semantics, or any control flow inside `/pipeline` and `/orchestration`. Stop and report if a non-database-naming change seems required.
- [ ] Validate that the development reset path addresses only the intended database and does not unexpectedly delete immutable RAW artifacts.
- [ ] Test clean creation on isolated test database before any destructive operation on development.
- [ ] Run offline unit/orchestration tests and PostgreSQL integration tests; compare behavior with baseline.

### P2 — Standalone three-day CDS smoke test (mandatory gate before administrative implementation)

- [ ] Keep Airflow scheduled DAG paused during the standalone test; no Airflow tasks should participate.
- [ ] Use `PIPELINE_START_DATE=2024-05-01` and `PIPELINE_END_DATE=2024-05-03` as an illustrative three-local-day period (finalize available dates at execution).
- [ ] Use the active `ACEH_BBOX_COORDINATES` and the five existing CDS variables without changing any scientific request/transformation behavior.
- [ ] After isolated database recreation, run the existing standalone entrypoint and verify retrieval, RAW NetCDF validation, transformed Parquet and successful PostgreSQL writes.
- [ ] Verify prefixed parent/detail tables (`t_daily_climate`, `t_daily_climate_detail`), FK integrity, correct request partition and `valid_time_local` boundaries.
- [ ] Verify expected 3 × 1,505 = 4,515 day/grid coordinate combinations **where the grid is complete**; account for missing meteorological values separately and never reinterpret missing as zero.
- [ ] Rerun the same standalone period and confirm idempotency, stable observation count and deterministic artifact/checkpoint behavior.
- [ ] Record the exact CLI, database count queries, timings, artifacts, observed warnings and all test evidence.
- [ ] **Gate:** do not begin administrative tables/importer until three-day download → transformation → load and rerun-idempotency succeed.

### P3 — Airflow schedule contract regression (no configuration removal)

- [ ] **Retain** `ERA5_CAMPAIGN_SCHEDULE_ENABLED` and root config `ENABLED_ENV` as-is; P3 is verification-only, not an Airflow scheduling refactor.
- [ ] Verify `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` keeps the DAG manually triggerable without automatic scheduling (`schedule=None`).
- [ ] Verify `ERA5_CAMPAIGN_SCHEDULE_ENABLED=true` preserves the current fixed campaign scheduling interval, bounded by `CAMPAIGN_START_YEAR` and `CAMPAIGN_END_YEAR`.
- [ ] Keep terminal standalone `PIPELINE_START_DATE` and `PIPELINE_END_DATE` behavior separate from Airflow campaign settings.
- [ ] Verify scheduled/manual DagRun handling and existing pause/unpause, reset, pacing, retry, and idempotency tests.
- [ ] Do not change `orchestration/scheduling/schedule.py` or remove related environment/documentation entries except to fix a proven regression introduced by table prefix naming.

### P4 — Administrative reference schema and importer

- [ ] Check PostGIS extension availability on isolated test database.
- [ ] Define/create `t_province`, `t_district`, `t_grid_region` (approved), constraints, indexes, and provenance; use `source_feature_ref` in source-derived records, never `source_object_id`.
- [ ] Document and test `OBJECTID` → `source_feature_ref` conversion (e.g. numeric 113.0 to canonical text `"113"`), source dataset/version scope, null handling for dissolved provinces, and idempotent conflict identity.
- [ ] Use `/indonesia/districts/Aceh.geojson` as the authoritative import input. Do not rely on a separately uploaded one-district sample.
- [ ] Inspect the repository GeoJSON for feature count, attributes (`OBJECTID`, `WADMPR`, `WADMKK`), actual CRS, geometry dimensions/type and validity before assuming the prior uploaded-file profile applies.
- [ ] Keep administration data loading in a separate component outside Airflow and outside scientific transformation stages; do not add an administrative task to the existing DAG.
- [ ] Implement idempotent GeoJSON reader/importer; validate GeoJSON properties (`OBJECTID`, `WADMKK`, `WADMPR`), geometry type, CRS (OGC:CRS84 input / EPSG:4326 storage), invalid polygon handling and duplicate identities. Report missing or duplicate `source_feature_ref` values within a source dataset.
- [ ] Import Aceh kabupaten/kota boundaries from `/indonesia/districts/Aceh.geojson`; reconcile actual feature counts and report anomalies.
- [ ] Prevent a partial one-kabupaten file load from deleting other imported administrative records.
- [ ] Include tests for re-import, invalid geometry, missing fields, CRS, transaction rollback, and unmatched features.

### P5 — Spatial mapping, query views and performance

- [ ] Extract unique ERA5-Land grid coordinates from canonical observation data, preserving coordinate precision.
- [ ] Use `ST_Covers` and account for 0/1/multiple matches, coastal/ocean points and boundaries; report unresolved cases.
- [ ] Enforce consistency between province and district.
- [ ] Persist one mapping per grid point and rerun mapping idempotently.
- [ ] Create `vw_daily_climate_region` and `vw_monthly_climate_region` from centrally prefixed identifiers; use actual detail tables for coordinates.
- [ ] Verify `LEFT JOIN` preserves every original climate observation, including unmapped points.
- [ ] Compare query execution plans and indexes; only then decide whether a direct FK referencing `t_grid_region.id` on climate records is justified; do not add one initially.
- [ ] Distinguish point-assignment statistics from area-weighted administrative statistics in documentation.

### P6 — Final validation and documentation

- [ ] Repeat comprehensive unit and integration suites, including idempotency, reconciliation, pacing, database read-only access and Airflow DAG parsing.
- [ ] Preserve recorded standalone three-day smoke-test evidence and demonstrate one controlled scheduled pilot with no scientific, pacing or retry-behavior change (when the environment is ready).
- [ ] Update `/AGENTS.md`, `/README.md`, `tests/AGENTS.md`, and relevant database/Airflow docs to reflect canonical prefixed object names without duplicated explanations.
- [ ] Record actual run output, performance, remaining caveats and final checklist status.
- [ ] Require explicit sign-off before closing this todo.

## 6. Stage-by-stage delivery and acceptance

| Stage | Work | Acceptance evidence |
| --- | --- | --- |
| P0 — Audit | Inventory table/view/column references and baseline tests; confirm one schema and naming scope. | Reviewed impacted-file inventory and baseline results. |
| P1 — Prefix and standalone config names | Centralize `t_` and `vw_` names, rename terminal date settings to `PIPELINE_START_DATE` / `PIPELINE_END_DATE` without logic changes; change only database object references in existing pipeline/orchestration; recreate empty dev database safely. | Database tables and schema references correct; SQL/unit/integration tests pass. |
| P2 — Standalone pilot | Run exactly three local days via `PIPELINE_START_DATE`/`PIPELINE_END_DATE`; validate RAW → Parquet → PostgreSQL and rerun. | Correct observations in prefixed tables, complete grid/day contract, no duplicates after rerun. |
| P3 — Airflow checks | Verify schedule-flag `true`/`false`, manual/scheduled modes, progress/pacing naming, with no DAG flow edits. | DAG parsing, scheduling and persistence tests pass; no behavior drift. |
| P4 — GeoJSON import | Load `indonesia/districts/Aceh.geojson` using a separate, idempotent administrative loader into province/district polygons. | Source properties/geometry validated, rows loaded and re-import safe. |
| P5 — Grid association | One-time grid-to-polygon mapping and read-only prefixed daily/monthly views. | Match/unmatched/ambiguous report; views preserve rows and permit district/province queries. |
| P6 — Final verification | Regression suites, documentation and signed checklist. | Recorded test evidence and consistent docs. |

## 7. Stop conditions

Stop implementation and return to design review if: changes to `/pipeline` or `/orchestration` require any program-flow modification (not solely database identifiers); the proposed schema introduces a second application schema; any scientific or Airflow scheduling behavior must change; polygon data does not match the assumed administrative level; ambiguous spatial assignment is silently resolved; destructive target cannot be confirmed; or automated integration tests indicate regressions.

**Current authorization:** documentation and planning only. No further code edits or database/data operations. The Airflow schedule enable flag remains part of the existing contract; do not replace it with unconditional scheduling.
