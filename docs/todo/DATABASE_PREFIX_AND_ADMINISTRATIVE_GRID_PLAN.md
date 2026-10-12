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
- No `adm_` prefix. Examples: `t_province`, `t_district`.
- New field names should be explicit: `province_name`, `district_name`; inspect genuinely problematic SQL identifiers before renaming existing columns.
- Database and application data will be recreated, **not migrated** from the old naming convention. Destructive execution requires separate controlled preflight on the intended development database; this plan does not run deletion.
- Pipeline and Airflow changes must be limited to database object names/references and absolutely necessary column-reference changes. Preserve scientific transformations, CDS requests, scheduling, deferral, retries, pacing, XCom, idempotency, and logging behavior.
- `/AGENTS.md` remains authoritative. It establishes single active CDS ERA5-Land path, immutable RAW, UTC+7 daily rules, independent pipeline logic, read-only repository behavior, and deterministic persistence.
- Change only branch `dev`; do not touch other branches.

## 2. Table naming decision pending confirmation

The user's new preference is a name such as `lat_long_region` because the records are Copernicus latitude/longitude locations.

**Recommendation: `grid_region`**, physical table `climate_data.t_grid_region`. It represents the grid-point-to-administration mapping more precisely than the generic `region`, and is not tied to a particular coordinate naming shorthand. Alternative: `t_lat_lon_region`. Do not implement this rename before the final naming review.

Proposed config entry (not implemented):

```python
GRID_REGION_TABLE = table_name("grid_region")
```

Proposed columns (subject to verification of climate coordinate types):

```text
grid_region_id (PK)
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
| Grid-to-region mapping | `climate_data.t_grid_region` (recommended, unapproved) |
| Daily partitions | `climate_data.t_daily_climate` |
| Daily observations | `climate_data.t_daily_climate_detail` |
| Monthly tables | `climate_data.t_monthly_climate` and `t_monthly_climate_detail` (verify actual definitions) |
| Progress | `climate_data.t_pipeline_progress` |
| Progress details | `climate_data.t_pipeline_progress_detail` |
| Submission pacing | `climate_data.t_submission_pacing` |
| Daily region view | `climate_data.vw_daily_climate_region` |
| Monthly region view | `climate_data.vw_monthly_climate_region` |

`t_province` and `t_district` should have explicit `province_id/province_name`, `district_id/district_name`, official codes nullable until reliable codes are available, provider identity/provenance, geometry in SRID 4326 and spatial indexes. `district` means kabupaten/kota in this provider, **not kecamatan**.

GeoJSON observations from earlier inspection: `Aceh.geojson` comprises Aceh kabupaten/kota records with `WADMPR`, `WADMKK`, `OBJECTID`; `Aceh_Barat.geojson` is a single kabupaten/kota feature. Neither establishes kecamatan-level coverage. Revalidate geometry CRS/validity and feature count when implementing. Do not treat `OBJECTID` as an official government code.

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

### P0 — Audit, freeze and baseline

- [x] Read `/AGENTS.md` on `dev` (2026-10-12).
- [x] Review actual root `/config.py` and `pipeline/database/config.py` status on `dev`.
- [x] Record prior prefix commits and their incomplete state above.
- [ ] Confirm naming: `grid_region` versus `lat_lon_region`; confirm `t_` versus `sk_` final value before any reset.
- [ ] Audit `/README.md`, `tests/AGENTS.md`, Airflow documentation, schema definitions, SQL references and scripts on `dev`.
- [ ] Record all existing table/view/column names and all call sites, including test fixtures.
- [ ] Audit actual monthly parent/detail schema and its coordinate-bearing table.
- [ ] Run and record baseline offline tests with scheduled campaign disabled.
- [ ] Freeze implementation scope: naming-only for existing pipeline/orchestration.

### P1 — Global naming refactor (only after P0 approval)

- [ ] Update central constants in root `config.py`, including `GRID_REGION_TABLE` if confirmed.
- [ ] Ensure all existing climate, progress, and pacing SQL uses centralized table identifiers.
- [ ] Update index, constraint, FK, reset/destroy/recreate and operational queries that reference physical table names.
- [ ] Review SQL keywords/identifier naming; rename only documented conflicts or newly designed fields, preserving scientific data contracts.
- [ ] Review `DatabaseConfig`, imports, fixture overrides, tests and documentation for consistent names.
- [ ] Do not change DAG topology, schedules, retry/pacing behavior, climate formulas, or loader semantics.
- [ ] Validate that the development reset path addresses only the intended database and does not unexpectedly delete immutable RAW artifacts.
- [ ] Test clean creation on isolated test database before any destructive operation on development.
- [ ] Run offline unit/orchestration tests and PostgreSQL integration tests; compare behavior with baseline.

### P2 — Administrative reference schema and importer

- [ ] Check PostGIS extension availability on isolated test database.
- [ ] Define/create `t_province`, `t_district`, `t_grid_region` (name pending), constraints, indexes, and provenance.
- [ ] Keep administration data loading outside Airflow and outside scientific transformation stages.
- [ ] Implement idempotent GeoJSON reader/importer; validate GeoJSON properties, geometry type, CRS (OGC:CRS84 input / EPSG:4326 storage), invalid polygon handling and duplicate identities.
- [ ] Import Aceh kabupaten/kota boundaries; reconcile expected counts and report anomalies.
- [ ] Prevent a partial one-kabupaten file load from deleting other imported administrative records.
- [ ] Include tests for re-import, invalid geometry, missing fields, CRS, transaction rollback, and unmatched features.

### P3 — Spatial mapping, query views and performance

- [ ] Extract unique ERA5-Land grid coordinates from canonical observation data, preserving coordinate precision.
- [ ] Use `ST_Covers` and account for 0/1/multiple matches, coastal/ocean points and boundaries; report unresolved cases.
- [ ] Enforce consistency between province and district.
- [ ] Persist one mapping per grid point and rerun mapping idempotently.
- [ ] Create `vw_daily_climate_region` and `vw_monthly_climate_region` from centrally prefixed identifiers; use actual detail tables for coordinates.
- [ ] Verify `LEFT JOIN` preserves every original climate observation, including unmapped points.
- [ ] Compare query execution plans and indexes; only then decide whether direct `grid_region_id` FKs on climate records are justified.
- [ ] Distinguish point-assignment statistics from area-weighted administrative statistics in documentation.

### P4 — Final validation and documentation

- [ ] Repeat comprehensive unit and integration suites, including idempotency, reconciliation, pacing, database read-only access and Airflow DAG parsing.
- [ ] Demonstrate one standalone pipeline run and one scheduled pilot with no scientific or scheduling behavior changes (when the environment is ready).
- [ ] Update `/AGENTS.md`, `/README.md`, `tests/AGENTS.md`, and relevant database/Airflow docs to reflect canonical prefixed object names without duplicated explanations.
- [ ] Record actual run output, performance, remaining caveats and final checklist status.
- [ ] Require explicit sign-off before closing this todo.

## 6. Stop conditions

Stop implementation and return to design review if: the proposed schema introduces a second application schema; any scientific or Airflow scheduling behavior must change; polygon data does not match the assumed administrative level; ambiguous spatial assignment is silently resolved; destructive target cannot be confirmed; or automated integration tests indicate regressions.

**Current authorization:** documentation and planning only. No further code edits or database/data operations.
