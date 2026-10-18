## P6 final regression operator evidence (2026-10-18)

Operator ran `source activate-airflow.sh -q` and `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q`: **377 passed in 8.10s**. The full `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration -q` suite: **17 passed in 2.40s**. Total **394 passing tests** across these two distinct suites; no failures or skips appeared in the reported output. These runs confirm the current unit/integration regression gate only; no full operations-suite result, live scheduled CDS pilot, monthly science loader, or six-query SQL acceptance is implied.

- [x] Full unit tests: 377 PASS.
- [x] Full integration tests: 17 PASS.
- [ ] Review and accept the six read-only PostgreSQL queries using the separate query-audit checklist.
- [ ] Reconcile root monthly pipeline documentation with actual implementation before final P6 acceptance.
- [ ] Evaluate any remaining operations-suite checks under non-destructive conditions.

## P6 documentation audit and final regression gate (2026-10-18)

P6 started with source inspection of root README, AGENTS, test README, orchestration README, geospatial README, query acceptance TODO and current `pipeline/runtime.py`. Airflow DAG ID and dynamic PostgreSQL prefix semantics had already been corrected during P3. P6 corrected stale bootstrap counts in root/geospatial READMEs (seven climate/operational + three geospatial tables, two region views), and replaced a stale geospatial claim that monthly tables did not exist. **Important drift:** root README describes working monthly statistics processing and persistence, but active `pipeline/runtime.py` runs daily ETL in calendar-month partitions, and currently verified DB evidence shows empty monthly tables and empty monthly view. The README has an explicit warning; full reconciliation of proposed scientific monthly workflow with actual modules requires separate review and must not be represented as a working monthly pipeline. No ETL or Airflow code was edited.

- [x] Review key architecture/operations documents and resolve known bootstrap count drift.
- [x] Preserve prefix configurability in documentation; physical names shown only as current examples.
- [x] Flag root monthly workflow implementation/documentation inconsistency for explicit follow-up.
- [ ] Operator run `source activate-airflow.sh -q` then `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q` and report output.
- [ ] Operator run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration -q` against isolated `TEST_POSTGRES_*`; report skips and failures, if any.
- [ ] Operator run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/operations -q` **only if** that directory exists and tests do not perform destructive operations without explicit fixtures.
- [ ] Verify six-query PostgreSQL catalog independently using `docs/todo/POSTGRESQL_QUERY_AUDIT_PLAN.md`; do not mark query acceptance until actual read-only results reported.
- [ ] Resolve monthly workflow documentation drift and final P6 acceptance after regressions; do not run live CDS, Airflow scheduled campaign, or reset database.

## P3 accepted — Airflow scheduling contract verification (2026-10-18)

**P3 code/runtime contract ACCEPTED; operational scheduled pilot explicitly NOT performed.** Operator evidence: orchestration unit suite 122 PASS (5.37s), isolated PostgreSQL orchestration + pipeline idempotency suite 11 PASS (1.70s), Airflow DAG `cds-era5-land` registered from `orchestration/dags/cds_era5_land_etl.py` with `is_paused=True`, import-error list empty, standalone/scheduler/DAG-processor/triggerer processes active, `cds_api` pool with exactly 1 slot, and current metadata `airflow dags list-runs cds-era5-land` returned `No data found`. The schedule flag true/false behavior, serial max-active-run, manual/scheduled selection, monthly offsets and retry/pacing boundaries are covered by the existing code audit and passing unit/integration tests; do not infer a successful live DagRun. Campaign keys and years remain intentionally mutable configuration values and are not P3 acceptance constraints.

The Airflow README was aligned with the actual DAG ID, existing physical filename, seven climate/operational and three spatial tables, two region views, and centralized `database/config.py` prefix-driven PostgreSQL identifiers. No DAG, pipeline, schema, scheduling, database data, or metadata writes were performed by the assistant for this acceptance. Scheduled pilot (requiring unpause and potential CDS requests) is a separately authorized future operational gate, not a requirement for P3 contract-only sign-off.

- [x] Verify orchestration unit and isolated PostgreSQL integration suites.
- [x] Verify live Airflow DAG discovery, import errors, runtime component processes, one-slot `cds_api` pool and current DagRun state.
- [x] Align Airflow documentation with current identifiers and flexible database prefixes.
- [x] **Close P3 scheduling contract verification within the non-executing scope.**
- [ ] P6 cross-suite regression, SQL/query acceptance and final documentation audit.
- [ ] Separately plan a controlled scheduled pilot before claiming end-to-end operational scheduler/CDS acceptance.

## P3 initial unit/integration acceptance and metadata discovery gate (2026-10-18)

Operator executed (explicit subprocess environment `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false`) `tests/unit/orchestration -q`: **122 passed (5.37s)**; `tests/integration/orchestration` plus `tests/integration/pipeline/test_postgresql_idempotency.py -q`: **11 passed (1.70s)**. Total **133 distinct tests passed**. Operator also ran `airflow dags list`, accepted first-time SQLite metadata database initialization, and received `No data found`; `airflow dags list-import-errors` also showed `No data found`. Neither list output yet establishes successful DAG processor registration. The first-time DB initialization is not an application data reset. Next gate: verify configured dags_folder, run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false airflow dag-processor` only, and recheck CLI list/import errors while no scheduler is running. Do not trigger/unpause DAG or issue CDS requests. Campaign key/year values are mutable configuration, and the operator explicitly instructed not to flag their current numerical/string mismatch as a P3 issue.

- [x] Run P3 unit orchestration tests: 122 PASS.
- [x] Run P3 orchestration/PostgreSQL idempotency integration tests: 11 PASS.
- [x] Record fresh SQLite metadata initialization and currently empty DAG list.
- [ ] Confirm configured dags_folder and DAG processor discovery in metadata; then compare real DAG ID with source.
- [ ] Verify read-only schedule flag true/false registration, before considering scheduled pilot.

## P3 Airflow scheduling verification — code audit checkpoint (2026-10-18)

Read-only audit of current `dev` sources: `orchestration/dags/cds_era5_land_etl.py`, `orchestration/scheduling/schedule.py`, `orchestration/tasks/build_monthly_requests.py`, `tests/unit/orchestration/test_dag.py`, `test_schedule.py` and `orchestration/README.md`. The production DAG and existing DAG fixture identify `DAG_ID = "cds-era5-land"`, while Airflow README still documents `cds_era5_land_etl`; do not rename active DAG without a separate migration decision. Flag false yields `schedule=None`, true yields ten-minute timedelta, invalid flag values raise. DAG policy: Jakarta zone, catchup false, max active runs 1, max active tasks 2; scheduled earliest monthly slots +0/+15/+30 minutes, manual slots None, and mapped depth-first monthly tasks preserve their retry/pool policies. No scheduler/CDS execution was initiated during audit.

Observed earlier operator activation output printed `ERA5_CAMPAIGN_SCHEDULE_ENABLED=true` despite a separately scoped `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest ...`: the activation log alone does not prove the environment of the later subprocess. The campaign key ends `2016_2025` while logged `CAMPAIGN_END_YEAR=2016`; clarify intended pilot scope before scheduling any live run. Airflow README still has stale unprefixed table names and bootstrap counts and must be harmonized after tests, without changing behavior.

- [x] Audit core DAG schedule flag, run-mode entrypoint, mapped slots, and existing unit tests on `dev`.
- [ ] Operator run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/orchestration -q` and report output.
- [ ] Operator run isolated `tests/integration/orchestration` and `tests/integration/pipeline/test_postgresql_idempotency.py` with `TEST_POSTGRES_*`.
- [ ] Operator perform read-only Airflow DAG parsing/list commands with both schedule flag values; confirm actual DAG ID, avoid triggering any DagRun.
- [ ] Resolve documentation ID/table/count inconsistencies after confirming the installed runtime; no DAG ID or behavior changes authorized.
- [ ] Decide whether a controlled scheduled pilot is needed once current metadata state and campaign year settings are inspected.

## Monthly schema bootstrap acceptance — operator evidence (2026-10-18)

Operator reported `source activate-airflow.sh -q` followed by **30 passed** geospatial unit tests (0.61s) and **6 passed** isolated geospatial integration tests (1.02s). `python -m database.bootstrap --execute` succeeded on `weather` with seven climate/operational tables, three spatial tables and two region views; PostgreSQL PostGIS reported version **3.4.2**. Read-only screenshots confirm `t_monthly_climate` and `t_monthly_climate_detail` exist; `SELECT COUNT(*) FROM climate_data.vw_monthly_climate_region` returns **0**; `SELECT COUNT(*) FROM climate_data.vw_daily_climate_region` still returns **1,590**. Monthly schema/bootstrap presence and daily read-regression are accepted. Monthly data ingestion/aggregation and populated-monthly-view correctness remain intentionally unimplemented/untested. Existing geospatial integration suite does not itself establish real PostgreSQL monthly fixture coverage.

- [x] Operator local geospatial unit suite: 30 passed.
- [x] Operator geospatial PostgreSQL integration suite: 6 passed.
- [x] Operator live PostgreSQL bootstrap: successful; both monthly tables exist.
- [x] Operator empty monthly view: query succeeds and returns zero rows.
- [x] Operator daily view unchanged: 1,590 rows.
- [ ] Separate scientific monthly aggregation contract and loader acceptance, if later requested.
- [ ] Continue deferred P3 Airflow schedule-contract verification.

## Monthly schema/view preparation (2026-10-18)

At operator request, schema-only monthly parent/detail tables (`t_monthly_climate`, `t_monthly_climate_detail`) are now part of climate bootstrap and `vw_monthly_climate_region` is part of geospatial bootstrap. DDL uses unique request/coordinate-month identities, a parent/detail FK and matching latitude/longitude LEFT JOINs. These objects are **not a monthly calculation implementation**. Empty monthly view is expected before an independently specified and tested monthly loader exists. No direct PostgreSQL operator test has been reported yet.

- [x] Add prospective monthly climate tables and wire into climate schema bootstrap.
- [x] Add monthly region view and wire into geospatial schema bootstrap.
- [x] Add unit contracts for monthly DDL.
- [ ] Run local unit and isolated PostgreSQL integration regressions.
- [ ] Verify both monthly tables and view exist and are empty on live `weather` after explicit bootstrap.
- [ ] Specify monthly aggregation methodology, eligibility/completeness and idempotent load contract separately before monthly ETL work.

# TODO — Global Database Prefix and Administrative Grid Mapping

Status: **PLANNING / IMPLEMENTATION PAUSED**
Date: 2026-10-12
Branch: **dev only**
Repository: `esnanta/etl-weather`

> This is a plan and checklist, not approval to refactor code, reset a database, or load GeoJSON.
> No production code, SQL schema, Airflow DAG, or data should be modified until the plan is reviewed.

## P5 daily-region acceptance — operator regression (2026-10-18)

Operator ran `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` suites after the Python module rename: **28 unit geospatial tests passed in 0.61s**; isolated `test_daily_region_view.py` **1 passed in 0.87s**; complete `tests/integration/geospatial` **6 passed in 0.98s**. The single view test is included in the six integration tests, not additional to them. Prior live `weather` read-only verification: 1,590 source/view/detail IDs, 1,350 matched and 240 unmatched observations, 0 missing mappings, `PASS`. Benchmark execution times (pilot scale only): baseline 0.344 ms; daily region view 1.391 ms; administrative grouping 2.928 ms; Aceh Besar time series 1.558 ms. No new index is justified by the current small pilot.

**Acceptance:** P5 daily grid mapping and `vw_daily_climate_region` implementation/regression are accepted. Monthly region view remains deferred pending a genuine monthly-statistics persistence contract; this is not an implicit instruction to build monthly climate tables. Long-horizon performance still needs representative-volume benchmarks.

- [x] Rerun unit geospatial suite after module rename (28 passed).
- [x] Run isolated daily view PostgreSQL integration (1 passed).
- [x] Run complete geospatial integration suite (6 passed).
- [x] Accept P5 daily regional mapping/view within its defined scope, without additional indexes or climate/airflow changes.
- [ ] Return to deferred P3 Airflow contract verification.
- [ ] Repeat regional-query benchmarking when materially more climate observations have accumulated.

## P5 daily region view integration fixture (2026-10-18)

Implemented `tests/integration/geospatial/test_daily_region_view.py` in `dev`: isolated `TEST_POSTGRES_*` runtime fixture using actual PostgreSQL schema/bootstrap, one matched coordinate, one unmatched coordinate, and one coordinate with no mapping; assert row identity, metadata NULL semantics, preserved observation count, repeat bootstrap idempotency and single view catalog entry. Do not execute it against `weather`. Renamed `database/region_views.py` to `database/vw_daily_climate_region.py`, and updated dependent imports/tests/docs. No changes to scientific ETL or orchestration.

- [x] Implement isolated PostgreSQL daily region view integration fixture.
- [x] Rename Python view module and update imports.
- [ ] Operator run `source activate-airflow.sh -q` and `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q` after rename.
- [ ] Operator run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration/geospatial/test_daily_region_view.py -q`.
- [ ] Operator run complete `tests/integration/geospatial -q` and capture actual results before P5 acceptance.

## P5 daily region view bootstrap revision (2026-10-18)

Operator executed manual `CREATE VIEW` against `weather`; read-only verification returned `PASS`: source/view/distinct = 1,590, missing mapping=0, matched observations=1,350, unmatched=240, ambiguous=0, invalid assignments=0. Baseline `EXPLAIN (ANALYZE, BUFFERS)` used sequential scan on 1,590 rows with execution time 0.266 ms; the two region-view/grouping plans have not yet been provided.

Operator requested daily view automatic creation during geospatial schema initialization. Implemented `database/region_views.py` and hooked `initialize_geospatial_schema()` (used by `database.geospatial --execute` and combined bootstrap), maintaining table-before-view order and existing low-level schema builder test isolation. Geospatial-only CLI now requires pre-existing climate parent/detail tables and fails explicitly if absent; combined bootstrap creates those first. No live DB deployment or local test run has been performed by the assistant.

- [x] Persist operator daily-region-view verification PASS results.
- [x] Integrate daily region view creation into geospatial initializer; do not require manual SQL in normal deployment.
- [x] Add unit checks for dependencies, view LEFT JOIN contract, and initialization order.
- [ ] Operator rerun `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q`.
- [ ] Operator rerun `python -m database.geospatial --execute` against existing `weather` and verify the view and `06_daily_region_view_verification.sql` still return PASS.
- [ ] Operator capture remaining two region-view/grouped EXPLAIN plans; no index decision until evaluated.
- [ ] Add and execute isolated PostgreSQL integration test for the view, including unmatched/no mapping and stable 1:1 row membership.
- [ ] Align or retire the manual `05_daily_climate_region_view.sql` reference if the canonical runtime view definition changes.

## P5.2 acceptance and P5 region-view rollout (2026-10-18)

Operator screenshots and logs verified persisted `weather.climate_data`: 530 source coordinates = 530 mapping rows; 450 matched, 80 unmatched, 0 ambiguous; no missing mappings or inconsistent assignments; 1,590 source and joined observations, `validation_status=PASS`. Explicit repeat `python -m geospatial.mapping --execute` committed the same classifications. Snapshot comparison showed 530 original rows, 0 missing rows, 0 changed classifications and 0 changed `mapped_at` timestamps; current mapped rows remained 530. Export of all 530 coordinate/status records was supplied and independently compared against the attached 23-feature Aceh geometry; 80 unmatched were retained rather than forcibly assigned. P5.2 database/idempotency/spatial comparison acceptance is complete; external authoritative boundary validation is out of this acceptance scope.

P5 view implementation is now staged as **SQL files on dev only**; no view creation has been executed against operator PostgreSQL by the assistant.

- [x] P5.2 verify live mapping, invariant row counts and repeated-run stability.
- [x] P5.2 export 530 coordinates and analyze 80 unmatched using Aceh geometry.
- [x] Audit canonical daily/detail/mapping schema: daily observations exist, monthly statistic storage does not.
- [x] Add `geospatial/sql/05_daily_climate_region_view.sql` (normal daily view), `06_daily_region_view_verification.sql` and `07_daily_region_view_performance.sql`.
- [ ] Operator apply daily region view on `weather` and capture verification PASS with actual row counts.
- [ ] Operator run and report all three EXPLAIN ANALYZE plans; evaluate real join costs and whether indexes are justified.
- [ ] Add isolated PostgreSQL region-view integration fixture and run regression tests before final P5 acceptance.
- [ ] Decide independently whether to implement monthly statistics persistence; **do not create monthly view** before a real monthly detail schema exists.
- [ ] Complete P5 review and sign-off prior to deferred P3 Airflow checks.

## P5.2 real grid mapping persisted (operator report, 2026-10-17)

Operator ran `python -m geospatial.mapping --execute` against `weather.climate_data`: **530 coordinates, 450 matched, 80 unmatched, 0 ambiguous**, with terminal `Grid mapping committed`. This confirms the CLI write transaction completed; independent read-only persisted-row verification and repeated-run timestamp/idempotency verification are next.

- [x] Complete live-grid mapping commit after successful 1–3 May 2024 ETL pilot.
- [x] Add `geospatial/sql/03_grid_mapping_verification.sql` with SELECT-only status, coordinate membership and LEFT JOIN row-count checks and a CSV export of exact coordinate/status pairs for a precise map.
- [ ] Run verification SQL on `weather` and capture `validation_status=PASS`, ideally 530 mapped rows, 450 matched, 80 unmatched, and 1,590 source/joined daily observations.
- [ ] Export **all 530** classified coordinates (not only unmatched coordinates) plus admin geometries to plot an exact, reproducible geographical map. Generated conceptual artwork is not geospatial evidence.
- [ ] Rerun mapping and confirm unchanged coordinates and `mapped_at` timestamps.
- [ ] Continue P5 region views and performance only after persisted mapping is verified.

## P5.2 real ERA5-Land pilot and spatial preview (operator report, 2026-10-17)

Operator ran `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-03 python -m scripts.run_pipeline_cds`. The CDS download used 2024-04-30 context and May 1–3 target in two chunks; RAW NetCDF had 96 hourly timestamps and 1,505 bounding-box grid locations (43 × 35); transformed output had 530 valid grid positions per local day × 3 days = **1,590** observations. Temporal/spatial/dataframe validation passed, with no duplicate grid records. PostgreSQL `weather.climate_data` accepted 1,590 observations, `daily_climate_id=1`, and the total runtime was 289.98s. Source artifact checkpoints were created.

Operator ran `python -m geospatial.mapping` in preview mode: **530 coordinates; 450 matched; 80 unmatched; 0 ambiguous** (84.91% covered by exactly one district, 15.09% with no covering district). The preview made no mapping writes. Do not assume that all unmatched coordinates are coastal/ocean without inspecting their spatial distribution; absence of ambiguous classifications is encouraging but not a standalone geometry-coverage proof.

- [x] Populate a limited real ERA5-Land observation pilot and preview P5 district assignment.
- [ ] Diagnose/report geographic distribution of the 80 unmatched points (coastal/offshore/outside district coverage); do not force match them.
- [ ] After reviewing the preview, run `python -m geospatial.mapping --execute` and verify 530 rows and match statuses in PostgreSQL; repeat to prove no duplicates and unchanged timestamps.
- [ ] Build read-only region views only after verifying mapping persistence and LEFT JOIN row-count invariants.

## P5.2 regression checkpoint and mapping README (2026-10-17)

Operator previewed `python -m geospatial.mapping` against `weather.climate_data`: **coordinates=0, matched=0, unmatched=0, ambiguous=0**. This is expected because climate observation rows have not been restored after DESTROY. No mapping rows were written.

Operator tests before targeted fix: `tests/unit/geospatial -q` **1 failed, 23 passed (0.29s)**. The sole failure, `test_ambiguous_and_unmatched_never_get_assigned_districts`, asserted an incorrect text fragment `WHEN match_count = 1 THEN MIN(...)`. Actual SQL uses `CASE WHEN COUNT(district_id) = 1 THEN MIN(...)` in the classification CTE; the unit assertion was corrected without changing the mapping implementation. `tests/integration/geospatial/test_grid_mapping.py -q` **1 passed (0.91s)** and `tests/integration/geospatial -q` **5 passed (0.84s)**.

Documented both preview and explicit `--execute` in `geospatial/README.md`, including the empty-observations case, `ST_Covers` classifications, idempotency and isolated verification. Root README expansion is explicitly **pending** by operator decision.

- [x] Re-ran `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q` after the assertion correction: **24 passed in 0.22s** (operator report, 2026-10-17).
- [x] Isolated PostgreSQL mapping integration test passed and full geospatial integration subset passed.
- [ ] Restore a limited real pilot of climate observations before previewing P5 mapping against `weather`. Do not execute mapping on empty observations merely to claim coverage.

## P5.1 audit and P5.2 implementation checkpoint (2026-10-17)

**P5.1 audited source:** `database/geospatial.py` already defines `t_grid_region` with `DOUBLE PRECISION` latitude/longitude, a unique coordinate key, allowed `matched/unmatched/ambiguous` statuses, match count checks, nullable administrative IDs on non-unique/no matches, and a composite district/province FK. `database/climate.py` stores observation coordinates as `DOUBLE PRECISION`. No schema migration is required for this mapping step. `geospatial.importers.districts` previously loaded and verified 23 districts and 1 Aceh province; polygon shapes are MultiPolygonZ/SRID 4326.

**P5.2 initial implementation:** `geospatial/mapping.py` provides preview by default and `python -m geospatial.mapping --execute` to upsert mappings **only for distinct persisted observations** from `climate_data.t_daily_climate_detail`. `ST_Covers` tests point (longitude, latitude) against every district polygon, including boundary points. Exactly one candidate gives both province and district IDs; zero gives `unmatched`; multiple gives `ambiguous` with NULL IDs (no arbitrary tie-breaking). Idempotent coordinate UPSERT preserves `mapped_at` when classification is unchanged. Preview does not write. This work deliberately does not create climate observations, administrative query views, or Airflow tasks.

**Fixture coverage written:** `tests/unit/geospatial/test_mapping.py` checks query and guard contracts; `tests/integration/geospatial/test_grid_mapping.py` is an isolated TEST_POSTGRES-only fixture with one inside point, one external point, and one shared-boundary point, asserting 1/1/1 match classes and identical repeated writes. Tests **have not yet been run by the operator**. The integration fixture refuses nonempty test tables, inserts synthetic source/observations only into `weather_test`, and cleans fixture rows on completion.

- [x] P5.1 inspect schema, coordinate types and mapping identity.
- [x] P5.1 define `ST_Covers` match semantics and no arbitrary assignment.
- [x] P5.2 add isolated fixture and mapping implementation in `geospatial/` without changing ETL or Airflow.
- [ ] Run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q`.
- [ ] Run `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration/geospatial/test_grid_mapping.py -q`; review SQL compilation/runtime and report any failure before accepting P5.2.
- [ ] Run full `tests/integration/geospatial -q` after fixture regression.
- [ ] Populate representative real climate points using an approved standalone pilot (the last DESTROY cleared observations), then preview mapping and inspect matched/unmatched/ambiguous counts against Aceh polygons before `--execute` on `weather`.
- [ ] Implement/verify read-only region query views and benchmark joins; keep observational LEFT JOIN row count invariant.

## P4 re-verification after DESTROY — accepted (2026-10-17)

Operator ran `python -m geospatial.importers.districts` (dry-run: 23 valid source features) and `python -m geospatial.importers.districts --execute` on freshly recreated `weather.climate_data`. Import committed **23 districts, 1 province** with source SHA-256 `b64051d0ba5cf434ad2bd50a8ba71520a16f623e534529f9276f29a946e7d2b1`. Read-only database query confirmed PostGIS **3.4.2** installed in schema `climate_data`.

The operator then executed `geospatial/sql/02_import_verification.sql` against `weather` and provided a screenshot of **1 province, 0 invalid provinces, 23 districts, 23 unique source references, 0 invalid districts, 0 unexpected first-Z values, validation_status=PASS**. Automated regression: `tests/unit/geospatial -q` **19 passed in 0.33s**, and `tests/integration/geospatial/test_import_idempotency.py -q` **4 passed in 0.91s**. P4 live import/reverification acceptance is complete.

**Next P5 dependency:** the earlier DESTROY wiped existing climate observations. Before building a grid-to-region mapping using actual observations, run a controlled standalone ETL pilot or use an isolated, representative coordinate fixture. Do not treat 1,505 bounding-box positions as 1,505 verified persisted observation locations (the earlier three-day pilot reported 530 complete-variable points per local day). Keep spatial mapping independent from Airflow and scientific ETL stages.

## Current work after root database refactor (2026-10-17)

The separate root database consolidation DB-R1–DB-R6 has passed its documented acceptance scope. Root `database/` now owns the canonical eight-table DDL and PostGIS lifecycle; `weather` was successfully destroyed/recreated and bootstrapped, followed by **15 PostgreSQL integration tests passed**, **366 unit tests passed**, and **93 operations tests passed**. The original detailed refactor plan was removed from `docs/todo/` after completion; its history remains available in Git.

**Important state reset:** the latest `destroy_all --execute` recreated **empty** `weather`, and reported zero data entries remaining. Historical P2 observations (May 2024 pilot) and P4 Aceh administrative imports recorded below are evidence of **earlier runs**, not records assumed to exist now. Before P5 spatial mapping, restore or regenerate validated climate observations and explicitly re-import `geospatial/sources/id/districts/Aceh.geojson`; confirm persisted province/district counts and geometry validity using read-only SQL. Do not infer that previous imported rows survived database recreation.

**Remaining mainline scope:** P3 verification of Airflow scheduling contract where not yet evidenced, P4 current-database administrative import verification, P5 grid-to-region association, spatial query views and performance, and P6 end-to-end documentation/acceptance.

**Separate maintenance follow-ups (not P5 prerequisites):**
- [ ] Make `destroy_all` inspect-only run its full read-only privileges/PostGIS/session preflight, without stopping processes or altering data.
- [ ] Revisit the conservative superuser requirement for PostGIS extension activation only with explicit security review.
- [ ] Optionally test RESET and cleanup against disposable populated fixtures; prior live DESTROY reported `*_entries_removed=0`, so populated-artifact deletion was not specifically exercised.

Historical statuses and superseded PostGIS-in-`public` instructions further below are **not** current deployment instructions. Canonical PostGIS schema is `climate_data`.

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

**Repository source of truth for district boundary import:** `/geospatial/sources/id/districts/Aceh.geojson` (verified as a path on branch `dev` on 2026-10-12). This is the input to the future independent administrative loader, not a source for the existing climate ETL or Airflow DAG. The current repository copy has not yet undergone feature-count, property, CRS, and geometry-validity inspection; validate it during P4. The previously uploaded files are background references, not the import path.

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
| Boundary dataset | `geospatial/sources/id/districts/Aceh.geojson` | File exists on dev; contents/geometry were not fully validated in P0. |

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

### P0 final targeted audit — 2026-10-12

**Targeted check A — database references and operational SQL (completed, source audit only):**

- Already centralized: `config.py` (`APPLICATION_SCHEMA`, `TABLE_PREFIX`, derived climate/progress/pacing names); `pipeline/database/config.py`, `pipeline/database/schema.py`, `pipeline/loaders/daily_climate_postgres.py`, `pipeline/repositories/daily_climate_reader.py`; `orchestration/repositories/pipeline_progress.py`, `submission_pacing.py`, `load_evidence.py`; `scripts/inspect_campaign_capacity.py`. These use configured object names or production DatabaseConfig, not proof all end-to-end paths have executed.
- **Confirmed stale literals:** `scripts/destroy_all.py` (`EXPECTED_APPLICATION_TABLES` still has `daily_climate`, `daily_climate_detail`); `query/01_schema__01_inventory.sql` (unprefixed inventory); `query/02_daily_climate__01_partition_summary.sql`, `query/03_daily_climate_detail__01_data_quality.sql`, `query/04_pipeline_progress__01_campaign_status.sql`, `query/05_pipeline_progress_detail__01_load_verification.sql` (unprefixed schema-qualified SELECT/JOIN targets). Fix these in P1 as SQL/name-only edits, and preserve query meaning.
- **Documentation drift:** `AGENTS.md`, `tests/AGENTS.md`, query SQL descriptions and relevant README files still require new physical-table nomenclature. `REGION_TABLE = table_name("region")` remains obsolete in root `config.py`; use agreed `GRID_REGION_TABLE` in P1.
- `scripts/recreate_postgres_database.py` and other reset wrappers need guarded review for target validation. Never run destructive scripts as part of this audit. The primary code paths containing dynamic SQL have been inspected; use a final literal/name scan as a P1 acceptance check.

**Targeted check B — actual PostgreSQL constraint, safety, and idempotency tests (completed, evidence-qualified):**

- `tests/integration/pipeline/test_postgresql_idempotency.py` includes four real-PostgreSQL tests: repeat-load de-duplication, forced detail `IntegrityError` and rollback, missing-observation restoration, and provenance/checksum mismatch recovery. The test uses production `load_to_postgresql`, temp artifacts and isolated `TEST_POSTGRES_*` settings.
- `tests/integration/orchestration/test_orchestration_database_bootstrap.py` executes real schema bootstrap and repeat initialization; `test_pipeline_progress_reconciliation.py` exercises actual persisted progress, completeness evidence, quarter cooldown and concurrent claim behavior; `test_submission_pacing.py` verifies real-DB submission coordination.
- **Not established by existing assertions:** explicit direct negative database cases for invalid parent FK, duplicate detail observation unique index, and cascade behavior across the renamed climate tables; explicit name/constraint inventory for every renamed progress/pacing table. Add focused real-PostgreSQL tests only where these contracts are not already proved, and invoke production schema builders rather than recreating DDL in test logic.
- **Safety caveat:** integration bootstrap and idempotency fixtures include `DROP SCHEMA ... CASCADE` on isolated test DB. Existing `TEST_POSTGRES_DATABASE` name-contains-`test` guard is necessary but not sufficient on its own; before rerunning integration against any shared DB ensure dedicated disposable test target and no external data. Do not perform a development/production reset in P0.
- Reported green local baseline remains **327 unit + 10 integration passed**; this measures covered contracts only, not exhaustive code coverage or regression sensitivity.

**P0 disposition:** the two targeted checks are finished and actionable P1 scope is identified. P0 is **audit-complete with documented test-coverage limitations**, not blanket proof of all DB constraints. Changes to `/pipeline` and `/orchestration` remain database-reference-only. Actual fixes, added negative integration tests and runtime verification belong to P1, not P0.

### P0 — Audit, freeze and baseline

- [x] Read `/AGENTS.md` on `dev` (2026-10-12).
- [x] Review actual root `/config.py` and `pipeline/database/config.py` status on `dev`.
- [x] Record prior prefix commits and their incomplete state above.
- [x] Confirm grid mapping table name: `t_grid_region` and primary key `id` on all new administrative tables.
- [x] Confirm GeoJSON provenance column `source_feature_ref` instead of `source_object_id` (non-FK, not an official area code).
- [x] Confirm the repository source file path `/geospatial/sources/id/districts/Aceh.geojson` exists on `dev` (content validation pending P4).
- [ ] Confirm `t_` versus `sk_` final global table prefix before any reset (currently `t_` in config).
- [ ] Finish exhaustive reference audit of `/README.md`, `tests/AGENTS.md`, Airflow documentation, all SQL scripts, and tests on `dev` (core database definitions and several operational modules inspected; not exhaustive).
- [x] Inventory known persisted object names and affected operational SQL/test paths, with limitations described in final targeted audit; require final no-stale-literals scan during P1.
- [x] Locate primary standalone date readers: root `config.py` defines `CDS_START_DATE_ENV`/`CDS_END_DATE_ENV`, `resolve_cds_period()`; `pipeline/requests/factory.py` calls the resolver; `env-example` contains both keys; actual entrypoint is `scripts/run_pipeline_cds.py`. Full references across tests/docs still pending.
- [x] Audit current database module for monthly tables: no `create_monthly_climate*` function in `pipeline/database/schema.py`; `pipeline/runtime.py` calls `run_monthly_batch` for monthly *partitioning*, not monthly-statistic database persistence. A separate future monthly schema is **not yet verified/implemented**. Recheck full codebase before any monthly view.
- [x] Record the passing user-executed baseline: unit 327 passed (7.54s), integration 10 passed (1.53s); no assistant-side test execution.
- [ ] Verify all references to `ERA5_CAMPAIGN_SCHEDULE_ENABLED` and `ENABLED_ENV` stay intact in root config, scheduling module, env-example, startup scripts, reset tooling, docs and tests.
- [x] Freeze implementation scope: naming-only for existing pipeline/orchestration.
- [x] Re-read `/tests/AGENTS.md`; apply its test construction and integration-boundary requirements.
- [x] Review representative unit/integration coverage quality and boundaries; results documented above.
- [x] Inspect real PostgreSQL integration tests and document proven versus missing FK/unique/cascade cases; targeted check B above.

### P1 requested configuration separation — 2026-10-12

At the user's request, the single source of database naming configuration is now `pipeline/database/config.py`: `TABLE_PREFIX`, `VIEW_PREFIX`, `table_name()`, `view_name()`, daily/monthly table names, province/district/grid-region table names, regional view names, plus operational progress/pacing naming and schema aliases. These definitions were removed from root `config.py`. Root `APPLICATION_SCHEMA` remains the common schema constant imported into database config, to avoid cyclic imports and retain the existing schema contract.

Updated import sites: pipeline database config and request importers as applicable; orchestration progress/pacing repositories and campaign reporting; inspection/destroy scripts; affected unit/integration tests. Added a direct configuration contract test using production database identifiers. This is an import/configuration relocation, not a change to physical table names or runtime algorithms. The changes require full user-local unit/integration verification before P1 can close.

User-reported immediately prior to this relocation: **327 unit tests passed in 7.79s; 11 integration tests passed in 1.57s**. These results do **not** verify the new configuration relocation. Use `source activate-airflow.sh -q` for subsequent tests.

### P1 execution log — 2026-10-12 (code changed; local verification pending)

Changes applied on `dev`, **without modifying ETL scientific logic or DAG control flow**:
- `config.py`: replace obsolete `REGION_TABLE` with `GRID_REGION_TABLE`; rename only standalone date environment keys and resolver to `PIPELINE_START_DATE`, `PIPELINE_END_DATE`, `resolve_pipeline_period()`.
- `pipeline/requests/factory.py`, `tests/conftest.py`, `tests/unit/pipeline/test_config_requests.py`, `env-example`: update references to the renamed standalone date settings; keep CDS provider request processing unchanged.
- `scripts/destroy_all.py`: use centrally defined prefixed application-table constants in final-state check.
- `query/01_schema__01_inventory.sql` through `query/05_pipeline_progress_detail__01_load_verification.sql`: update object identifiers to prefixed tables, leaving query meaning unchanged.
- Root and testing `AGENTS.md`: update documented physical table names. Airflow README commands now use `source activate-airflow.sh -q`.
- `tests/integration/pipeline/test_postgresql_idempotency.py`: add real database checks of orphan detail FK rejection, duplicate grid/time rejection and parent-delete cascade (rollback preserves fixtures); invokes the existing production loader and isolated `TEST_POSTGRES_*` configuration.

**Not yet verified:** no local pytest or PostgreSQL integration execution was possible from GitHub-only code-edit access. Do not claim P1 complete or green. Confirm target test database is isolated/disposable before running integration tests. Do not drop or recreate application database without separately confirmed destructive preflight.

**Remaining P1 acceptance:** run unit/integration suites locally, review failures without changing pipeline/orchestration flow, confirm final project-wide old-table/name scan, inspect object existence and constraints in isolated test PostgreSQL, and document results. Historical P0 notes above describe *the earlier state* and remain preserved for audit traceability.

### P1 — Global naming refactor (only after P0 approval)

- [x] Update central constants in root `config.py` to use the approved `GRID_REGION_TABLE = table_name("grid_region")`; remove obsolete `REGION_TABLE`.
- [ ] Ensure all existing climate, progress, and pacing SQL uses centralized table identifiers, **without changing any ETL or Airflow execution/control flow**.
- [x] Update identified operational SQL `query/` and destroy-state table inventory, using the existing central prefix constants where Python exposes them; core FK/index naming already used configured names. Final audit/check still pending.
- [ ] Review SQL keywords/identifier naming; rename only documented conflicts or newly designed fields, preserving scientific data contracts.
- [ ] Review remaining `DatabaseConfig`, imports, fixture overrides, tests and documentation for consistent names; P1 changes listed above are applied but not runtime-verified.
- [x] Rename standalone period settings to `PIPELINE_START_DATE` and `PIPELINE_END_DATE`; update root `config.py` env-key definitions and resolver names as appropriate, all read sites, tests, `env-example` and documentation. Preserve the inclusive period, UTC+7 daily boundaries, and existing standalone behavior. Remove obsolete `CDS_*DATE` aliases only once all references are updated; do not add fallback compatibility for discarded development config.
- [ ] Do not change DAG topology, schedule enable switch, schedule interval, retry/pacing behavior, climate formulas, loader semantics, or any control flow inside `/pipeline` and `/orchestration`. Stop and report if a non-database-naming change seems required.
- [ ] Validate that the development reset path addresses only the intended database and does not unexpectedly delete immutable RAW artifacts.
- [ ] Test clean creation on isolated test database before any destructive operation on development.
- [ ] Run offline unit/orchestration tests and PostgreSQL integration tests; compare behavior with baseline.

### Execution priority update — 2026-10-12

The user approved completing administrative geography (P4) and grid mapping (P5) before returning to Airflow (P3). **P3 is deferred, not cancelled.** Keep administrative imports and spatial mapping independent from the meteorological ETL and Airflow DAG; existing and future climate observations must be able to join the grid-to-region mapping without rewriting source observations. Newly encountered grid points must be eligible for a separate idempotent mapping refresh.

P2 acceptance evidence: the standalone run produced three local days (2024-05-01..03), 530 complete-variable grid points daily and 1,590 observations; the repeat run resumed checkpoints and skipped database writes. The SQL pilot verification returned three rows with `validation_status=PASS`, duplicate_keys=0 and orphan_count=0 (user screenshot). Fresh isolated DB recreation was **not** established by this pilot; retain that limitation explicitly and use independent integration/bootstrap checks rather than clearing the successful `weather` database. No Airflow process was reported in the user's process check; Airflow SQLite metadata does not contain the `dag` table and will be addressed when P3 resumes.

### P2 actual pilot evidence (12 October 2026)

- First standalone run: CDS retrieval in two chunks (30 April context; 1–3 May target), RAW 96 complete hourly steps, 43 x 35 = 1,505 bbox grid positions, validated RAW NetCDF and transformed Parquet, 530 output points per local day, 1,590 rows written to PostgreSQL. Total 166.04s (extract 165.58s, transform 0.17s, load 0.29s).
- Second identical run: both checkpoints resumed, no CDS download, Parquet validated, metadata parent ID remained 1; matching request identity, checksum, artifact path and expected/database rows (1,590); PostgreSQL write skipped, zero rows processed, total 0.45s.
- Client-side SQL checks were issued for parent count, detail count, per-day counts, orphan rows and duplicates. Pasted output confirms the duplicates query returned zero rows, but does not show scalar COUNT values from the other queries.
- Caveats: existing database `weather` was reused, so empty-database recreation was not exercised. The user's logs do not independently establish Airflow scheduler/DagRun inactivity. P2 acceptance remains conditional on these checks and missing SQL count outputs.

### P2 next verification step (prepared, not yet run)

- Added `query/06_p2__01_standalone_pilot_verification.sql`: single read-only, request-key-scoped report returning three rows (one per local day) with date bounds, grid count, UTC+7 consistency, duplicates, orphan count and per-variable NULL counts. Its expected outputs are three `PASS` rows, 530 observations per day, total 1,590 and zero duplicate/orphan/time errors. No actual database query results have been returned yet.
- Before using the SQL file in PhpStorm, verify the connection is the intended `weather` database (use `query/00_connection__01_database_context.sql`); open query 06 and record the actual output rows, rather than reporting only that queries executed.
- Independently inspect Airflow status (read-only): `airflow dags list-runs cds-era5-land` and `airflow dags details cds-era5-land` where supported by the installed Airflow version; do not pause/unpause, reset, or clear DagRuns as part of this evidence capture. An environment-switch log line alone is not proof of scheduler inactivity.
- Existing pilot was run against a database that already existed. Fresh bootstrap after isolated recreation remains unverified and **must not be simulated by deleting the current database** without a separate controlled plan.
- P2 currently has successful extraction/transform/load and unchanged-state idempotency evidence, but final acceptance remains open until independent SQL results and environmental checks are recorded.

### P2 — Standalone three-day CDS smoke test (mandatory gate before administrative implementation)

- [ ] Keep Airflow scheduled DAG paused during the standalone test; no Airflow tasks should participate.
- [x] Run the standalone target period 2024-05-01 through 2024-05-03 using `PIPELINE_START_DATE` and `PIPELINE_END_DATE`.
- [x] Use the active Aceh bounding box (N6.1, S1.9, W94.9, E98.3) and five CDS variables without scientific behavior changes.
- [ ] After isolated database recreation, run the existing standalone entrypoint and verify retrieval, RAW NetCDF validation, transformed Parquet and successful PostgreSQL writes.
- [x] Verify prefixed parent/detail tables, FK integrity, partition identity and UTC+7 local-day boundaries; SQL pilot returned three PASS rows with zero duplicate and orphan counts.
- [x] Reconcile 1,505 RAW bbox locations with 530 complete-variable grid points per local day (1,590 transformed rows for three days). Do not assume all RAW bbox locations are valid complete-variable output; missing values are not zero.
- [x] Rerun the same standalone period: RAW and Parquet checkpoints resumed; request key, checksum, path, and 1,590-row count matched; database writes skipped (0 rows processed); second run completed in 0.45s.
- [ ] Record the exact CLI, database count queries, timings, artifacts, observed warnings and all test evidence.
- [x] **Gate:** three-day download → transform → load and rerun-idempotency succeeded; proceed to spatial P4/P5 before deferred Airflow P3.

### P3 — Airflow schedule contract regression (no configuration removal)

- [ ] **Retain** `ERA5_CAMPAIGN_SCHEDULE_ENABLED` and root config `ENABLED_ENV` as-is; P3 is verification-only, not an Airflow scheduling refactor.
- [ ] Verify `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` keeps the DAG manually triggerable without automatic scheduling (`schedule=None`).
- [ ] Verify `ERA5_CAMPAIGN_SCHEDULE_ENABLED=true` preserves the current fixed campaign scheduling interval, bounded by `CAMPAIGN_START_YEAR` and `CAMPAIGN_END_YEAR`.
- [ ] Keep terminal standalone `PIPELINE_START_DATE` and `PIPELINE_END_DATE` behavior separate from Airflow campaign settings.
- [ ] Verify scheduled/manual DagRun handling and existing pause/unpause, reset, pacing, retry, and idempotency tests.
- [ ] Do not change `orchestration/scheduling/schedule.py` or remove related environment/documentation entries except to fix a proven regression introduced by table prefix naming.

### P4.1 source GeoJSON audit — 2026-10-12

Read-only audit of `geospatial/sources/id/districts/Aceh.geojson`, with bytes verified against Git blob SHA `1158b116c33b27024d20e48ac84f36acbc3137fb` (22,912,586 bytes). Parsing and geometry inspection used JSON and Shapely; the source itself was not changed.

- FeatureCollection name `Aceh`, declared CRS `urn:ogc:def:crs:OGC:1.3:CRS84` (longitude/latitude).
- **23** features, all `MultiPolygon`; **18 kabupaten and five kota**; every feature includes exactly `OBJECTID`, `WADMKK`, `WADMPR`; all `WADMPR` values equal `Aceh`. There are **23 unique non-null OBJECTID** values (source JSON numbers, parsed as floats), **23 unique non-blank district names**, zero missing required property values.
- All **23/23 polygons valid and nonempty** by Shapely, **423 total polygon parts**, 480,227 coordinate tuples; every tuple has X/Y/Z with **Z=0**. No non-finite coordinates and no holes in constituent polygons. Horizontal extent (W,S,E,N): 94.971911046, 1.977152001, 98.287444011, 6.076832204. The source bounding rectangle lies within the project's ERA5-Land retrieval bbox.
- No pair of features had positive overlapping area above `1e-12` square degrees in a Shapely pairwise intersection inspection. This is not authoritative evidence of full provincial coverage or exact gap-free administrative topology.
- Design implications for P4.2/P4.3: preserve immutable GeoJSON; explicitly normalize integer-valued numeric `OBJECTID` (e.g. `113.0`) to string `source_feature_ref='113'` scoped to a named/versioned source; preserve all source XYZ coordinates as `geometry(MultiPolygonZ,4326)`; zero-valued Z is not evidence of surveyed zero elevation and the vertical datum is not established. No importer or database schema was created during P4.1.
- Source dataset release date, definitive license/attribution and official administrative-code provenance are **not established** by the GeoJSON content alone; verify before publication. `OBJECTID` is a source feature reference, **not** a government administrative code.

### P4 source mapping and 3D geometry decisions — 2026-10-12

- Preserve the source Z coordinate (including Z=0) on import. Target PostGIS geometry is `geometry(MultiPolygonZ,4326)`, with CRS84 longitude/latitude mapped to EPSG:4326 X/Y. Do not treat source Z=0 as verified elevation or invent a vertical datum. The original GeoJSON stays immutable.
- Add `geospatial/config.py` when implementing P4: centralize the source-specific mapping of `WADMPR` to province name, `WADMKK` to district name, and `OBJECTID` to `source_feature_ref`, plus source identity/version, declared CRS, geometry dimensionality, and ID normalization policy. Importer must consume this configuration instead of hard-coding property keys.
- Store `source_feature_ref` as `VARCHAR(128)` (a bounded character string) because it identifies a source feature rather than a numeric measure. PostgreSQL `VARCHAR(n)` and `TEXT` use essentially the same variable-length storage; the limit adds validation, not a fixed-size storage saving. Reject overlong values explicitly rather than silently truncating. Apply number-to-canonical-text normalization only where explicitly prescribed by a source profile (e.g., numeric 113.0 -> "113"); retain lexical form for string references, including leading zeros and string "113.0". Scope uniqueness to a documented source identity and version.
- Add unit tests for mapping substitution, missing properties, source-specific numeric/string ID behavior, XYZ preservation and invalid CRS/geometry. Verify PostGIS 3D round-trip in isolated integration tests.

### P4.2 PostGIS preflight — partial local evidence received (2026-10-12)

Read-only preflight SQL: `geospatial/sql/01_postgis_preflight.sql`. Execute using PhpStorm connected to the existing **`weather`** application database. The user explicitly chose `weather.climate_data` for spatial tables; avoid separate `geospatial_test` deployment. It reports database context, installed/available PostGIS versions, privileges and a status. This query does **not** install PostGIS; a successful query only establishes extension package visibility/installation on that database. When PostGIS is installed, separately execute the commented 3D `ST_GeomFromEWKT`/`ST_AsEWKT` round-trip query. User's first screenshot on `weather@localhost` shows `postgis` with `default_version=3.4.2` and `installed_version=NULL`: package installed, extension not activated in that database. A second screenshot shows a successful `PostGIS_Version()` and `ST_AsEWKT(ST_GeomFromEWKT(...))` round-trip with XYZ ordinates on a console whose selected schema is `geospatial_test.public`; it appears the test database supports PostGIS 3.4. This is sufficient evidence of 3D functions in that connection, but the screenshot does not separately establish the full preflight context/privileges. The user has approved enabling PostGIS in `weather` for the spatial implementation. No Airflow actions.

### P4 bootstrap integration decision — 2026-10-12

Implemented `geospatial/database/bootstrap.py` with a standalone CLI: `python -m geospatial.database.bootstrap`. It verifies the configured database identity, checks the installed extension, and activates PostGIS idempotently if absent. It does not create/drop application databases or climate tables. When the GeoJSON importer is implemented in P4.4, it **must invoke the same bootstrap helper automatically before the import transaction**; this importer integration remains pending and must be tested (first-run activation and repeated import). Do not claim auto-import integration already works. The chosen application target is `weather.climate_data` via `POSTGRES_*` configuration. The CLI has not been executed on the user's host by the assistant.

### P4.2 runtime evidence — 2026-10-12

User ran `source activate-airflow.sh -q` and `python -m geospatial.database.bootstrap`. Actual output: `INFO | PostGIS ready | database=weather | version=3.4.2` and `PostGIS ready (version 3.4.2).` This verifies the independent Python bootstrap against the configured application database. No claim is made that the importer auto-invocation is implemented (P4.4) or that schema tables already exist (P4.3). Airflow remains deferred.

### P4.3 spatial table design — implemented, not yet applied to PostgreSQL

- `geospatial/config.py`: typed immutable source profile for Aceh fields `WADMPR`, `WADMKK`, `OBJECTID`, CRS84, 3D geometry, country code `id`, and source dataset/version. Version is explicitly `unverified` pending provider metadata.
- `geospatial/database/schema.py`: idempotent CREATE TABLE/INDEX for `climate_data.t_province`, `climate_data.t_district`, `climate_data.t_grid_region`. Names sourced from centralized `pipeline/database/config.py`. Province/district geometry uses `MultiPolygonZ` 4326 with validity/nonempty checks and GiST indexes. Source references are `VARCHAR(128)` and scoped by source dataset/version; parent/child constraints and unique administrative names are explicit. Province feature reference may be NULL for a dissolved geometry.
- `t_grid_region` stores one row per latitude/longitude pair, matching status and match count, nullable IDs for unmatched/ambiguous points; composite district/province FK ensures district belongs to the assigned province. Direct FK on climate observations is intentionally deferred (P5).
- CLI `python -m geospatial.database.schema` only previews the target; `--execute` performs CREATE TABLE/INDEX. It invokes PostGIS bootstrap, never creates/drops the database, changes climate tables, or imports GeoJSON. No DDL has been executed on the user's machine by the assistant.
- Unit tests exercise real schema DDL generation and mapping contracts via a captured connection; server-side PostGIS constraints, FK behavior, 3D round-trip and idempotency still require integration verification against the user's database. Do not mark P4.3 runtime verification complete prematurely.

### P4.4 importer implementation — pending user runtime verification

Implemented `geospatial/importers/reader.py` and `geospatial/importers/districts.py`: mapped immutable-source validation before writes, source-specific reference normalization, automatic PostGIS bootstrap, transaction-scoped upserts, 3D geometry, and province dissolve from *all stored district boundaries* so a partial import does not delete other districts or reduce the province outline. Added `tests/unit/geospatial/test_importer.py` and read-only `geospatial/sql/02_import_verification.sql`. CLI defaults to validation-only; `--execute` performs writes. **Do not mark P4.4 completed until user executes tests, first import, repeat import and SQL checks.** Source release/version still unverified.

### P4.4 first import evidence — 2026-10-13

User reported `python -m pytest tests/unit/geospatial -q`: **19 passed in 0.70s**. Running `python -m geospatial.importers.districts --execute` targeted `weather.climate_data`, validated 23 GeoJSON features, confirmed PostGIS 3.4.2, processed 23/23 districts, and logged transaction commit with one province. Source SHA-256: `b64051d0ba5cf434ad2bd50a8ba71520a16f623e534529f9276f29a946e7d2b1`. This establishes a successful *first* import run, **not** yet database-side geometry checks or repeated-import idempotency; those require user verification using `geospatial/sql/02_import_verification.sql` and rerun. Airflow remains out of scope.

### Administrative geometry column naming — 2026-10-13

Approved column name for provincial and district boundary geometries: `boundary_geometry geometry(MultiPolygonZ,4326)` (replaces `geom`). DDL, importer, validation query and schema tests now use it. The one-off rename SQL file has been removed at user request; future clean-slate database recreation will use `boundary_geometry` directly. The user provided a screenshot of the import verification query returning PASS for 1 province, 23 districts, 23 distinct source references, 0 invalid geometries and 0 unexpected first-Z values, as well as 19 passing unit tests. A second importer execution and rollback behavior remain unverified. **Do not run destructive database reset unless the user expressly requests execution and confirms the targets.**

### P4.4 idempotency and rollback integration coverage — awaiting execution

Added `tests/integration/geospatial/test_import_idempotency.py` to exercise the **production** schema builder and importer on actual PostgreSQL/PostGIS. The fixture requires guarded `TEST_POSTGRES_*` settings, uses a unique throwaway schema and drops only that schema. Tests cover two consecutive imports with stable IDs/XYZ; partial-file import preserving other districts and province; forced failure after the first genuine INSERT proving rollback; and a failed repeat import preserving previous committed state. Run `python -m pytest tests/integration/geospatial/test_import_idempotency.py -q`. These tests have not yet been run on the user's PostgreSQL server and are not claimed to have passed. After a pass, verify the second full Aceh import and `geospatial/sql/02_import_verification.sql` against `weather`.

### P4.4 idempotency / rollback runtime evidence — 2026-10-13

User ran `python -m pytest tests/integration/geospatial/test_import_idempotency.py -q`: **4 passed in 1.48s**, confirming the real PostgreSQL test cases for repeat imports, partial imports, rollback after a write, and failed-retry rollback/recovery under isolated TEST_POSTGRES configuration. User then reran `python -m geospatial.importers.districts --execute` on `weather.climate_data`: **23/23 features processed, one province, transaction committed**; source SHA-256 unchanged (`b64051d0ba5cf434ad2bd50a8ba71520a16f623e534529f9276f29a946e7d2b1`). Previous SQL verification on `weather` returned 1 province, 23 districts, 23 unique source IDs and `PASS` (before the repeat run). **Final post-repeat read-only SQL verification is still requested** to independently establish persisted counts after the second run.

User also added commit `4f8e0b2` (`tests/integration/geospatial/__init__.py`); it was preserved.

### Unified PostGIS schema contract — 2026-10-13

The old P4 deployment arrangement used `public`; the new approved extension placement is `climate_data`, while application tables reside in `climate_data`. Bootstrap creates the extension with `WITH SCHEMA public` if missing and fails on an incompatible pre-existing installation rather than relocating it automatically. DDL refers to `public.geometry`, validity functions to `public.ST_*`, and importer/verification queries use schema-qualified PostGIS functions. The PostgreSQL server-side PostGIS package remains an explicit deployment prerequisite. User still needs to rerun unit/integration tests after these changes. The latest screenshot showed 23 districts, 23 unique references and zero invalid geometries, though the displayed result pane did not include the final PASS column.

### PostGIS extension placement revised — 2026-10-13

User chose `climate_data` for **both** the application tables and the PostGIS extension objects. This supersedes the earlier `public` placement. On a clean database, bootstrap creates schema `climate_data` and uses `CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA climate_data`, validating the installed schema. DDL, importer, verification SQL, and integration query references use `climate_data.geometry` and `climate_data.ST_*`. The existing `weather` database still has PostGIS installed in `public` and should **not** be modified or destroyed by this change; the new bootstrap intentionally rejects the old location until an explicitly authorized clean-slate rebuild. Test databases with PostGIS previously in `public` also need an isolated fresh target; unit/integration regressions under the new contract are pending.

### P4 — Administrative reference schema and importer

- [x] Check PostGIS package availability on PostgreSQL 16 and activate/verify it on `weather`: user executed `python -m geospatial.database.bootstrap` successfully; PostGIS version 3.4.2 reported (2026-10-12).
- [ ] Define/create `t_province`, `t_district`, `t_grid_region` (approved), constraints, indexes, and provenance; use `source_feature_ref` in source-derived records, never `source_object_id`.
- [ ] Document and test `OBJECTID` → `source_feature_ref` conversion (e.g. numeric 113.0 to canonical text `"113"`), source dataset/version scope, null handling for dissolved provinces, and idempotent conflict identity.
- [ ] Use `/geospatial/sources/id/districts/Aceh.geojson` as the authoritative import input. Do not rely on a separately uploaded one-district sample.
- [x] Inspect source GeoJSON feature count, OBJECTID/WADMPR/WADMKK, CRS84, 3D MultiPolygon Z=0 and Shapely geometric validity; findings recorded above.
- [ ] Keep administration data loading in a separate component outside Airflow and outside scientific transformation stages; do not add an administrative task to the existing DAG.
- [ ] Implement idempotent GeoJSON reader/importer; validate GeoJSON properties (`OBJECTID`, `WADMKK`, `WADMPR`), geometry type, CRS (OGC:CRS84 input / EPSG:4326 storage), invalid polygon handling and duplicate identities. Report missing or duplicate `source_feature_ref` values within a source dataset.
- [ ] Import Aceh kabupaten/kota boundaries from `/geospatial/sources/id/districts/Aceh.geojson`; reconcile actual feature counts and report anomalies.
- [ ] Prevent a partial one-kabupaten file load from deleting other imported administrative records.
- [x] Add and execute real PostgreSQL tests for re-import, partial import, rollback, failed retry; 4 passed (2026-10-13). Unit tests cover missing mapped fields and invalid CRS; invalid geometry/topology and unmatched grid cases remain additional P4/P5 acceptance checks.

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
| P4 — GeoJSON import | Load `geospatial/sources/id/districts/Aceh.geojson` using a separate, idempotent administrative loader into province/district polygons. | Source properties/geometry validated, rows loaded and re-import safe. |
| P5 — Grid association | One-time grid-to-polygon mapping and read-only prefixed daily/monthly views. | Match/unmatched/ambiguous report; views preserve rows and permit district/province queries. |
| P6 — Final verification | Regression suites, documentation and signed checklist. | Recorded test evidence and consistent docs. |

## 7. Stop conditions

Stop implementation and return to design review if: changes to `/pipeline` or `/orchestration` require any program-flow modification (not solely database identifiers); the proposed schema introduces a second application schema; any scientific or Airflow scheduling behavior must change; polygon data does not match the assumed administrative level; ambiguous spatial assignment is silently resolved; destructive target cannot be confirmed; or automated integration tests indicate regressions.

**Current authorization:** documentation and planning only. No further code edits or database/data operations. The Airflow schedule enable flag remains part of the existing contract; do not replace it with unconditional scheduling.
