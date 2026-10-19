# Geospatial

Administrative boundaries and ERA5-Land grid-to-region mapping, independent of Airflow. All spatial tables and PostGIS objects belong to PostgreSQL schema `climate_data`; connection settings come from the project's existing `POSTGRES_*` configuration.

## Source

`sources/id/districts/Aceh.geojson` is an **immutable** GeoJSON with 23 Aceh district/city features (XYZ). Do not edit it during import. Its release/version metadata is still **unverified**; do not treat the provisional BIG attribution as confirmed.

## Setup and import

PostGIS must be installed on the PostgreSQL **server** before the Python bootstrap can enable the extension. Run from the repository root:

```bash
source activate-airflow.sh -q
python -m database.climate               # dry run: seven climate/operational tables
python -m database.geospatial            # dry run: three spatial tables + daily region view
python -m database.bootstrap             # dry run: all ten tables and two views
python -m database.climate --execute     # climate/operational only
python -m database.geospatial --execute  # spatial tables + daily region view; climate tables required
python -m database.bootstrap --execute   # both independent initialization paths
python -m geospatial.importers.districts --execute
```

Bootstrap checks that the extension belongs to `climate_data` and refuses to relocate an existing extension in another schema. The schema command creates missing `t_province`, `t_district` and `t_grid_region` tables and then creates or replaces `vw_daily_climate_region`; it does not migrate existing layouts. The view requires existing `t_daily_climate` and `t_daily_climate_detail` tables. On a fresh database run `python -m database.climate --execute` first, or use the combined `python -m database.bootstrap --execute` initializer. The importer validates the immutable input and transactionally UPSERTs districts and derives province geometry. The commands do not alter meteorological observations.

For a **validation-only** import, omit `--execute`:

```bash
python -m geospatial.importers.districts
```

After import, run `geospatial/sql/02_import_verification.sql` against `weather`. Confirm the **full query result**, including `validation_status = PASS`, not just the district counts. Repeating the import should not duplicate province/district rows.

## Grid-to-region mapping (P5)

Mapping reads **distinct coordinates already stored** in `climate_data.t_daily_climate_detail` and classifies them against administrative district polygons using PostGIS `ST_Covers`. It does **not** download ERA5-Land data or create climate observations. When the observation table is empty (for example after DESTROY), preview correctly reports `coordinates=0`; no mapping is possible until a validated ETL pilot loads climate observations.

Preview matching counts without changing the database:

```bash
python -m geospatial.mapping
```

Expected summary columns are `coordinates`, `matched`, `unmatched`, and `ambiguous`. Exactly one covering district assigns its province/district IDs; zero matches is `unmatched`, and more than one match (including shared boundaries) is `ambiguous` without an arbitrary district assignment. After reviewing the counts, persist the classification:

```bash
python -m geospatial.mapping --execute
```

Re-running the mapping updates existing coordinate rows rather than duplicating them; an unchanged classification retains its `mapped_at` timestamp. This tool is independent of Airflow and does not alter scientific climate observations. Run the isolated mapping integration test before using `--execute` with real observations.

## Verification

```bash
python -m pytest tests/unit/geospatial -q
python -m pytest tests/integration/geospatial/test_import_idempotency.py -q
python -m pytest tests/integration/geospatial/test_grid_mapping.py -q
python -m pytest tests/integration/geospatial/test_daily_region_view.py -q
```

Integration tests require an **isolated** `TEST_POSTGRES_*` database; never point them at `weather` or shared data. The test database must satisfy the current PostGIS-in-`climate_data` contract. If a legacy `weather_test` database has PostGIS in `public`, the integration fixture fails before writing test tables. It deliberately does **not** remove or relocate the extension. On a dedicated disposable test database only, after confirming no dependent data must be preserved, connect to `weather_test` and run `DROP EXTENSION postgis RESTRICT;` (it will refuse removal if other objects depend on it). Then run `CREATE SCHEMA IF NOT EXISTS climate_data; CREATE EXTENSION postgis WITH SCHEMA climate_data;`. Otherwise provision a new isolated test database with the proper extension location and update `TEST_POSTGRES_DATABASE`. Never perform this operation on `weather` or shared databases.

Latest user-reported clean-database results (2026-10-16): PostGIS **3.4.2** enabled in `weather.climate_data`; all three spatial tables created; **19 unit tests passed**; importer committed **23 districts and 1 province**, source SHA-256 `b64051d0ba5cf434ad2bd50a8ba71520a16f623e534529f9276f29a946e7d2b1`. The latest SQL screenshot confirms **23 district rows, 23 unique references, 0 invalid geometries and 0 unexpected first-Z values**; the final province status and `PASS` column were not visible. The earlier isolated four integration tests passed **before** the PostGIS extension-schema relocation, so rerun them against an appropriately configured test database before treating that coverage as current.

The importer progress bar reports processed districts; transaction success is confirmed only by `Transaction committed successfully.`


## P5 regional query views and performance

The canonical daily observations remain in `climate_data.t_daily_climate_detail`. The P5 query layer adds a normal PostgreSQL view rather than copying observations or adding a foreign key. **Database bootstrap owns its creation:** `python -m database.geospatial --execute` (after climate tables exist) or `python -m database.bootstrap --execute` creates the view automatically. The original `geospatial/sql/05_daily_climate_region_view.sql` is a manual reference and recovery script, not an additional required deployment step. The canonical runtime view definition is in `database/vw_daily_climate_region.py`. To verify on the configured `weather` database in PhpStorm:

1. Confirm that `climate_data.vw_daily_climate_region` exists after initialization.
2. Execute `geospatial/sql/06_daily_region_view_verification.sql` and record the complete result. For the approved May 1–3, 2024 pilot, expect `source_rows=view_rows=distinct_detail_ids=1590`, `missing_mappings=0`, `matched_observations=1350`, `unmatched_observations=240`, `ambiguous_observations=0` and `validation_status=PASS`; actual counts should follow current observations if the database changes.
3. Execute the three SELECT-only `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` statements in `geospatial/sql/07_daily_region_view_performance.sql` individually; capture actual rows, runtime, buffer hits/reads, scan and join types.
4. Do not create new indexes or change observation tables until representative measured query plans justify it.

`vw_daily_climate_region` has exactly one result per existing daily detail row, preserving records with `unmatched`, `ambiguous`, or absent grid mappings (nullable region labels). Administrative classifications are **point assignment**, not area-weighted climate averages; a grouped query across grid points should not be interpreted as an area-weighted district statistic.

`MONTHLY_CLIMATE_REGION_VIEW` is defined in `database/config.py`. The empty monthly parent/detail tables and `vw_monthly_climate_region` now exist after bootstrap. Their creation alone does not imply monthly aggregation or loading is operational; the separate experimental monthly CLI has not been integration-tested. The active `pipeline/runtime.py` processes daily observations in calendar-month request partitions.

Operator screenshots (2026-10-18) confirmed manual CREATE VIEW and verification `PASS`: 1,590 original/view/unique IDs, 1,350 matched observations, 240 unmatched, no missing/ambiguous/invalid assignments. A baseline SELECT EXPLAIN returned 1,590 rows with 0.266 ms execution (sequential scan); both view EXPLAIN plans remain unreported. The operator verified 28 geospatial unit tests passed in 0.81s, ran the geospatial initializer successfully, and again verified the daily view as PASS on 2026-10-18. The isolated daily view PostgreSQL integration fixture has been added, but its local execution result is pending; no assistant access to local PostgreSQL.

### P5 isolated daily view regression

`tests/integration/geospatial/test_daily_region_view.py` uses only `TEST_POSTGRES_*`, requires empty canonical test tables, and exercises matched/unmatched/no-mapping rows, one-to-one view membership, and repeated geospatial schema initialization. It cleans up only fixture rows and does not drop test schemas or change `weather`. Execute with:

```bash
source activate-airflow.sh -q
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit/geospatial -q
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration/geospatial/test_daily_region_view.py -q
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/integration/geospatial -q
```

## Prospective monthly climate region view

The climate bootstrap now creates empty `climate_data.t_monthly_climate` and `climate_data.t_monthly_climate_detail` parent/detail tables, and geospatial bootstrap creates `climate_data.vw_monthly_climate_region` after spatial tables. The monthly view uses coordinate-based LEFT JOINs to preserve unmatched or not-yet-mapped monthly observations. These objects were initially deployed without a loader. A separate experimental monthly aggregation/write CLI now exists at `scripts/run_monthly_climate.py`, but its scientific completeness policy and PostgreSQL writes require isolated integration tests and acceptance before use against `weather`. Run `python -m database.bootstrap --execute` after review to apply all objects, or run `python -m database.climate --execute` followed by `python -m database.geospatial --execute`. Both are additive and do not reset existing daily observations. Run `source activate-airflow.sh -q` before commands.


## PostgreSQL identifier configuration

Runtime schema and table/view names originate in `database/config.py` (`APPLICATION_SCHEMA`, `TABLE_PREFIX`, `VIEW_PREFIX`, and their derived constants). The Python spatial DDL, administrative importer, mapping, and region view builders use these identifiers. PostGIS functions/types are schema-qualified using `APPLICATION_SCHEMA`.

The files under `geospatial/sql/` are **static operator examples** written for the default `climate_data.t_*` and `climate_data.vw_*` names. They are not dynamically reconfigured by Python: review and adapt them before executing with alternate identifiers. Changing names on an existing PostgreSQL database requires a coordinated migration and revalidation; changing config values alone never renames stored objects.
