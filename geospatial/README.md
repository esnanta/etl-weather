# Geospatial

Administrative boundaries and ERA5-Land grid-to-region mapping, independent of Airflow. All spatial tables and PostGIS objects belong to PostgreSQL schema `climate_data`; connection settings come from the project's existing `POSTGRES_*` configuration.

## Source

`sources/id/districts/Aceh.geojson` is an **immutable** GeoJSON with 23 Aceh district/city features (XYZ). Do not edit it during import. Its release/version metadata is still **unverified**; do not treat the provisional BIG attribution as confirmed.

## Setup and import

PostGIS must be installed on the PostgreSQL **server** before the Python bootstrap can enable the extension. Run from the repository root:

```bash
source activate-airflow.sh -q
python -m geospatial.database.bootstrap
python -m geospatial.database.schema --execute
python -m geospatial.importers.districts --execute
```

Bootstrap checks that the extension belongs to `climate_data` and refuses to relocate an existing extension in another schema. The schema command creates missing `t_province`, `t_district` and `t_grid_region` tables; it does not migrate existing layouts. The importer validates the immutable input and transactionally UPSERTs districts and derives province geometry. The commands do not alter meteorological observations.

For a **validation-only** import, omit `--execute`:

```bash
python -m geospatial.importers.districts
```

After import, run `geospatial/sql/02_import_verification.sql` against `weather`. Confirm the **full query result**, including `validation_status = PASS`, not just the district counts. Repeating the import should not duplicate province/district rows.

## Verification

```bash
python -m pytest tests/unit/geospatial -q
python -m pytest tests/integration/geospatial/test_import_idempotency.py -q
```

Integration tests require an **isolated** `TEST_POSTGRES_*` database; never point them at `weather` or shared data. The test database must satisfy the current PostGIS-in-`climate_data` contract.

Latest user-reported clean-database results (2026-10-16): PostGIS **3.4.2** enabled in `weather.climate_data`; all three spatial tables created; **19 unit tests passed**; importer committed **23 districts and 1 province**, source SHA-256 `b64051d0ba5cf434ad2bd50a8ba71520a16f623e534529f9276f29a946e7d2b1`. The latest SQL screenshot confirms **23 district rows, 23 unique references, 0 invalid geometries and 0 unexpected first-Z values**; the final province status and `PASS` column were not visible. The earlier isolated four integration tests passed **before** the PostGIS extension-schema relocation, so rerun them against an appropriately configured test database before treating that coverage as current.

The importer progress bar reports processed districts; transaction success is confirmed only by `Transaction committed successfully.`
