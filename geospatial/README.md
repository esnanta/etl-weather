# Geospatial

The geospatial component manages administrative boundaries, PostGIS spatial tables, grid-to-region mapping, and regional query views. It is independent of Airflow and does not alter the scientific climate observations.

For project installation and the primary setup CLI, see [`/README.md`](../README.md).

## Source Data

Administrative district boundaries are stored under:

```text
geospatial/sources/id/districts/
```

The Aceh GeoJSON source is treated as immutable during import.

## PostGIS Contract

PostGIS is a PostgreSQL extension and must be available on the PostgreSQL server.

Spatial objects use the application schema:

```text
climate_data
```

Bootstrap expects PostGIS to be installed in that schema. If an existing PostGIS extension is found in another schema, initialization stops rather than relocating it automatically.

Spatial tables:

```text
t_province
t_district
t_grid_region
```

Regional views:

```text
vw_daily_climate_region
vw_monthly_climate_region
```

Runtime identifiers are derived from `database/config.py`.

## Bootstrap

The primary project-level database setup commands are documented in the root README.

For an existing database that only needs application objects initialized:

```bash
python -m database.bootstrap --execute
```

Lower-level spatial-only initialization remains available for diagnostics or partial recovery:

```bash
python -m database.geospatial --execute
```

## District Import

Validate the source without writing:

```bash
python -m geospatial.importers.districts
```

Import/upsert administrative boundaries:

```bash
python -m geospatial.importers.districts --execute
```

The importer validates the complete source before committing changes.

## Grid-to-Region Mapping

Mapping reads distinct coordinates already present in canonical daily climate observations.

Preview:

```bash
python -m geospatial.mapping
```

Persist mapping:

```bash
python -m geospatial.mapping --execute
```

Classification rules:

- exactly one covering district → mapped;
- zero covering districts → unmatched;
- more than one covering district → ambiguous;
- ambiguous points are not assigned arbitrarily.

Re-running the mapping updates existing coordinate rows instead of duplicating them.

## Regional Views

`vw_daily_climate_region` preserves one row per daily climate detail record and attaches region labels through grid mapping.

The daily view exposes the canonical climate observation values, including `swvl2_daily_mean_m3_m3`, `swvl3_daily_mean_m3_m3`, `u10_daily_mean_m_s`, `v10_daily_mean_m_s`, `wind_speed_daily_mean_m_s`, and `wind_direction_daily_deg`. These six fields are appended after the existing regional columns to preserve the previous view projection order. Historical five-variable rows may contain `NULL` in these fields. The view does not recompute wind or change stored observations.

Unmatched, ambiguous, or not-yet-mapped observations remain visible with nullable region labels.

Administrative classification is point-based, not area-weighted. Aggregating grid observations by district must not be interpreted as an area-weighted district climate statistic.

The monthly regional view is provisioned separately from the active daily ETL. Monthly aggregation/loading remains experimental until its scientific and integration contracts are accepted.

## SQL References

Files under `geospatial/sql/` are operator/reference SQL and use the default `climate_data.t_*` and `climate_data.vw_*` names.

They are not dynamically rewritten when database identifier prefixes change.

Review static SQL before using non-default schema/table/view identifiers.

## Verification

Run focused geospatial unit tests:

```bash
python -m pytest tests/unit/geospatial -q
```

Run geospatial integration tests:

```bash
python -m pytest tests/integration/geospatial -q
```

Integration tests must use an isolated `TEST_POSTGRES_*` database and must never point at the development `weather` database.

The test database must satisfy the same PostGIS-in-`climate_data` contract as the application database.

For complete testing rules, see [`tests/README.md`](../tests/README.md).
