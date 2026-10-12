# Geospatial

This package owns administrative boundary source-data handling and, in later P4/P5 stages, geographic validation, import and ERA5-Land grid-to-region mapping.

## Source layout

```text
geospatial/
├── __init__.py
├── README.md
└── sources/
    └── id/
        └── districts/
            └── Aceh.geojson
```

`id` is the lowercase ISO 3166-1 alpha-2 country code for Indonesia. The Aceh GeoJSON was moved unchanged from `indonesia/districts/Aceh.geojson`. Source attribution previously documented: Badan Informasi Geospasial (BIG), <https://geoportal.big.go.id/>; verify dataset release, license and geometry metadata in P4.1 before importing.

Treat everything under `sources/` as immutable inputs. Importers must never edit or overwrite these files. The geospatial package is independent from Airflow and scientific ETL; it may access the shared PostgreSQL/PostGIS database through the existing database configuration. Database schema/importer/mapping implementation is deferred to P4/P5.

## Database bootstrap

The existing PostgreSQL application database is selected by `POSTGRES_*` settings in `pipeline/database/config.py`. Spatial tables will be stored under `climate_data`. PostGIS must be installed on the PostgreSQL server. To activate it in the configured database, run:

```bash
source activate-airflow.sh -q
python -m geospatial.database.bootstrap
```

This is idempotent and activates PostGIS only when missing; it never creates/drops the database or alters weather observations. Initial activation requires sufficient database permissions. **The future GeoJSON importer must invoke `ensure_postgis_extension(engine, expected_database=...)` automatically before writing features.** P4.4 importer has not been implemented yet. This module does not use Airflow.

## P4.3 spatial schema

The schema builder in `geospatial/database/schema.py` manages only `climate_data.t_province`, `climate_data.t_district` and `climate_data.t_grid_region`. It calls the PostGIS bootstrap before non-destructive `CREATE TABLE IF NOT EXISTS`/index statements. It does **not** import GeoJSON, run Airflow, or modify canonical climate tables. Review the target first:

```bash
source activate-airflow.sh -q
python -m geospatial.database.schema
```

To create missing tables on the configured application database (a database **write**):

```bash
python -m geospatial.database.schema --execute
```

Existing mismatched table layouts are not migrated by `IF NOT EXISTS`; verify table definitions before using P4.4. `t_province` and `t_district` preserve XYZ as `geometry(MultiPolygonZ,4326)`. The grid mapping uses `latitude` and `longitude` with a unique coordinate pair and an explicit status (`matched`, `unmatched`, `ambiguous`); only unambiguous matches receive district and province foreign keys. Source property mapping lives in `geospatial/config.py`. Version metadata for the Aceh source is currently `unverified` and must be resolved before import or published attribution.
