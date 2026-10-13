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

This is idempotent and activates PostGIS only when missing; it never creates/drops the database or alters weather observations. Initial activation requires sufficient database permissions. The GeoJSON importer invokes `ensure_postgis_extension(engine, expected_database=...)` automatically before writing features. This module does not use Airflow.

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

## P4.4 GeoJSON district importer

`geospatial/importers/reader.py` validates the configured source file and never changes it. Property names and source identity are read from `geospatial/config.py`, not hard-coded in importer logic. `geospatial/importers/districts.py` imports all valid features in one PostgreSQL transaction with district UPSERT keyed by `(source_dataset, source_version, source_feature_ref)` and a unique province name scoped to country. The province polygon is dissolved from **all districts currently in the database** after import, so a partial input does not remove previously loaded regions or shrink the province polygon. The importer does not touch climate observations or grid mappings.

Validate source without modifying PostgreSQL:

```bash
source activate-airflow.sh -q
python -m geospatial.importers.districts
python -m pytest tests/unit/geospatial -q
```

Import into configured `weather.climate_data` (**database write**):

```bash
python -m geospatial.importers.districts --execute
```

Then run `geospatial/sql/02_import_verification.sql` in PhpStorm, and repeat the import and verification to check idempotency. Expected initial Aceh counts: one province, 23 districts, and no grid mapping yet. The configured source version remains `unverified`; replace it only once actual release metadata is established, with a corresponding migration/identity strategy. The user has confirmed the first live import: 23 districts, one province, and a read-only validation query returning PASS with no invalid geometry; repeated-import idempotency and explicit rollback tests are still pending.

The `--execute` command displays a `tqdm` progress bar for district UPSERTs (23 expected for the Aceh source). A full bar means all district statements finished; the province dissolve and transaction COMMIT may still be in progress. The importer reports success only after COMMIT. If an exception occurs, the transaction rolls back and the importer does not print its success message. The progress bar is CLI-only; library callers can omit the optional progress callback.

Administrative boundary columns are named `boundary_geometry` in both `t_province` and `t_district` (`geometry(MultiPolygonZ,4326)`); `geometry` is the PostGIS type. Fresh schema bootstrap creates `boundary_geometry` directly. Existing databases may be deliberately reset by the user at a later stage; the project does not retain a one-off rename migration.
