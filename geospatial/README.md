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
