# Weather Dataset Pipeline

## Overview

This repository contains a weather ETL pipeline for ERA5-family climate data. The pipeline runs independently from Apache Airflow and is intended to be orchestrated by Airflow later.

The current active path is **Copernicus CDS ERA5-Land hourly data**, transformed into validated UTC+7 daily climate observations for Aceh/Indonesia use cases.

Core goals:

- scientific correctness;
- deterministic artifacts and logical partitions;
- resumable RAW/transformed checkpoints;
- idempotent PostgreSQL persistence;
- controlled modelling-data access;
- strong offline test coverage before historical backfill;
- thin future Airflow orchestration.

---

## Scientific Contract

Active dataset:

```text
source  : CDS
product : reanalysis-era5-land
input   : hourly ERA5-Land
output  : validated daily climate observations
```

Provider codes remain the primary variable names:

```text
tp     total precipitation
t2m    2 metre temperature
d2m    2 metre dewpoint temperature
swvl1  volumetric soil water layer 1
ssrd   surface solar radiation downwards
```

Current local timezone:

```text
Asia/Jakarta = UTC+7
```

Daily rules:

```text
tp     accumulated -> local-day total -> mm/day
ssrd   accumulated -> local-day total -> J/m2/day
t2m    hourly instantaneous -> local-day mean -> degC
d2m    hourly instantaneous -> local-day mean -> degC
swvl1  hourly instantaneous -> local-day mean -> m3/m3
```

For ERA5-Land accumulated fields:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

Daily outputs retain both:

```text
valid_time
valid_time_local
```

Neither timestamp is replaced by a generic `date` field.

---

## Pipeline Flow

```text
WeatherRequest
    ↓
EXTRACT OR RESUME
    ↓
RAW NetCDF checkpoint
    ↓
RAW validation
    ↓
NetCDF -> DataFrame
    ↓
TRANSFORM OR RESUME
    ↓
Daily transformed Parquet checkpoint
    ↓
Transformed validation
    ↓
IDEMPOTENT POSTGRESQL LOAD
    ↓
climate_data.daily_climate
climate_data.daily_climate_detail
    ↓
CONTROLLED MODELLING READ
    ↓
in-memory Pandas DataFrame
```

Runtime ETL boundaries remain:

```text
extract_or_resume()
transform_or_resume()
load()
```

---

## Storage Contract

```text
RAW         = NetCDF
TRANSFORMED = Parquet
```

Artifacts live under `data/`, remain source-separated, and are deterministic by logical request partition.

RAW checkpoints are immutable. Existing RAW/transformed checkpoints are resumed and validated rather than silently overwritten.

Minimal provenance:

```text
RAW NetCDF metadata
  downloaded_at

Parquet metadata
  raw_artifact_path
  raw_sha256
  downloaded_at
  transformed_at

daily_climate metadata
  downloaded_at
  transformed_artifact_path
  transformed_sha256
  transformed_at
```

RAW path/checksum stay in Parquet metadata and are not duplicated into PostgreSQL. Provenance is partition metadata, not observation columns.

---

## PostgreSQL Daily Climate Contract

Canonical tables:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

### `daily_climate`

Stores metadata for one logical daily-climate partition:

```text
id
request_key
dataset_name
source
start_date
end_date
variable_signature
north_lat
south_lat
east_lon
west_lon
transformed_at
```

`request_key` is required and deterministic from source, dataset, date range, variable set, and bbox.

### `daily_climate_detail`

Stores daily grid-cell observations:

```text
id
daily_climate_id
valid_time
valid_time_local
latitude
longitude
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
```

Observation identity for database UPSERT is:

```text
daily_climate_id + valid_time + latitude + longitude
```

`daily_climate_id` is the canonical foreign key to `daily_climate.id`.

---

## Controlled Modelling Data Access

The daily-climate repository provides read-only access from canonical PostgreSQL daily climate observations into an in-memory modelling DataFrame.

Implementation:

```text
pipeline/daily_climate_repository.py
```

Primary API:

```python
DailyClimateSelection(...)
load_daily_climate_for_modelling(selection)
```

A selection explicitly defines:

```text
source
dataset_name
start_date
end_date
bbox
```

The returned DataFrame contract is:

```text
daily_climate_id
valid_time
valid_time_local
latitude
longitude
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
```

The repository validates selection, required columns, non-empty results, local-midnight semantics, requested period bounds, duplicate local-day/grid observations across partitions, deterministic ordering, and preservation of missing values.

The repository does **not** create a duplicate physical feature dataset, mutate database state, add wet-day labels, scale values, or choose final clustering features.

Verification entrypoint:

```bash
python scripts/read_daily_climate.py
```

Verified sanity result:

```text
rows                     747
daily_climate_partitions 1
local_days               3
grid_cells               249
```

---

## Test Coverage Hardening

The test suite protects ERA5-Land behavior before Airflow orchestration and long historical backfill.

Hardening added in this stage covers:

- runtime `extract_or_resume()` fresh and resume branches;
- validation of resumed RAW checkpoints;
- runtime `transform_or_resume()` fresh and resume branches;
- validation of resumed transformed checkpoints;
- loader delegation boundary;
- canonical PostgreSQL schema DDL contracts;
- observation UPSERT conflict identity;
- missing-value conversion to database `NULL`;
- empty observation loads;
- transformed Parquet deterministic path, save/load, and overwrite refusal;
- missing checkpoint failure behavior;
- transformation rejection of missing requested variables;
- transformation rejection of incomplete local days;
- modelling-data access failures for empty results, invalid bbox/dataset, local-midnight, and period boundaries.

The current unit-test baseline is:

```text
90 passed
```

Warnings emitted by third-party `xarray/netCDF4` internals against NumPy 2.5 are not treated as pipeline failures.

---

## Minimal Provenance

Provenance metadata adds practical traceability without transform-version or Git-commit versioning.

The provenance chain is:

```text
RAW NetCDF
  downloaded_at
  SHA-256 calculated from file bytes
        ↓
Transformed Parquet
  raw_artifact_path
  raw_sha256
  downloaded_at
  transformed_at
        ↓
PostgreSQL daily_climate
  downloaded_at
  transformed_artifact_path
  transformed_sha256
  transformed_at
```

A transformed checkpoint is rejected on resume when the current RAW SHA-256 differs from the checksum recorded in the Parquet metadata. This prevents stale transformed data from being silently reused after RAW bytes change.

Development artifacts and database schema created before this provenance contract should be recreated for verification rather than migrated.

---

## Deferred Modelling Decisions

Wet-day definition and feature-contribution analysis are intentionally deferred until approximately ten years of historical daily climate data have been collected.

```text
Wet-Day Definition
Feature-Contribution Analysis
```

The five current meteorological variables remain **candidate parameters**. The daily-climate repository does not assume all five become clustering inputs.

---

## Airflow Boundary

Airflow is not required by core pipeline code.

Future Airflow owns schedules, data intervals, task dependencies, retries, concurrency, sensors, XCom wiring, Connections/secrets, and operational alerting.

Pipeline modules own extraction, validation, scientific transformation, checkpoint/resume, PostgreSQL persistence, and modelling-data access.

Future XCom handoff should contain small identifiers/artifact references, not full DataFrames or NetCDF payloads.

---

## Verification State

```text
[x] retain hourly ERA5-Land RAW data
[x] local day UTC+7
[x] variable-specific aggregation
[x] observation schema whitelist
[x] exclude CDS metadata from observations
[x] provider variable naming contract
[x] checkpoint / resume
[x] idempotent database load
[x] temporal and spatial validation
[x] unit conversion
[x] controlled daily-climate modelling access
[x] artifact provenance
[x] retry and backfill test coverage
[ ] Airflow DAG / historical backfill orchestration
[ ] 10-year historical backfill
[ ] wet-day definition — deferred until 10-year data exists
[ ] feature-contribution analysis — deferred until 10-year data exists
[ ] wet-day clustering
```

---

## Current Roadmap

```text
Controlled Modelling Access
        ↓
Test Coverage Hardening
        ↓
Artifact Provenance
        ↓
Airflow DAG ETL
        ↓
10-Year Historical Backfill
        ↓
Wet-Day Definition
        ↓
Feature-Contribution Analysis
        ↓
Wet-Day Clustering
```

---

## Authoritative Guidance

Before changing pipeline behavior, read:

- `AGENTS.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` when changing tests

Scientific correctness takes precedence over convenience, especially for units, timestamps, accumulation semantics, missing data, spatial grids, and source-specific assumptions.
