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
P15 CONTROLLED MODELLING READ
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

## P15 — Controlled Modelling Data Access

P15 provides read-only access from canonical PostgreSQL daily climate observations into an in-memory modelling DataFrame.

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

P15 validates selection, required columns, non-empty results, local-midnight semantics, requested period bounds, duplicate local-day/grid observations across partitions, deterministic ordering, and preservation of missing values.

P15 does **not** create a duplicate physical feature dataset, mutate database state, add wet-day labels, scale values, or choose final clustering features.

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

## P18 — Test Coverage Hardening

P18 strengthens the existing ERA5-Land test suite before provenance, Airflow orchestration, and long historical backfill.

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
- P15 empty result, invalid bbox/dataset, local-midnight, and period-boundary failures.

P18 remains **verification pending** until the expanded focused suite is executed locally. The previous verified baseline is:

```text
37 passed
```

The expanded P18 suite is expected to collect:

```text
59 tests
```

Warnings emitted by third-party `xarray/netCDF4` internals against NumPy 2.5 are not treated as pipeline failures.

---

## P16 / P17 Timing

P16 and P17 are intentionally deferred until approximately ten years of historical daily climate data have been collected.

```text
P16 Wet-Day Definition
P17 Feature Contribution
```

The five current meteorological variables remain **candidate parameters**. P15 does not assume all five become clustering inputs.

---

## Airflow Boundary

Airflow is not required by core pipeline code.

Future Airflow owns schedules, data intervals, task dependencies, retries, concurrency, sensors, XCom wiring, Connections/secrets, and operational alerting.

Pipeline modules own extraction, validation, scientific transformation, checkpoint/resume, PostgreSQL persistence, and modelling-data access.

Future XCom handoff should contain small identifiers/artifact references, not full DataFrames or NetCDF payloads.

---

## Verification State

```text
[x] P4  retain hourly RAW data
[x] P5  local day UTC+7
[x] P6  variable-specific aggregation
[x] P7  observation schema whitelist
[x] P8  exclude CDS metadata from observations
[x] P9  provider variable naming contract
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[x] P15 controlled daily-climate modelling access
[x] P20 hourly ERA5-Land as primary RAW input
[~] P18 test coverage hardening — implementation added, verification pending
[ ] P19 provenance
[ ] Airflow DAG / historical backfill orchestration
[ ] 10-year historical backfill
[ ] P16 wet-day definition — deferred until 10-year data exists
[ ] P17 feature contribution — deferred until 10-year data exists
[ ] wet-day clustering
```

---

## Current Roadmap

```text
P15 Controlled Modelling Access
        ↓
P18 Test Coverage Hardening
        ↓
P19 Provenance
        ↓
Airflow DAG ETL
        ↓
10-Year Historical Backfill
        ↓
P16 Wet-Day Definition
        ↓
P17 Feature Contribution
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
