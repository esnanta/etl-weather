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
- testability without Airflow;
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
tp     accumulated → local-day total → mm/day
ssrd   accumulated → local-day total → J/m²/day
t2m    hourly instantaneous → local-day mean → °C
d2m    hourly instantaneous → local-day mean → °C
swvl1  hourly instantaneous → local-day mean → m³/m³
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
NetCDF → DataFrame
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

The development database supports only the current canonical schema. Discarded development schemas are not carried through compatibility code.

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

It does **not**:

- create a `daily_feature_dataset` table/file;
- change database schema;
- copy observations into a second physical dataset;
- replace missing values with zero;
- add wet-day labels;
- scale or normalize meteorological values;
- choose final clustering features.

Implementation:

```text
pipeline/modelling_data.py
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

P15 validates that:

- selection dates and bbox are valid;
- required modelling columns exist;
- the query returns observations;
- no duplicate `(valid_time_local, latitude, longitude)` modelling observations exist across selected partitions;
- `valid_time_local` remains local midnight;
- observations stay inside the requested local-date period;
- missing meteorological values remain missing.

The read path deliberately opens the configured PostgreSQL database without creating database/schema objects. P15 is read-only.

A thin local verification entrypoint is available:

```bash
python scripts/read_modelling_data.py
```

For the current sanity configuration it should read the same three local days already loaded by `scripts/run_pipeline_cds.py`.

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

Verified before P15 implementation:

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
[x] P20 hourly ERA5-Land as primary RAW input
```

Latest verified focused baseline before the new P15 tests:

```text
32 passed
```

Current implementation status:

```text
[~] P15 controlled daily-climate modelling access — implemented, verification pending
[ ] P18 complete ERA5-Land test coverage
[ ] P19 provenance
[ ] Airflow DAG / historical backfill orchestration
[ ] P16 wet-day definition — deferred until 10-year data exists
[ ] P17 feature contribution — deferred until 10-year data exists
```

---

## Authoritative Guidance

Before changing pipeline behavior, read:

- `AGENTS.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` when changing tests

Scientific correctness takes precedence over convenience, especially for units, timestamps, accumulation semantics, missing data, spatial grids, and source-specific assumptions.
