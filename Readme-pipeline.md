# Weather Dataset Pipeline

## Overview

This repository contains a weather ETL pipeline designed to process ERA5-family climate data independently from Apache Airflow while remaining ready to be orchestrated by Airflow later.

The current active development path is **Copernicus CDS ERA5-Land hourly data**, transformed into validated daily climate observations for Aceh/Indonesia use cases.

Core design goals:

- scientific correctness;
- deterministic artifacts and logical partitions;
- resumable RAW/transformed checkpoints;
- idempotent PostgreSQL persistence;
- testability without Airflow;
- thin future Airflow orchestration.

---

## Current Scientific Scope

### Active dataset

```text
source  : CDS
product : reanalysis-era5-land
input   : hourly ERA5-Land
output  : validated daily climate observations
```

### Provider variables

Provider codes are the primary variable contract:

```text
tp     total precipitation
t2m    2 metre temperature
d2m    2 metre dewpoint temperature
swvl1  volumetric soil water layer 1
ssrd   surface solar radiation downwards
```

Long CDS API names are external API-boundary details only.

### Daily aggregation

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

For accumulated ERA5-Land variables:

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
```

Runtime stage boundaries:

```text
extract_or_resume()
transform_or_resume()
load()
```

These boundaries are intentionally suitable for future Airflow tasks.

---

## Storage Contract

```text
RAW         = NetCDF
TRANSFORMED = Parquet
```

Artifacts live under `data/` and remain source-separated.

Example:

```text
data/reanalysis-era5-land/raw/cds/2022/2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc

data/reanalysis-era5-land/transformed/cds/2022/2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

RAW is immutable. Existing deterministic checkpoints are resumed and validated rather than silently overwritten.

---

## PostgreSQL Daily Climate Contract

The database terminology reflects the data semantics directly.

### `climate_data.daily_climate`

Stores metadata for one logical daily-climate partition, including:

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

`request_key` is deterministic from:

```text
source
dataset
start_date
end_date
variable set
bbox
```

The same logical request reuses the same metadata row.

### `climate_data.daily_climate_detail`

Stores daily grid-cell observations:

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

Observation identity is:

```text
daily_climate_id + valid_time + latitude + longitude
```

Writes use UPSERT semantics, so retrying the same logical partition does not create duplicate observations.

`daily_climate_id` is the canonical foreign key from `daily_climate_detail` to `daily_climate.id`.

### Legacy schema migration

For development databases created before this naming change, the pipeline can migrate:

```text
weather_dataset        → daily_climate
weather_dataset_detail → daily_climate_detail
dataset_id             → daily_climate_id
```

when the canonical replacement does not already exist.

This preserves existing development data while establishing the domain-specific schema.

---

## P15 — Modelling Data Access

P15 is defined as:

> **Provide controlled and validated access from daily climate observations in PostgreSQL to an in-memory dataset ready for modelling, without creating a new physical copy and without changing meteorological semantics.**

Therefore P15 does **not** create a separate `daily_feature_dataset` table/file merely to duplicate the same columns.

The canonical modelling source is PostgreSQL daily climate data:

```text
climate_data.daily_climate
        +
climate_data.daily_climate_detail
        ↓
controlled query / repository access
        ↓
in-memory Pandas DataFrame
        ↓
P16 wet-day identification
        ↓
P17 feature contribution evaluation
```

The modelling selection must explicitly constrain the relevant:

```text
source
dataset
period
spatial scope
variable set / partition identity
```

All five current meteorological variables remain **candidate parameters**. P15 does not declare that all five must be clustering features.

Missing meteorological values remain missing; they must not be silently converted to zero.

---

## Airflow Boundary

Airflow is planned but is intentionally not required by the pipeline implementation.

Future Airflow owns:

- schedules;
- data intervals;
- task dependencies;
- retries and retry delays;
- pools/concurrency;
- sensors;
- XCom control metadata;
- Connections/secrets;
- operational alerting.

Pipeline code continues to own:

- extraction;
- validation;
- transformation;
- scientific formulas;
- checkpoint/resume;
- PostgreSQL persistence;
- P15 modelling-data access.

Airflow should pass small identifiers/artifact references, not full DataFrames or NetCDF payloads through XCom.

---

## Current Verification State

Completed and verified:

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

P15 is now the active design/implementation stage.

---

## Authoritative Guidance

Before changing pipeline behavior, read:

- `AGENTS.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` when changing tests

Scientific correctness takes precedence over convenience, especially for units, timestamps, accumulation semantics, missing data, spatial grids, and source-specific assumptions.
