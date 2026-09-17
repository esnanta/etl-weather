# Weather Dataset Pipeline

## Overview

This repository contains a weather ETL pipeline designed to process ERA5-family climate data independently from Apache Airflow while remaining ready to be orchestrated by Airflow later.

The current active development path is **Copernicus CDS ERA5-Land hourly data**, transformed into validated daily data for Aceh/Indonesia use cases. ARCO ERA5 support remains in the repository as a separate source path and must not be merged with CDS artifacts.

The pipeline is designed to be:

- **Scientifically explicit** — units, time semantics, aggregation rules, and source assumptions are visible in code and tests.
- **Deterministic** — the same logical request produces the same artifact identity and database partition identity.
- **Resumable** — completed RAW and transformed checkpoints can be reused safely.
- **Idempotent** — retrying a database load does not create duplicate observations.
- **Testable without Airflow** — core ETL behavior is ordinary Python code covered by pytest.
- **Airflow-ready** — orchestration concerns can be added later without moving ETL business logic into DAG files.

---

## Current Scientific Scope

### Active dataset

```text
source  : CDS
product : reanalysis-era5-land
input   : hourly ERA5-Land
output  : daily local-time dataset
```

### Active provider variables

Provider variable codes are the primary variable contract throughout the pipeline:

```text
tp     total precipitation
t2m    2 metre temperature
d2m    2 metre dewpoint temperature
swvl1  volumetric soil water layer 1
ssrd   surface solar radiation downwards
```

Long CDS API names such as `total_precipitation` or `2m_temperature` are external API details and human-readable descriptions. They are not the primary internal variable names.

RAW and transformed artifacts therefore retain:

```text
tp, t2m, d2m, swvl1, ssrd
```

### Daily aggregation contract

The current local daily timezone is:

```text
Asia/Jakarta = UTC+7
```

The daily aggregation rules are variable-specific:

```text
tp     accumulated → local-day total → m to mm
ssrd   accumulated → local-day total → J/m²/day
t2m    instantaneous hourly values → local-day mean → K to °C
d2m    instantaneous hourly values → local-day mean → K to °C
swvl1  instantaneous hourly values → local-day mean → m³/m³
```

ERA5-Land accumulated variables are not aggregated by simply summing hourly stored accumulation values. For local day `D` in UTC+7, the current boundary method is:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

where `A` is the ERA5-Land forecast-step accumulation.

The extractor retrieves one preceding UTC day as context when required by this local-day boundary calculation.

---

## Pipeline Architecture

The current logical flow is:

```text
WeatherRequest
    |
    v
EXTRACT OR RESUME
    |
    +--> CDS / ARCO source adapter
    |
    v
RAW NetCDF checkpoint
    |
    v
RAW VALIDATION
    |
    v
NetCDF -> DataFrame
    |
    v
TRANSFORM OR RESUME
    |
    v
Daily transformed Parquet checkpoint
    |
    v
TRANSFORMED VALIDATION
    |
    v
IDEMPOTENT PostgreSQL LOAD
```

The main runtime boundary is intentionally compatible with future Airflow tasks:

```text
extract_or_resume()
        ↓
transform_or_resume()
        ↓
load()
```

`WeatherPipeline.run()` remains a local wrapper that composes these same stage boundaries.

---

## Core Components

### `pipeline/model.py`

Defines typed domain objects such as:

- `BoundingBox`
- `WeatherRequest`
- `RawWeatherDataset`
- `LoadedWeatherDataset`
- `TransformedWeatherDataset`
- `DatasetArtifact`
- `DataLayer`
- `DataSource`

### `pipeline/extract.py`

Responsible for source retrieval and source-boundary normalization.

Current CDS behavior includes:

- translating provider codes to CDS API variable names only at the API request boundary;
- converting API-returned long variable names back to provider codes immediately;
- retaining `valid_time` as the pipeline temporal coordinate;
- retrieving one preceding day for ERA5-Land UTC+7 accumulated-variable aggregation;
- supporting cross-month retrieval safely.

### `pipeline/validation.py`

Validates scientific and structural contracts before data continues downstream.

Current ERA5-Land validation includes:

- RAW hourly temporal continuity;
- unique and monotonic `valid_time`;
- expected 1-hour spacing;
- RAW source-grid resolution and bounding-box extent;
- local/UTC daily timestamp relationship;
- exact requested local-day coverage;
- duplicate daily grid-record detection;
- transformed source-grid alignment;
- stable land-grid membership between days;
- required DataFrame columns.

### `pipeline/transform.py`

Transforms hourly ERA5-Land data into the canonical daily representation.

Current behavior includes:

- UTC+7 local-day grouping;
- distinct rules for accumulated and instantaneous variables;
- scientific unit conversion;
- removal of source metadata fields that are not observations;
- preservation of both `valid_time` and `valid_time_local`.

Current transformed schema:

```text
valid_time
valid_time_local
latitude
longitude
tp
t2m
swvl1
ssrd
d2m
```

### `pipeline/storage.py`

Manages deterministic filesystem artifacts.

Current contracts:

- RAW = NetCDF;
- transformed = Parquet;
- artifacts are stored under `data/`;
- artifact names are deterministic by logical request partition;
- RAW artifacts are immutable;
- RAW writes are atomic;
- existing RAW/Parquet checkpoints are used for resume;
- resumed artifacts are validated before use.

### `pipeline/database.py`

Creates and migrates PostgreSQL schema objects required by the pipeline.

Current idempotency contracts:

- `weather_dataset.request_key` identifies a logical request partition;
- one transformed observation is uniquely identified by:

```text
dataset_id + valid_time + latitude + longitude
```

### `pipeline/load.py`

Loads transformed daily data into PostgreSQL using idempotent UPSERT behavior.

Provider variables are mapped to explicit daily-statistic database columns:

```text
tp     -> tp_daily_total_mm
t2m    -> t2m_daily_mean_c
d2m    -> d2m_daily_mean_c
swvl1  -> swvl1_daily_mean_m3_m3
ssrd   -> ssrd_daily_total_j_m2
```

A retry of the same logical partition reuses the same `dataset_id` and updates the same observation keys instead of appending duplicates.

### `pipeline/runtime.py`

Composes the ETL stages for local execution.

It owns pipeline composition, not Airflow orchestration.

Important stage methods:

```text
extract_or_resume()
transform_or_resume()
load()
```

### `scripts/config.py`

Contains the default local execution requests.

For current CDS ERA5-Land development, the configured request is intentionally small enough for fast sanity checking before large historical extraction.

### `scripts/run_pipeline_arco.py` and `scripts/run_pipeline_cds.py`

Thin local entrypoints. They do not contain transformation business logic.

---

## Data Storage

Artifact paths are source-specific and period-specific.

Example current CDS ERA5-Land layout:

```text
data/
└── reanalysis-era5-land/
    ├── raw/
    │   └── cds/
    │       └── 2022/
    │           └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc
    └── transformed/
        └── cds/
            └── 2022/
                └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

The filename represents a logical data partition, not a program execution. Therefore a retry or future Airflow rerun for the same request resolves to the same artifact identity.

ARCO and CDS outputs remain in separate source directories even when their date ranges overlap.

---

## Checkpoint and Resume Contract

A repeated request behaves as follows:

```text
RAW missing
→ extract
→ validate
→ atomically create RAW checkpoint

RAW exists
→ resume RAW
→ validate again
```

Then:

```text
Parquet missing
→ transform RAW
→ validate
→ create transformed checkpoint

Parquet exists
→ resume Parquet
→ validate again
```

RAW must never be silently overwritten.

This behavior is required for safe Airflow retries and partial-failure recovery.

---

## Database Idempotency Contract

A deterministic `request_key` is built from the logical request identity, including:

```text
source
dataset
start_date
end_date
variable set
bounding box
```

Variable order does not change the request identity.

For the same logical request:

```text
same request_key
→ same dataset_id
```

Daily detail rows use UPSERT behavior on:

```text
dataset_id + valid_time + latitude + longitude
```

The loader reports `rows_processed`, not `rows_inserted`, because a rerun may update existing rows rather than insert new ones.

---

## Airflow Boundary

Airflow is planned but is intentionally not required by the pipeline implementation.

The future DAG layer should own:

- schedule;
- data interval selection;
- task dependencies;
- retry count and retry delay;
- pools and concurrency;
- sensors;
- XCom handoff metadata;
- Airflow Connections / secret management;
- alerting and operational controls.

The pipeline layer should continue to own:

- extraction;
- validation;
- transformation;
- scientific rules;
- artifact persistence;
- checkpoint/resume behavior;
- database idempotency.

Future XCom values should contain small control metadata such as artifact paths, request keys, dataset IDs, partition dates, or row counts — never full NetCDF files or Pandas DataFrames.

---

## Running Locally

Run the current CDS ERA5-Land pipeline:

```bash
python scripts/run_pipeline_cds.py
```

Run ARCO separately:

```bash
python scripts/run_pipeline_arco.py
```

Default local request configuration belongs in `scripts/config.py` and should not be duplicated in the runner scripts.

---

## Current Verification State

The following contracts have been completed and verified:

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

Latest focused unit-test verification after P10/P11:

```text
31 passed
```

Runtime verification confirmed:

- first run creates RAW and transformed checkpoints;
- second identical run resumes both checkpoints;
- the same `dataset_id` is reused;
- observation duplicate query returns zero rows.

---

## Next Modelling Stage

The next planned stage is:

```text
P15 — Daily Feature Dataset
```

Important: P15 must not assume that all five available ERA5-Land variables will become clustering inputs.

The five current variables are **candidate meteorological parameters**. The final feature subset for wet-day modelling will be determined empirically in later modelling work.

The conceptual downstream sequence is:

```text
daily climate data
→ P15 modelling-ready daily feature dataset
→ P16 wet-day definition / identification
→ P17 feature contribution and feature-set evaluation
→ wet-day clustering
→ cluster interpretation
→ comparison with Keuneunong
→ recommendations / warnings / scenarios
```

---

## Authoritative Project Guidance

Before changing pipeline behavior, read:

- `AGENTS.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` when changing tests

Scientific correctness takes precedence over convenience, especially for units, timestamps, accumulation semantics, missing data, spatial grids, and source-specific assumptions.
