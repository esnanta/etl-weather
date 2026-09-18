# ERA5-Land Pipeline — Consolidated Design Decisions

**Project:** `etl-weather`  
**Branch:** `dev`  
**Scope:** consolidated design decisions from `docs/design_decision/01` through `06`  
**Last consolidated:** 18 September 2026

---

## 1. Purpose and Document Authority

This document is the single consolidated reference for the ERA5-Land pipeline design decisions recorded in `docs/design_decision`.

It does not replace the historical value of the individual documents. Instead, it gathers their active contracts into one place and resolves status differences by using the most recent documented state.

Where an older document records a roadmap item as unfinished but a newer document records it as completed, the newer state takes precedence. In particular:

- P15 documentation (17 September 2026) still listed P18 and P19 as future work.
- The P18/P19 documentation (18 September 2026) records both as completed and reports **61 passing tests**.

The design contracts below therefore reflect the latest documented state while retaining earlier decisions that remain valid.

---

## 2. Architectural Overview

The pipeline is intentionally separated into scientific processing, persistence, controlled data access, and future orchestration/modelling.

```text
Copernicus CDS
ERA5-Land hourly
        |
        v
EXTRACT OR RESUME
        |
        v
RAW NetCDF checkpoint
        |
        v
RAW validation
        |
        v
hourly -> local daily transformation
        |
        v
TRANSFORM OR RESUME
        |
        v
Daily Parquet checkpoint
        |
        v
transformed validation
        |
        v
IDEMPOTENT POSTGRESQL LOAD
        |
        v
climate_data.daily_climate
climate_data.daily_climate_detail
        |
        v
P15 controlled read-only repository
        |
        v
validated in-memory Pandas DataFrame
        |
        v
future wet-day / feature / clustering stages
```

Core principle:

> Airflow will orchestrate *when* and *in what order* work runs; the pipeline remains responsible for extraction, scientific transformation, validation, storage, provenance, and idempotent persistence.

Scientific/business logic must remain outside the DAG so the pipeline can still run locally, be tested independently, and be reused without Airflow.

---

## 3. Source Dataset and Active Variables

Current source contract:

```text
Source   : cds
Dataset  : reanalysis-era5-land
Input    : hourly ERA5-Land
Timezone : Asia/Jakarta / UTC+7
```

The active ERA5-Land provider variables are:

| Provider code | Meaning | Daily statistic |
|---|---|---|
| `tp` | total precipitation | daily total |
| `t2m` | 2 metre temperature | daily mean |
| `d2m` | 2 metre dewpoint temperature | daily mean |
| `swvl1` | volumetric soil water layer 1 | daily mean |
| `ssrd` | surface solar radiation downwards | daily total |

Provider/native codes remain the scientific vocabulary through extraction and transformation. Explicit persistence names are introduced only at the database boundary.

---

## 4. Time Contract: UTC and Local Day

The pipeline preserves two time representations:

```text
valid_time
valid_time_local
```

Their meanings are:

```text
valid_time       = ERA5-Land/source reference time in UTC
valid_time_local = local time in UTC+7
```

The relation is:

```text
valid_time_local = valid_time + 7 hours
```

Example:

```text
2021-12-31 17:00 UTC
        =
2022-01-01 00:00 UTC+7
```

Therefore a local day starts at 17:00 UTC on the previous calendar date.

Both columns are retained deliberately:

- `valid_time` preserves scientific traceability to the source dataset.
- `valid_time_local` provides the correct local calendar day for agriculture and wet-day analysis.

Do not replace `valid_time_local` with a generic `date` field and do not remove `valid_time`.

---

## 5. Daily Aggregation Contract

### 5.1 Instantaneous variables

`t2m`, `d2m`, and `swvl1` are aggregated as daily means over the UTC+7 local day.

### 5.2 Accumulated variables

`tp` and `ssrd` use ERA5-Land accumulation semantics and must not be treated as a simple sum of hourly accumulated values.

The documented local-day formula is:

```text
Daily(D) =
    A(D 17 UTC)
  + A(D 00 UTC)
  - A(D-1 17 UTC)
```

The pipeline must retrieve enough source context to calculate the target local day correctly. For example, a target period starting on 1 January requires data from the preceding UTC date because:

```text
00:00 UTC+7 = 17:00 UTC previous day
```

This preceding-day context is input context for aggregation, not an additional modelling partition.

---

## 6. Unit Contract

RAW data remains in provider units. Unit conversion occurs in the transformed daily layer.

| Variable | ERA5-Land unit | Daily transformed unit |
|---|---|---|
| `tp` | m | mm/day |
| `t2m` | K | °C |
| `d2m` | K | °C |
| `swvl1` | m³/m³ | m³/m³ |
| `ssrd` | J/m² | J/m²/day |

Conversions:

```text
tp_mm = tp_m * 1000

temperature_C = temperature_K - 273.15
```

`swvl1` remains unchanged. `ssrd` remains in J/m²/day for the current baseline; conversion to MJ/m²/day is deferred until scientifically required.

Database persistence names make statistic and unit explicit:

```text
tp    -> tp_daily_total_mm
t2m   -> t2m_daily_mean_c
d2m   -> d2m_daily_mean_c
swvl1 -> swvl1_daily_mean_m3_m3
ssrd  -> ssrd_daily_total_j_m2
```

---

## 7. Validation Contracts

Validation is not optional and is executed for both fresh and resumed artifacts.

### 7.1 RAW temporal validation

RAW hourly data must have:

- `valid_time`;
- datetime-compatible timestamps;
- chronological order;
- unique timestamps;
- one-hour intervals;
- no missing expected hourly timestep.

### 7.2 Daily temporal validation

Daily transformed data must satisfy:

```text
valid_time_local = valid_time + 7 hours
valid_time       = 17:00 UTC
valid_time_local = 00:00 local
```

The requested local days must be complete and there must be no duplicate record for:

```text
valid_time_local + latitude + longitude
```

### 7.3 Spatial validation

The documented ERA5-Land regular grid resolution is:

```text
0.1°
```

Validation checks consistency with the requested bounding box, expected latitude/longitude spacing, expected geometric grid dimensions, and stable valid-land grid membership from day to day.

In the documented sanity run:

```text
bbox latitude  : 3.4 .. 5.2
bbox longitude : 96.1 .. 97.6
latitude_count : 19
longitude_count: 16
RAW grid cells : 304
valid land grid: 249
local days     : 3
daily rows     : 747
```

The reduction from 304 geometric grid points to 249 transformed land-valid cells is expected because ERA5-Land can contain invalid/NaN land-surface values over ocean or non-valid cells. The accepted land-grid membership must remain stable across local days.

### 7.4 Missing values

Missing is never equivalent to zero.

```text
NaN -> NULL
0   -> observed zero
```

This distinction is especially critical for precipitation and later wet-day analysis.

---

## 8. Deterministic Request Identity

Each logical request has a deterministic `request_key`.

Its identity includes the information that determines the requested partition, including:

```text
source
dataset
start_date
end_date
variable set
bounding box
```

Variable order is not semantically significant. Equivalent sets such as:

```text
tp,t2m,d2m
d2m,tp,t2m
```

must resolve to the same logical variable identity.

A deterministic `variable_signature` is also retained.

Core invariant:

```text
same logical request -> same request_key
```

---

## 9. Checkpoint and Resume Design

Two primary persisted checkpoints exist:

```text
RAW checkpoint         = NetCDF
Transformed checkpoint = Parquet
```

Artifact names are deterministic and represent a logical data partition, not a runtime execution.

Example:

```text
data/reanalysis-era5-land/raw/cds/2022/
  2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc

data/reanalysis-era5-land/transformed/cds/2022/
  2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

### 9.1 RAW immutability

RAW NetCDF is treated as the closest retained scientific source truth after CDS retrieval.

Contract:

```text
RAW exists -> do not silently overwrite
```

RAW writes are atomic: write to a temporary location, complete successfully, then rename to the final path. The final checkpoint path must indicate a completed artifact, not a partial write.

### 9.2 Checkpoint status

Normal checkpoint states are:

```text
created
resumed
```

`created` means a new artifact was produced.

`resumed` means an existing artifact was reused after the relevant integrity/validation checks.

Other pipeline state labels are separate concepts:

```text
schema/table : ready
pipeline     : completed
DB mode      : idempotent upsert
```

Invalid/stale/checksum-mismatched artifacts are not represented as a successful checkpoint state; the pipeline fails explicitly.

### 9.3 Resume never skips validation

Invariant:

```text
file exists != file valid
```

On RAW resume, RAW validations are rerun.

On Parquet resume, provenance and checksum are checked, the artifact is loaded, and transformed validations are rerun.

---

## 10. Provenance Contract

P19 adds minimal artifact traceability without duplicating provenance on every observation row.

### 10.1 RAW

RAW NetCDF records:

```text
downloaded_at
```

`downloaded_at` represents when the source artifact was actually obtained and is preserved on resume.

SHA-256 is calculated from RAW file bytes.

### 10.2 Parquet file metadata

Transformed Parquet stores file-level metadata:

```text
raw_artifact_path
raw_sha256
downloaded_at
transformed_at
```

These are metadata, not observation columns.

### 10.3 PostgreSQL metadata

`daily_climate` stores transformed-artifact provenance:

```text
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

The chain is therefore:

```text
PostgreSQL
   |
   | transformed_artifact_path
   | transformed_sha256
   v
Parquet
   |
   | raw_artifact_path
   | raw_sha256
   v
RAW NetCDF
```

RAW path/SHA are not duplicated in PostgreSQL because the database is built from the transformed artifact and can reach RAW through Parquet metadata.

### 10.4 Provenance check on resume

Before a transformed checkpoint is resumed:

1. Parquet `raw_artifact_path` must match the current RAW artifact.
2. Parquet `raw_sha256` must match the current RAW SHA-256.
3. Only then can the Parquet checkpoint be resumed.
4. A mismatch is an error; stale transformed data must not be silently accepted.

The documented P19 implementation is intentionally minimal and does **not** currently include:

```text
transform_version
Git commit SHA
daily_climate_raw table
```

---

## 11. PostgreSQL Persistence and Idempotency

Canonical schema:

```text
climate_data
```

Canonical tables:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

### 11.1 `daily_climate`

Conceptually stores partition metadata such as:

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
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

`request_key` is unique and metadata load uses UPSERT semantics.

### 11.2 `daily_climate_detail`

Stores one daily observation per valid grid cell with:

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

The foreign key to `daily_climate` uses `ON DELETE CASCADE`.

Observation identity is:

```text
daily_climate_id
+ valid_time
+ latitude
+ longitude
```

Loading uses UPSERT rather than append-only insert.

This guarantees:

```text
same logical observation -> same database row
```

Retry/rerun must not create duplicate observations.

The metric is called `rows_processed`, not `rows_inserted`, because a rerun may update existing rows through UPSERT.

---

## 12. Canonical Store for Modelling

After successful extract, transform, validation, and load, PostgreSQL is the canonical persisted daily-climate source for modelling access.

The pipeline intentionally does not create a redundant persistent `daily_feature_dataset` merely to copy daily climate.

The access boundary is:

```text
pipeline/daily_climate_repository.py
```

with read-only behaviour.

It may read and validate data, but it must not:

- create or alter tables;
- write observations;
- scale or impute features;
- create wet-day labels;
- select final clustering features.

`DailyClimateSelection` explicitly selects by:

```text
source
dataset_name
start_date
end_date
bbox
```

The repository returns a deterministic in-memory Pandas DataFrame and validates required columns, non-empty results, local-midnight semantics, requested period, and uniqueness of:

```text
valid_time_local + latitude + longitude
```

This boundary protects canonical climate truth from modelling transformations.

---

## 13. Monthly Partition Strategy

Historical execution is designed around monthly partitions using:

```text
run_monthly_batch()
split_request_by_month()
```

For approximately ten years, the expected order of magnitude is about 120 monthly partitions.

Each partition has its own:

```text
request_key
RAW checkpoint
Parquet checkpoint
daily_climate metadata row
```

This is the intended unit for retry/backfill/orchestration.

Example retry behaviour:

```text
extract succeeded
transform succeeded
database load failed

retry:
RAW     -> resumed
Parquet -> resumed
DB load -> retried
```

Thus:

```text
retry != full reprocessing
```

---

## 14. Airflow Boundary

Airflow is planned as an orchestration layer only.

Airflow responsibilities:

```text
schedule
partition/data interval
task dependencies
retry
concurrency
pooling
alerting
artifact-reference handoff
backfill orchestration
```

Core pipeline responsibilities remain:

```text
extraction
validation
scientific transformation
checkpointing
checksum/provenance
database persistence
controlled repository access
```

Large DataFrames/xarray objects should not be passed through XCom. Pass small metadata or artifact references instead.

---

## 15. Contracts That Must Not Change Implicitly

The following decisions should be treated as stable contracts unless deliberately redesigned and documented:

- retain hourly ERA5-Land RAW;
- preserve both `valid_time` and `valid_time_local`;
- use UTC+7 local-day semantics;
- use variable-specific daily aggregation;
- do not sum ERA5-Land accumulated variables as ordinary hourly values;
- retain provider codes during scientific transformation;
- convert units only in transformed daily data;
- keep RAW immutable;
- use deterministic checkpoint/request identity;
- validate resumed artifacts;
- verify RAW checksum before Parquet resume;
- keep missing values distinct from zero;
- use idempotent database UPSERT;
- keep PostgreSQL daily climate canonical for modelling reads;
- keep modelling repository read-only;
- keep scientific logic outside Airflow DAGs;
- do not decide wet-day labels or final clustering features inside ETL.

---

## 16. Current Roadmap State

Latest documented status:

```text
[x] P4  RAW hourly retained
[x] P5  UTC+7 local day
[x] P6  variable-specific aggregation
[x] P7  observation schema whitelist
[x] P8  CDS/source metadata excluded from observation rows
[x] P9  provider variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[x] P15 controlled modelling access
[x] P18 test coverage hardening
[x] P19 provenance
[x] P20 hourly ERA5-Land RAW

[ ] Airflow DAG ETL
[ ] 10-year historical backfill
[ ] P16 wet-day definition
[ ] P17 feature contribution / feature-set evaluation
[ ] wet-day clustering
```

P16 and P17 remain deferred until a sufficiently representative historical dataset is available. The five current meteorological variables are candidate parameters; the ETL does not declare them to be the final clustering feature set.

---

## 17. Verification Snapshot

The latest consolidated design documentation reports:

```text
61 passed
```

and a verified sanity run with:

```text
local days             = 3
valid land grid cells  = 249
rows processed         = 747
```

Fresh run behaviour:

```text
RAW checkpoint         = created
Transformed checkpoint = created
```

Resume run behaviour:

```text
RAW checkpoint         = resumed
Transformed checkpoint = resumed
same daily_climate_id
same logical observations
no duplicate rows
```

The documented resume runtime is much faster than a fresh run, demonstrating that checkpoint reuse is operationally meaningful while still preserving validation and provenance checks.

---

## 18. Module Responsibility Map

```text
pipeline/model.py
    domain/request/data structures

pipeline/interfaces.py
    pipeline contracts/interfaces

pipeline/extract*.py
    CDS retrieval and normalization

pipeline/transform.py
    scientific hourly -> local-daily transformation

pipeline/validation.py
    temporal/spatial/data validation

pipeline/storage.py
    RAW/Parquet persistence, checkpointing, checksums/metadata

pipeline/database.py
    PostgreSQL configuration/schema/table management

pipeline/load.py
    idempotent metadata and observation persistence

pipeline/daily_climate_repository.py
    controlled read-only PostgreSQL -> modelling access

pipeline/runtime.py
    stage orchestration
```

Operational/sanity scripts documented by the design notes include:

```text
scripts/run_pipeline_cds.py
scripts/read_daily_climate.py
```

---

## 19. Source Design-Decision Documents

This consolidation is derived from:

1. `01 era5_land_valid_time_dan_valid_time_local.md`
2. `02 era5_land_validasi_temporal_dan_konversi_unit.md`
3. `03 ERA5_Land_validation_and_units.md`
4. `04 era5_land_Checkpoint_Resume_dan_Idempotent_Database_Load.md`
5. `05 ERA5_Land_Pipeline_Documentation.md`
6. `06 ERA5_Land_Checkpoint_Provenance_Documentation.md`

The individual files remain useful as historical/detail records. This document should be used as the first entry point for the current consolidated architecture and contracts.
