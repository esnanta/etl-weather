# AGENTS.md

This repository contains a weather ETL pipeline that runs independently and is intended to be orchestrated by Apache Airflow later.

Authoritative references:

- `Readme-pipeline.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` for test-specific work

Scientific data contracts documented in code and tests must not be changed implicitly.

---

## Current Project Scope

- Active development path: CDS ERA5-Land hourly data.
- ARCO remains a separate source path.
- RAW hourly data is the scientific source of truth.
- Daily transformed data uses UTC+7 local-day semantics.
- PostgreSQL stores canonical daily climate observations.
- P15 controlled PostgreSQL-to-DataFrame access is implemented and verified.
- P18 test coverage hardening is implemented and awaits local verification.
- P16 wet-day definition and P17 feature contribution are deferred until approximately ten years of daily data are available.

Do not merge ARCO/CDS artifacts or silently harmonize source-specific behavior.

---

## Airflow Boundary

Core rule:

> Airflow orchestrates the pipeline; pipeline modules own ETL and modelling-data access logic.

Keep in `pipeline/`:

- extraction;
- validation;
- scientific transformation;
- unit conversion;
- artifact storage;
- checkpoint/resume;
- database persistence/idempotency;
- P15 PostgreSQL-to-in-memory modelling access.

Keep in future `dags/`:

- schedules;
- data intervals;
- task dependencies;
- retry policy;
- pools/concurrency;
- sensors;
- XCom wiring;
- Connections/secrets;
- alerting.

Do not import Airflow into core pipeline modules.

---

## ERA5-Land Scientific Contract

Provider codes are the primary variable names:

```text
tp
t2m
d2m
swvl1
ssrd
```

Daily timezone:

```text
Asia/Jakarta = UTC+7
```

Retain both:

```text
valid_time
valid_time_local
```

Do not replace either with a generic `date` field.

Accumulated variables:

```text
tp
ssrd
```

Daily-mean variables:

```text
t2m
d2m
swvl1
```

Accumulation formula for local day `D`:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

Output units:

```text
tp     = mm/day
t2m    = degC
d2m    = degC
swvl1  = m3/m3
ssrd   = J/m2/day
```

Missing values are not zero.

---

## Transformed Schema

Canonical transformed Parquet columns:

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

Source metadata fields such as `number` and `expver` must not become weather observation columns unless a new explicit contract justifies them.

---

## Artifact Contract

```text
RAW         = NetCDF
TRANSFORMED = Parquet
```

Artifacts live under `data/` and are deterministic by logical request partition.

RAW is immutable.
Existing RAW/transformed checkpoints must be resumed and validated, not silently overwritten.

Runtime boundaries:

```text
extract_or_resume()
transform_or_resume()
load()
```

---

## PostgreSQL Daily Climate Contract

Canonical tables:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

Canonical detail foreign key:

```text
daily_climate_id
```

Observation identity for database UPSERT:

```text
daily_climate_id + valid_time + latitude + longitude
```

Database daily-statistic columns:

```text
tp     -> tp_daily_total_mm
t2m    -> t2m_daily_mean_c
d2m    -> d2m_daily_mean_c
swvl1  -> swvl1_daily_mean_m3_m3
ssrd   -> ssrd_daily_total_j_m2
```

The development database supports only the current schema. Prefer recreating development state over adding compatibility behavior for discarded schemas.

---

## P15 — Daily Climate Repository Contract

Implementation:

```text
pipeline/daily_climate_repository.py
```

Tests:

```text
tests/unit/test_daily_climate_repository.py
```

P15 provides controlled and validated read-only access from canonical PostgreSQL daily climate observations to an in-memory Pandas DataFrame.

Selection explicitly defines:

```text
source
dataset_name
start_date
end_date
bbox
```

Returned columns:

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

P15 must remain read-only, preserve missing values, preserve both timestamps, enforce local-midnight and period bounds, reject empty selections and duplicate local-day/grid observations, and return deterministic ordering.

P15 must not add wet-day labels, scaling, anomaly/rolling features, final clustering features, or create duplicate physical modelling datasets.

---

## P18 — Test Coverage Hardening

P18 protects the pipeline boundaries needed before provenance and historical backfill.

Coverage should include:

- fresh/resumed RAW checkpoints and validation after resume;
- fresh/resumed transformed checkpoints and validation after resume;
- deterministic artifact paths and overwrite refusal;
- missing-artifact failures;
- ERA5-Land transformation edge cases;
- canonical database schema and indexes;
- metadata and observation UPSERT contracts;
- missing-value preservation into database NULL;
- P15 negative cases and handoff validation;
- runtime loader delegation without Airflow.

Do not mark P18 complete until the expanded focused test suite passes locally.

---

## Validation Rules

Do not bypass validation because an artifact or database row already exists.
Scientific validation takes precedence over generic row-count checks.

---

## Testing

Follow `tests/AGENTS.md`.

Tests must be pytest-based, deterministic, offline by default, small, readable, and independent from Airflow for core pipeline behavior.

---

## Current Status

```text
[x] P4  RAW hourly retained
[x] P5  local day UTC+7
[x] P6  variable-specific aggregation
[x] P7  observation schema whitelist
[x] P8  CDS metadata excluded
[x] P9  provider variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[x] P15 controlled daily-climate modelling access
[x] P20 hourly ERA5-Land RAW
[~] P18 test coverage hardening — implementation added, verification pending
[ ] P19 provenance
[ ] Airflow DAG / historical backfill orchestration
[ ] 10-year historical backfill
[ ] P16 wet-day definition — deferred until 10-year data exists
[ ] P17 feature contribution — deferred until 10-year data exists
[ ] wet-day clustering
```

---

## Forbidden Actions

- Do not overwrite RAW checkpoints.
- Do not combine ARCO and CDS artifacts.
- Do not change provider variable names silently.
- Do not change unit/time semantics silently.
- Do not bypass validation on resumed artifacts.
- Do not return database loading to blind append behavior.
- Do not use any foreign key other than `daily_climate_id` for canonical daily-climate details.
- Do not create a duplicate P15 physical dataset without a demonstrated need.
- Do not drop `valid_time` or rename `valid_time_local` to generic `date`.
- Do not treat missing values as zero.
- Do not encode wet-day or clustering semantics into canonical daily climate data.
- Do not make the P15 read path create database objects.
- Do not place scientific transformations in future DAG files.
- Do not pass full datasets through future XCom.

---

## Practical Decision Order

When in doubt, prefer:

1. scientific correctness;
2. immutable RAW source truth;
3. deterministic partition identity;
4. explicit source separation;
5. explicit units and timestamps;
6. validation before downstream use;
7. safe retry/resume;
8. idempotent persistence;
9. canonical daily climate data as modelling source;
10. thin Airflow orchestration;
11. simple implementation over unnecessary abstraction.
