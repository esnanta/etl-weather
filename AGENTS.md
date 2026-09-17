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
- P15 is the current modelling-data access stage.

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

Do not reintroduce `weather_dataset` / `weather_dataset_detail` as active table names. Those names are legacy migration inputs only.

### `daily_climate`

Stores logical partition metadata. `request_key` is deterministic from:

```text
source
dataset
start_date
end_date
variable set
bbox
```

Variable order must not change request identity.

### `daily_climate_detail`

Stores canonical daily climate observations.

Observation identity:

```text
dataset_id + valid_time + latitude + longitude
```

Use UPSERT semantics. Do not return to append-only loading.

Database daily-statistic columns:

```text
tp     -> tp_daily_total_mm
t2m    -> t2m_daily_mean_c
d2m    -> d2m_daily_mean_c
swvl1  -> swvl1_daily_mean_m3_m3
ssrd   -> ssrd_daily_total_j_m2
```

`dataset_id` remains the foreign-key identity of the logical partition.

Development databases with legacy table names may be migrated by renaming:

```text
weather_dataset        -> daily_climate
weather_dataset_detail -> daily_climate_detail
```

only when the canonical table names do not already exist.

---

## P15 — Modelling Data Access Contract

P15 means:

> Provide controlled and validated access from canonical daily climate observations in PostgreSQL to an in-memory dataset ready for modelling, without creating a duplicate physical dataset and without changing meteorological semantics.

P15 does **not** create a separate physical `daily_feature_dataset` merely to copy existing columns.

Canonical flow:

```text
daily_climate + daily_climate_detail
        ↓
explicit modelling query / repository access
        ↓
in-memory DataFrame
        ↓
P16 wet-day identification
        ↓
P17 feature contribution evaluation
```

Modelling selection must explicitly constrain the relevant:

```text
source
dataset
period
spatial scope
partition / variable identity
```

All five meteorological variables are candidate parameters only. Do not assume all five become clustering features.

Do not add `is_wet_day` to canonical climate truth during P15. Wet-day semantics belong to P16.

Do not perform scaling, anomaly calculation, rolling features, or clustering transformations in P15 unless separately justified by a later contract.

---

## Validation Rules

Do not bypass validation because an artifact or database row already exists.

Important checks include:

- continuous hourly RAW time;
- valid ERA5-Land grid spacing;
- exact UTC/local +7-hour relation;
- local midnight semantics;
- complete requested local-day coverage;
- duplicate grid-record prevention;
- stable valid-land grid membership;
- required transformed columns.

Scientific validation takes precedence over generic row-count checks.

---

## Testing

Follow `tests/AGENTS.md`.

Tests must be:

- pytest-based;
- deterministic;
- offline by default;
- small and readable;
- independent from Airflow for core pipeline behavior.

For P15, tests must verify controlled selection and returned schema/semantics rather than creating a duplicate feature-storage layer.

---

## Current Status

Completed and verified:

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
[x] P20 hourly ERA5-Land RAW
```

Active:

```text
[ ] P15 controlled daily-climate modelling access
```

Do not mark a roadmap item complete without test/runtime evidence.

---

## Forbidden Actions

- Do not overwrite RAW checkpoints.
- Do not combine ARCO and CDS artifacts.
- Do not change provider variable names silently.
- Do not change unit/time semantics silently.
- Do not bypass validation on resumed artifacts.
- Do not return database loading to blind append behavior.
- Do not use `weather_dataset` names as the active schema contract.
- Do not create a duplicate P15 physical dataset without a demonstrated need.
- Do not drop `valid_time` or rename `valid_time_local` to generic `date`.
- Do not treat missing values as zero.
- Do not encode wet-day or clustering semantics into canonical daily climate data.
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
