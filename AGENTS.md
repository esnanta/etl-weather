# AGENTS.md

This repository contains a weather ETL pipeline that runs independently and is intended to be orchestrated by Apache Airflow later.

Project and coding references:

- `README.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` for test-specific work

Scientific data contracts documented in code and tests must not be changed implicitly.

---

## Current Project Scope

- The only active provider path is Copernicus CDS ERA5-Land hourly data.
- RAW hourly data is the scientific source of truth.
- Daily transformed data uses UTC+7 local-day semantics.
- PostgreSQL stores canonical daily climate observations.
- Controlled PostgreSQL-to-DataFrame access is implemented and verified.

Do not add another provider path without an explicit design decision, isolated artifacts, dedicated dependencies, and source-specific tests.

---

## Repository Conventions

- Use English for code, comments, and identifiers.
- Treat RAW data as immutable.
- Put Airflow orchestration in `dags/`.
- Put ETL logic in `pipeline/`.
- Use `scripts/` only for thin entrypoints and wrappers.
- Keep the active provider and dataset identity explicit in requests, artifacts, logs, and database metadata.
- Do not restore ARCO entrypoints, dependencies, artifacts, or runtime branches without an approved design and dedicated tests.

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
- controlled PostgreSQL-to-in-memory modelling access.

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

Logical partition identity is the deterministic `request_key`, derived from the provider, dataset, date range, variable set, and bounding box.

Observation identity for database UPSERT:

```text
daily_climate_id + valid_time + latitude + longitude
```

An unchanged database load may be skipped only when the existing `request_key`,
transformed artifact path/checksum, and observation count all match the current
Parquet partition. Any mismatch must fall back to the normal transactional
metadata and observation UPSERT path.

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

## Daily Climate Repository Contract

Implementation:

```text
pipeline/daily_climate_repository.py
```

Tests:

```text
tests/unit/test_daily_climate_repository.py
```

The repository provides controlled and validated read-only access from canonical PostgreSQL daily climate observations to an in-memory Pandas DataFrame.

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

The repository must remain read-only, preserve missing values, preserve both timestamps, enforce local-midnight and period bounds, reject empty selections and duplicate local-day/grid observations, and return deterministic ordering.

The repository must not add wet-day labels, scaling, anomaly/rolling features, final clustering features, or create duplicate physical modelling datasets.

---

## Retry and Backfill Test Coverage

Tests protect the pipeline boundaries needed before historical backfill.

Coverage should include:

- fresh/resumed RAW checkpoints and validation after resume;
- fresh/resumed transformed checkpoints and validation after resume;
- deterministic artifact paths and overwrite refusal;
- missing-artifact failures;
- ERA5-Land transformation edge cases;
- canonical database schema and indexes;
- metadata and observation UPSERT contracts;
- missing-value preservation into database NULL;
- daily-climate repository negative cases and handoff validation;
- runtime loader delegation without Airflow.

Keep this coverage verified whenever retry, resume, persistence, or backfill boundaries change.

---

## Minimal Provenance Contract

Provenance metadata tracks artifact identity without introducing transform-version or Git-commit versioning.

Rules:

- RAW NetCDF stores `downloaded_at` as file metadata.
- Parquet metadata stores `raw_artifact_path`, `raw_sha256`, `downloaded_at`, and `transformed_at`.
- PostgreSQL `daily_climate` stores `downloaded_at`, `transformed_artifact_path`, `transformed_sha256`, and `transformed_at`.
- RAW path/checksum do not belong in PostgreSQL.
- Provenance values are partition metadata, not repeated observation columns.
- A resumed transformed checkpoint must reject a changed RAW checksum.
- Development artifacts/schema created before this provenance contract should be recreated rather than migrated.

---

## Validation Rules

Do not bypass validation because an artifact or database row already exists.
Scientific validation takes precedence over generic row-count checks.

---

## Testing

Follow `tests/AGENTS.md`.

Tests must be pytest-based, deterministic, offline by default, small, readable, and independent from Airflow for core pipeline behavior.

---

## Forbidden Actions

- Do not overwrite RAW checkpoints.
- Do not restore or combine inactive provider artifacts with the CDS ERA5-Land pipeline.
- Do not change provider variable names silently.
- Do not change unit/time semantics silently.
- Do not bypass validation on resumed artifacts.
- Do not return database loading to blind append behavior.
- Do not use any foreign key other than `daily_climate_id` for canonical daily-climate details.
- Do not create a duplicate daily-climate modelling dataset without a demonstrated need.
- Do not drop `valid_time` or rename `valid_time_local` to generic `date`.
- Do not treat missing values as zero.
- Do not encode wet-day or clustering semantics into canonical daily climate data.
- Do not make the daily-climate read path create database objects.
- Do not place scientific transformations in future DAG files.
- Do not pass full datasets through future XCom.

---

## Practical Decision Order

When in doubt, prefer:

1. scientific correctness;
2. immutable RAW source truth;
3. deterministic partition identity;
4. explicit provider, dataset, and spatial identity;
5. explicit units and timestamps;
6. validation before downstream use;
7. safe retry/resume;
8. idempotent persistence;
9. canonical daily climate data as modelling source;
10. thin Airflow orchestration;
11. simple implementation over unnecessary abstraction.
