# Pipeline Testing

This document defines the testing strategy for the current ERA5-Land ETL and modelling-data access pipeline.

Current path:

```text
CDS ERA5-Land hourly RAW
→ validation
→ RAW NetCDF checkpoint
→ UTC+7 daily transformation
→ transformed validation
→ Parquet checkpoint
→ idempotent PostgreSQL load
→ daily_climate / daily_climate_detail
→ controlled in-memory modelling access
```

Core behavior must remain testable without Apache Airflow.

---

## Testing Goals

Tests must protect:

- provider variable naming;
- CDS normalization;
- hourly RAW integrity;
- temporal/spatial validation;
- UTC+7 daily semantics;
- accumulated-variable formulas;
- daily means and units;
- transformed schema;
- deterministic artifact naming;
- RAW immutability;
- checkpoint/resume;
- deterministic request identity;
- idempotent PostgreSQL loading;
- canonical daily-climate table and foreign-key names;
- controlled database-to-memory access;
- artifact provenance and checksum integrity;
- atomic NetCDF and Parquet writes;
- isolated PostgreSQL idempotency and transaction rollback.

Scientific correctness is part of software correctness.

---

## Locked Scientific Contract

Provider variables:

```text
tp
t2m
d2m
swvl1
ssrd
```

Transformed schema:

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

Output units:

```text
tp    → mm/day
t2m   → degC
d2m   → degC
swvl1 → m3/m3
ssrd  → J/m2/day
```

Do not drop `valid_time`, rename `valid_time_local` to a generic date field, or replace missing values with zero unless a specific contract explicitly requires it.

---

## PostgreSQL Contract Under Test

Canonical tables:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

Canonical detail foreign key:

```text
daily_climate_id
```

The development database supports only the current schema. Tests must not preserve compatibility behavior for discarded development schemas.

`daily_climate.request_key` identifies a logical request partition.

Partition provenance stored in `daily_climate`:

```text
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

RAW path/checksum are stored in Parquet file metadata rather than PostgreSQL.

Observation uniqueness:

```text
daily_climate_id + valid_time + latitude + longitude
```

Retry behavior must use UPSERT semantics rather than duplicate append.

Provider-to-database mapping:

```text
tp     → tp_daily_total_mm
t2m    → t2m_daily_mean_c
d2m    → d2m_daily_mean_c
swvl1  → swvl1_daily_mean_m3_m3
ssrd   → ssrd_daily_total_j_m2
```

---

## Unit Test Responsibilities

Current modules include:

```text
test_config_requests.py
test_daily_climate_repository.py
test_database.py
test_extract.py
test_extract_cds_normalization.py
test_load.py
test_runtime.py
test_storage.py
test_transform.py
test_utils_request_validation.py
test_validation.py
```

`test_load.py` should protect:

- canonical `daily_climate` / `daily_climate_detail` configuration;
- `daily_climate_id` foreign-key naming;
- provider-to-database column mapping;
- deterministic request-key UPSERT;
- idempotent metadata behavior;
- UPSERT-oriented observation persistence.

`test_daily_climate_repository.py` should protect:

- explicit source/dataset/period/bbox selection;
- canonical table joins;
- required DataFrame columns;
- deterministic ordering;
- duplicate local-day grid detection;
- preservation of missing values;
- read-only PostgreSQL-to-DataFrame behavior.

Routine unit tests must not require a live PostgreSQL server.

---

## Checkpoint and Idempotency Regression Rules

Checkpoint behavior:

```text
same logical request → same artifact path
RAW existing         → resume, never overwrite silently
Parquet existing     → resume and validate
```

Database idempotency:

```text
same request_key → same logical partition identity
same observation key → UPSERT, not duplicate append
```

When an isolated database integration test is used, loading the same partition twice should leave zero duplicate observation keys.

---

## Modelling-Data Access Contract

Controlled modelling-data access is defined as:

> controlled and validated access from canonical PostgreSQL daily climate observations to an in-memory modelling dataset, without creating a duplicate physical dataset and without changing meteorological semantics.

Repository tests should verify:

- selection is explicit by source/dataset/period/spatial or partition identity as appropriate;
- returned data preserves both `valid_time` and `valid_time_local`;
- daily statistic columns retain their meanings and units;
- missing values remain distinguishable from observed zero;
- canonical climate truth is not mutated by the read path;
- no extra physical `daily_feature_dataset` is created merely to duplicate existing rows.

The five meteorological variables are candidate parameters only. Tests must not assume all five are final clustering inputs.

Modelling-data access must remain separate from:

```text
wet-day identification
feature contribution / subset evaluation
```

---

## Retry, Resume, and Provenance Verification

The suite protects the retry, resume, validation, storage, database, and modelling-data boundaries needed before historical backfill.

Provenance coverage protects:

- SHA-256 byte-change detection;
- preservation of RAW `downloaded_at`;
- Parquet RAW provenance metadata;
- rejection of transformed resume after RAW checksum change;
- PostgreSQL provenance columns;
- metadata UPSERT of transformed artifact path/checksum;
- runtime artifact handoff into transform/resume and database load.

The current verified baseline is:

```text
90 unit tests passed
2 PostgreSQL integration tests passed
```

The integration result requires an explicitly configured test database. It is not part of the default offline unit-test run.

---

## PostgreSQL Integration Tests

`tests/integration/test_postgresql_idempotency.py` uses the real loader and an isolated schema to verify two database behaviors:

1. loading the same logical partition twice does not create duplicate observations;
2. a detail-row failure rolls back both the detail rows and the metadata UPSERT in the same transaction.

Configuration is loaded from `.env` through:

```text
TEST_POSTGRES_HOST
TEST_POSTGRES_PORT
TEST_POSTGRES_DATABASE
TEST_POSTGRES_USER
TEST_POSTGRES_PASSWORD
```

If these variables are absent, the integration module is skipped rather than connecting to a development or production database implicitly.

---

## Known NetCDF Runtime Warning

The project retains:

```text
numpy==2.5.1
netCDF4==1.7.4
```

Under pytest, importing the NetCDF backend can emit an upstream `numpy.ndarray size changed` runtime warning. It remains visible by project decision because changing either version did not remove the warning consistently, `pip check` remains clean, and NetCDF tests still pass.

Do not add a broad `RuntimeWarning` filter. Warnings produced by scientific validation, such as negative accumulations, must remain observable.

---

## Test Style

- Use `pytest`.
- Keep fixtures small, deterministic, and readable.
- Prefer offline execution.
- Mock external systems, not internal scientific logic.
- Use explicit expected values for formulas, units, and timestamps.
- Include negative cases for validation and persistence behavior.
- Use temporary directories for generated artifacts.
- Do not write tests into the real project `data/` directory.

---

## Airflow Testing

Airflow tests begin when DAGs are implemented.

They should cover orchestration only:

- DAG import/parse;
- task IDs and dependencies;
- schedule/data intervals;
- retries;
- concurrency/pools;
- small artifact/control metadata handoff.

Do not duplicate scientific transformation or modelling-data access tests inside DAG tests.

---

## Running Tests

```bash
python -m pytest tests/unit -q
python -m pytest tests/integration -v
```

Focused ERA5-Land pipeline suite:

```bash
python -m pytest \
  tests/unit/test_utils_request_validation.py \
  tests/unit/test_extract_cds_normalization.py \
  tests/unit/test_transform.py \
  tests/unit/test_validation.py \
  tests/unit/test_storage.py \
  tests/unit/test_database.py \
  tests/unit/test_load.py \
  tests/unit/test_runtime.py \
  tests/unit/test_daily_climate_repository.py -v
```

Latest verified baseline:

```text
90 unit tests passed
2 PostgreSQL integration tests passed with TEST_POSTGRES_* configured
```

---

## Definition of Done

New behavior should normally have:

```text
implementation
+ focused unit tests
+ negative cases where relevant
+ integration/runtime verification for external I/O
+ documentation update when a data contract changes
```

Do not mark a persistence or scientific behavior complete from a manual run alone.
