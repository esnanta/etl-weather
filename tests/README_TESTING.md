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
→ P15 controlled in-memory modelling access
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
- P15 controlled database-to-memory access.

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
test_extract.py
test_extract_cds_normalization.py
test_load.py
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

Routine unit tests must not require a live PostgreSQL server.

---

## P10/P11 Regression Rules

P10:

```text
same logical request → same artifact path
RAW existing         → resume, never overwrite silently
Parquet existing     → resume and validate
```

P11:

```text
same request_key → same logical partition identity
same observation key → UPSERT, not duplicate append
```

When an isolated database integration test is used, loading the same partition twice should leave zero duplicate observation keys.

---

## P15 Testing Contract

P15 is defined as:

> controlled and validated access from canonical PostgreSQL daily climate observations to an in-memory modelling dataset, without creating a duplicate physical dataset and without changing meteorological semantics.

P15 tests should verify:

- selection is explicit by source/dataset/period/spatial or partition identity as appropriate;
- returned data preserves both `valid_time` and `valid_time_local`;
- daily statistic columns retain their meanings and units;
- missing values remain distinguishable from observed zero;
- canonical climate truth is not mutated by the read path;
- no extra physical `daily_feature_dataset` is created merely to duplicate existing rows.

The five meteorological variables are candidate parameters only. Tests must not assume all five are final clustering inputs.

P15 must remain separate from:

```text
P16 wet-day identification
P17 feature contribution / subset evaluation
```

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

Do not duplicate scientific transformation or P15 data-access tests inside DAG tests.

---

## Running Tests

```bash
pytest tests/unit
pytest tests/integration
```

Focused ERA5-Land/P9-P14/P10-P11 suite:

```bash
pytest \
  tests/unit/test_utils_request_validation.py \
  tests/unit/test_extract_cds_normalization.py \
  tests/unit/test_transform.py \
  tests/unit/test_validation.py \
  tests/unit/test_storage.py \
  tests/unit/test_load.py -v
```

Baseline before the current clean daily-climate schema:

```text
31 passed
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
