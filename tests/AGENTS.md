# Tests AGENTS

This document defines testing rules for `tests/` and complements `../AGENTS.md`.

Authoritative references:

- `../AGENTS.md`
- `../Readme-pipeline.md`
- `../docs/AIRFLOW_BEST_PRACTICE.md`
- `../docs/AIRFLOW_CODING_GUIDELINES.md`
- `README_TESTING.md`

## Scope

Core tests protect the Weather ETL pipeline and P15 modelling-data access before Airflow orchestration is introduced.

Current path:

```text
CDS ERA5-Land hourly RAW
→ UTC+7 daily transformed data
→ PostgreSQL daily_climate / daily_climate_detail
→ P15 read-only in-memory modelling access
```

Core tests must not require Airflow.

## Locked Contracts

Provider variables:

```text
tp
t2m
d2m
swvl1
ssrd
```

Daily timestamps:

```text
valid_time
valid_time_local
```

Do not drop either timestamp or rename `valid_time_local` to generic `date`.

Output units:

```text
tp    = mm/day
t2m   = degC
d2m   = degC
swvl1 = m3/m3
ssrd  = J/m2/day
```

Missing values are not zero.

## PostgreSQL Contract

Canonical tables:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

Canonical detail foreign key:

```text
daily_climate_id
```

The development database supports only the current schema. Do not add tests for compatibility with discarded development schemas.

Database observation identity:

```text
daily_climate_id + valid_time + latitude + longitude
```

Database writes must remain idempotent UPSERT operations.

## P15 Rules

Implementation:

```text
pipeline/modelling_data.py
tests/unit/test_modelling_data.py
```

P15 tests must protect:

- explicit source/dataset/period/bbox selection;
- use of canonical `daily_climate` and `daily_climate_detail` tables;
- preservation of `daily_climate_id`, `valid_time`, and `valid_time_local`;
- preservation of database daily-statistic column meanings;
- missing values remaining missing;
- deterministic ordering;
- rejection of missing required columns;
- rejection of duplicate `(valid_time_local, latitude, longitude)` observations across selected partitions;
- rejection of invalid date ranges;
- read-only behavior with no database/schema creation in the P15 access path.

The five meteorological variables are candidate parameters, not a locked clustering feature set.

Do not add wet-day labels, scaling, anomaly features, rolling features, or clustering semantics to P15 tests. P16 and P17 are deferred until the historical dataset is available.

## Test Structure

Use:

```text
tests/unit/
tests/integration/
```

Unit tests should be small, deterministic, offline-first, and focused on one responsibility.

Integration tests should use isolated resources and real pipeline components where practical.

## Fixtures

Prefer 1–3 local days, one or a few grid cells, explicit UTC/local timestamps, deterministic numeric values, and explicit missing-value cases.

Use temporary directories for artifact tests. Do not require live CDS/ARCO access in routine tests.

## Scientific Testing

Expected values must be traceable to explicit rules such as accumulation formulas, unit conversion, timezone offset, grid resolution, and schema contract.

Do not settle for non-empty/truthy assertions when a precise value can be asserted.

Negative tests are required where meaningful.

## Module Guidance

- `test_config_requests.py`: typed source/dataset request configuration.
- `test_extract.py`: extraction helpers without live provider access.
- `test_extract_cds_normalization.py`: API names → provider codes and `valid_time` normalization.
- `test_utils_request_validation.py`: request validation, signatures, deterministic request keys.
- `test_transform.py`: daily aggregation, timestamps, units, source metadata removal.
- `test_validation.py`: temporal/spatial positive and negative contracts.
- `test_storage.py`: deterministic artifacts, RAW immutability, checkpoint resume.
- `test_load.py`: canonical daily-climate schema, column mapping, metadata UPSERT, observation UPSERT preparation.
- `test_modelling_data.py`: P15 selection, SQL read contract, handoff validation, ordering, and missing-value preservation.

## Airflow Testing

When DAGs are added, test orchestration only: DAG parsing, task graph, schedule/data intervals, retries, concurrency/pools, and small metadata/artifact handoff.

Do not duplicate pipeline scientific or P15 data-access tests inside DAG tests.

## Current Priority

```text
[x] P9  provider variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[~] P15 controlled daily-climate modelling access — implementation added, verification pending
[ ] P18 complete ERA5-Land test coverage
[ ] P19 provenance
```

New tests must extend this baseline without silently changing it.
