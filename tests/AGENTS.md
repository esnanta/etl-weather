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
→ P15 in-memory modelling access
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

Legacy names:

```text
weather_dataset
weather_dataset_detail
dataset_id
```

may appear only in migration tests. Do not use them as the active schema contract.

Logical partition identity uses deterministic `request_key`.

Observation identity:

```text
daily_climate_id + valid_time + latitude + longitude
```

Database writes must remain idempotent UPSERT operations.

## P10/P11 Rules

Protect:

```text
same logical request → deterministic artifact path
RAW checkpoint       → immutable
existing checkpoint  → resume and validate
same request_key     → same partition identity
same observation key → UPSERT, not duplicate append
```

Do not weaken these tests to make later modelling work pass.

## P15 Rules

P15 means controlled, validated read access from canonical PostgreSQL daily climate observations to an in-memory modelling dataset.

P15 tests must verify that:

- source/dataset/period/spatial or partition selection is explicit;
- both time fields are preserved;
- daily climate values retain their database meanings and units;
- missing values remain missing;
- read access does not mutate canonical climate truth;
- no duplicate physical feature table/file is created merely to copy existing data.

The five meteorological variables are candidate parameters, not a locked clustering feature set.

Do not add wet-day labels, scaling, anomaly features, rolling features, or clustering semantics to P15 tests. Those belong to later stages.

## Test Structure

Use:

```text
tests/unit/
tests/integration/
```

Unit tests should be small, deterministic, offline-first, and focused on one responsibility.

Integration tests should use isolated resources and real pipeline components where practical.

## Fixtures

Prefer:

```text
1–3 local days
1–a few grid cells
small xarray.Dataset
small pandas.DataFrame
explicit UTC/local timestamps
manually verifiable numeric values
```

Use temporary directories for artifact tests.

Do not require live CDS/ARCO access in routine tests.

## Scientific Testing

Expected values must be traceable to explicit rules:

- accumulation formula;
- unit conversion;
- timezone offset;
- grid resolution;
- schema contract.

Do not settle for non-empty/truthy assertions when a precise value can be asserted.

Negative tests are required where meaningful, including missing hourly steps, wrong grid resolution, wrong UTC/local offset, duplicate observations, unsupported variables, and RAW overwrite attempts.

## Module Guidance

- `test_config_requests.py`: typed source/dataset request configuration.
- `test_extract.py`: extraction helpers without live provider access.
- `test_extract_cds_normalization.py`: API names → provider codes and `valid_time` normalization.
- `test_utils_request_validation.py`: request validation, signatures, deterministic request keys.
- `test_transform.py`: daily aggregation, timestamps, units, source metadata removal.
- `test_validation.py`: temporal/spatial positive and negative contracts.
- `test_storage.py`: deterministic artifacts, RAW immutability, checkpoint resume.
- `test_load.py`: canonical daily-climate table names, `daily_climate_id`, column mapping, metadata reuse, UPSERT preparation.

P15 access tests should be placed in a dedicated test module when the modelling data-access component is introduced rather than overloading transform tests.

## Airflow Testing

When DAGs are added, test orchestration only:

- DAG parsing;
- task graph;
- schedule/data intervals;
- retries;
- concurrency/pools;
- small metadata/artifact handoff.

Do not duplicate pipeline scientific tests inside DAG tests.

## Current Priority

```text
[x] P9  provider variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[ ] P15 controlled daily-climate modelling access
```

New tests must extend this baseline without silently changing it.
