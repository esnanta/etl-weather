# Tests AGENTS

This document defines testing rules for `tests/` and complements `../AGENTS.md`.

Authoritative references:

- `../AGENTS.md`
- `../docs/AIRFLOW_BEST_PRACTICE.md`
- `../docs/AIRFLOW_CODING_GUIDELINES.md`
- `README_TESTING.md`

## Scope

Core tests protect the ERA5-Land ETL pipeline and controlled daily-climate repository before Airflow orchestration and long historical backfill.

Current path:

```text
CDS ERA5-Land hourly RAW
-> UTC+7 daily transformed data
-> PostgreSQL daily_climate / daily_climate_detail
-> read-only in-memory modelling access
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

Database observation identity:

```text
daily_climate_id + valid_time + latitude + longitude
```

Database writes must remain idempotent UPSERT operations.
An unchanged partition may skip database writes only when request identity,
transformed artifact path/checksum, and observation count all match. Any mismatch
must fall back to the transactional metadata and observation UPSERT path.

## Daily-Climate Repository Rules

Implementation:

```text
pipeline/repositories/daily_climate.py
tests/unit/test_daily_climate_repository.py
```

Repository tests must protect explicit source/dataset/period/bbox selection, canonical tables, both time columns, missing values, deterministic ordering, local-midnight semantics, period bounds, duplicate local-day/grid rejection, and read-only behavior.

Do not add wet-day labels, scaling, anomaly features, rolling features, or clustering semantics to repository tests.

## Retry and Backfill Hardening Rules

Tests must protect the boundaries that become critical during retry/backfill:

- fresh versus resumed RAW checkpoints;
- validation after RAW resume;
- fresh versus resumed transformed checkpoints;
- validation after transformed resume;
- deterministic artifact paths;
- refusal to overwrite checkpoints silently;
- missing-artifact failures;
- canonical database schema and indexes;
- metadata and observation UPSERT identities;
- unchanged-partition load skipping and checksum/path/observation-count mismatch fallback;
- missing-value preservation into database NULL;
- transformation failure for missing inputs or incomplete local days;
- daily-climate repository negative cases and deterministic handoff.

`tests/unit/test_runtime.py` owns orchestration-boundary behavior without Airflow.
`tests/unit/test_database.py` owns canonical PostgreSQL DDL contracts without requiring live PostgreSQL.

## Provenance Rules

Provenance tests protect minimal artifact traceability:

- SHA-256 changes when artifact bytes change;
- RAW `downloaded_at` survives save/resume;
- transformed Parquet metadata contains RAW path/checksum plus download/transform timestamps;
- transformed resume rejects a changed RAW checksum;
- `daily_climate` DDL includes download time and transformed artifact path/checksum;
- metadata UPSERT persists transformed provenance;
- runtime passes RAW artifact into transform/resume and transformed artifact into the loader.

Do not introduce transform-version or Git-commit provenance tests. RAW checksum/path remain Parquet metadata rather than PostgreSQL columns.

---

## Test Structure

Use:

```text
tests/unit/
tests/integration/
```

Unit tests should be small, deterministic, offline-first, and focused on one responsibility.
Integration tests should use isolated resources and real pipeline components where practical.

Prefer 1-3 local days, one or a few grid cells, explicit UTC/local timestamps, deterministic numeric values, and explicit missing-value cases.

Scientific expected values must be traceable to accumulation formulas, unit conversion, timezone offset, grid resolution, or schema contracts. Prefer exact assertions over truthy/non-empty assertions.

## Module Guidance

- `test_config_requests.py`: typed request configuration.
- `test_extract.py`: extraction helpers without live provider access.
- `test_extract_cds_normalization.py`: API names -> provider codes and `valid_time` normalization.
- `test_request.py`: request validation, signatures, deterministic request keys.
- `test_transform.py`: aggregation, timestamps, units, metadata removal, incomplete-day failures.
- `test_validation.py`: temporal/spatial positive and negative contracts.
- `test_storage.py`: RAW/Parquet deterministic artifacts, immutability, provenance metadata/checksum, resume, missing artifacts, and atomic-write cleanup.
- `test_database.py`: canonical schema, provenance fields, foreign key, indexes and observation identity.
- `test_load.py`: column mapping, NULL handling, provenance metadata UPSERT,
  observation UPSERT, and unchanged-partition load skipping.
- `test_runtime.py`: extract/transform resume branches, resumed validation and loader boundary.
- `test_daily_climate_repository.py`: modelling-data selection, SQL contract, validation, ordering and missing values.
- `integration/test_postgresql_idempotency.py`: real PostgreSQL idempotency and transaction rollback using an isolated test schema.

## Dependency and Warning Policy

The active dependency set supports CDS ERA5-Land NetCDF input, Parquet output, and PostgreSQL persistence. Do not restore ARCO-only Zarr, Google Cloud Storage, Dask, or async HTTP dependencies unless an explicitly approved source path requires them again.

The pinned `numpy==2.5.1` and `netCDF4==1.7.4` combination can emit this upstream runtime warning when the NetCDF backend is imported under pytest:

```text
numpy.ndarray size changed, may indicate binary incompatibility.
Expected 16 from C header, got 96 from PyObject
```

The warning is intentionally left visible. Do not silence all `RuntimeWarning` messages or downgrade dependencies solely to hide it. NetCDF behavior remains protected by extractor and storage tests; investigate again if it becomes an exception, corrupts a round trip, or fails on a supported environment.

## Airflow Testing

When DAGs are added, test orchestration only: DAG parsing, task graph, schedule/data intervals, retries, concurrency/pools, and small metadata/artifact handoff.

Do not duplicate scientific or daily-climate repository tests inside DAG tests.

## Verified Coverage

```text
[x] provider variable naming
[x] checkpoint / resume
[x] idempotent database load
[x] unchanged-partition database-write skip
[x] temporal validation
[x] spatial validation
[x] unit conversion
[x] controlled daily-climate modelling access
[x] retry and backfill boundary coverage
[x] minimal artifact provenance
[x] ARCO-only dependency removal
```

Latest verified baseline:

```text
100 unit tests passed
2 PostgreSQL integration tests passed
```
