# Tests AGENTS

This document defines technical testing rules for `tests/` and complements `../AGENTS.md`.

Human-facing testing strategy and commands belong in `README.md`. This file keeps the lower-level invariants and development rules that must remain true when tests are added or changed.

Authoritative references:

- `../AGENTS.md`
- `../docs/AIRFLOW_BEST_PRACTICE.md`
- `../docs/AIRFLOW_CODING_GUIDELINES.md`
- `README.md`

## Scope

Tests protect the ERA5-Land ETL pipeline, canonical PostgreSQL persistence, controlled daily-climate read access, and Airflow orchestration policy.

Scientific pipeline behavior must remain testable without Airflow. Routine unit tests must not require live CDS or live PostgreSQL.

## Test Ownership

Use directory-level ownership:

```text
tests/unit/
    fast, deterministic, offline-first pipeline and maintenance behavior

tests/airflow_orchestration/
    Airflow scheduling, mapping, retry/deferral, pacing, progress, reporting,
    DAG policy, concurrency, pools, and small control-metadata handoff

tests/integration/
    isolated real-PostgreSQL behavior
```

Do not maintain a complete filename catalog here. Name an individual test module only when it has a uniquely important canonical ownership that would otherwise be ambiguous.

## Locked Scientific Contracts

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

Do not drop either timestamp or rename `valid_time_local` to a generic `date`.

Output units:

```text
tp    = mm/day
t2m   = degC
d2m   = degC
swvl1 = m3/m3
ssrd  = J/m2/day
```

Missing values are not zero.

Expected scientific values must be traceable to accumulation rules, unit conversion, timezone offset, grid behavior, or an explicit schema contract. Prefer exact assertions over truthy/non-empty assertions.

## PostgreSQL Contracts

Canonical climate tables:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

Canonical detail foreign key:

```text
daily_climate_id
```

Observation identity:

```text
daily_climate_id + valid_time + latitude + longitude
```

Database writes must remain idempotent.

An unchanged partition may skip database writes only when request identity, transformed artifact path/checksum, and expected observation count match. Any mismatch must use the normal transactional metadata and observation persistence path.

Integration tests must use the isolated `TEST_POSTGRES_*` database. They must never fall back silently to development or production PostgreSQL.

## Checkpoint, Retry, and Recovery Rules

Tests must protect:

- fresh versus resumed RAW checkpoints;
- validation after RAW resume;
- fresh versus resumed transformed checkpoints;
- validation after transformed resume;
- deterministic artifact paths;
- refusal to overwrite checkpoints silently;
- missing-artifact failures;
- missing-value preservation into database NULL;
- transformation failure for missing inputs or incomplete local days;
- idempotent retry after partial or failed work;
- database transaction rollback;
- campaign repair and progress reconciliation.

Retry/recovery tests should prove observable outcomes rather than depending unnecessarily on internal implementation details.

## Daily-Climate Repository Rules

Repository tests must protect:

- explicit source/dataset/period/bbox selection;
- canonical-table reads;
- both time columns;
- missing-value preservation;
- deterministic ordering;
- local-day/period bounds;
- duplicate local-day/grid rejection;
- read-only behavior.

Do not add wet-day labels, scaling, anomaly features, rolling features, clustering semantics, or other modelling decisions to repository tests.

## Pipeline Progress and Pacing Rules

The durable operational tables are:

```text
climate_data.pipeline_progress
climate_data.pipeline_progress_detail
climate_data.submission_pacing
```

Tests must keep progress persistence independent from scheduler cadence. Progress identity is based on configured date ranges and configuration identity rather than quarter number.

Detail evidence should protect request identity, artifact identity, validation state, extract/transform/load state, and the counts/metrics required for reconciliation.

Quarter grouping is scheduler policy. Persistence tests must remain valid if campaign grouping changes later.

Submission pacing tests must protect coordination through PostgreSQL time/locking and must distinguish provider-submission pacing from Airflow scheduler cadence.

## Provenance Rules

Provenance tests protect the minimum artifact traceability required by the current pipeline:

- SHA-256 changes when artifact bytes change;
- RAW `downloaded_at` survives save/resume;
- transformed Parquet metadata records the required RAW artifact identity;
- transformed resume rejects a changed RAW checksum;
- canonical metadata persists transformed artifact identity;
- runtime boundaries pass artifact identity correctly into transform/resume/load behavior.

Do not introduce transform-version or Git-commit provenance tests. RAW checksum/path remain Parquet metadata rather than PostgreSQL columns.

## Test Data and Fixtures

Prefer:

- one to three local days;
- one or a few grid cells;
- explicit UTC and local timestamps;
- deterministic numeric values;
- explicit missing-value cases;
- temporary directories for generated artifacts.

Do not write test artifacts into the real project `data/` directory.

Mock external systems where possible, but do not mock away the scientific logic that the test is intended to prove.

## Airflow Testing Rules

Airflow tests should cover orchestration only:

- DAG parsing and graph policy;
- run mode and schedule behavior;
- mapped-month construction;
- retries and deferral;
- campaign selection and reconciliation wiring;
- submission pacing policy;
- pools/concurrency/timeouts;
- reporting/monitoring;
- small XCom/control metadata.

Do not duplicate climate transformation or daily-climate repository tests inside DAG tests.

Scheduled tests should normally set or assume `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` unless schedule behavior itself is under test.

## Dependency and Warning Policy

The active dependency set supports CDS ERA5-Land NetCDF input, Parquet output, PostgreSQL persistence, and Airflow orchestration.

Do not restore ARCO-only Zarr, Google Cloud Storage, Dask, or async HTTP dependencies unless an explicitly approved source path requires them again.

The pinned `numpy==2.5.1` and `netCDF4==1.7.4` combination can emit the upstream runtime warning:

```text
numpy.ndarray size changed, may indicate binary incompatibility.
Expected 16 from C header, got 96 from PyObject
```

Do not silence all `RuntimeWarning` messages or change dependencies solely to hide this warning. NetCDF behavior remains protected by extraction and storage tests; investigate again if the warning becomes an exception, corrupts a round trip, or fails on a supported environment.

## Change Rules

When adding or changing behavior:

- update or add focused tests at the lowest appropriate test layer;
- include negative cases where failure behavior is part of the contract;
- use integration coverage when correctness depends on real PostgreSQL behavior;
- avoid duplicating the same contract across multiple test layers;
- update `README.md` when testing strategy or canonical commands change;
- update this file when a technical testing invariant changes.

Do not treat a manual successful ETL or Airflow run as a substitute for automated coverage.
