# Tests AGENTS

This document defines technical testing rules for `tests/` and complements `../AGENTS.md`.

Human-facing testing strategy and commands belong in `README.md`. This file keeps lower-level invariants and development rules that must remain true when tests are added or changed.

Authoritative references:

- `../AGENTS.md`
- `../docs/AIRFLOW_BEST_PRACTICE.md`
- `../docs/AIRFLOW_CODING_GUIDELINES.md`
- `README.md`

## Scope

Tests protect the ERA5-Land ETL pipeline, canonical PostgreSQL persistence, controlled daily-climate read access, Airflow orchestration policy, and guarded operational tooling.

Scientific pipeline behavior must remain testable without Airflow. Routine unit tests must not require live CDS or live PostgreSQL.

## Test Ownership

Tests are organized by **level first** and **domain second**:

```text
tests/unit/pipeline/
    fast/offline scientific ETL, storage, persistence decisions, and data access

tests/unit/orchestration/
    Airflow scheduling, mapping, retry/deferral, pacing, progress, reporting,
    DAG policy, concurrency, pools, and control-metadata behavior

tests/unit/operations/
    reset, cleanup, recreation, and other guarded maintenance behavior

tests/integration/pipeline/
    isolated real-PostgreSQL climate persistence

tests/integration/orchestration/
    isolated real-PostgreSQL orchestration coordination
```

Do not create an empty domain directory only for symmetry. Do not maintain a complete filename catalog here.

Shared deterministic environment setup belongs in `tests/conftest.py`. Add narrower `conftest.py` files only when fixture scope genuinely requires them.

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
climate_data.t_daily_climate
climate_data.t_daily_climate_detail
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

Repository tests must protect explicit source/dataset/period/bbox selection, canonical-table reads, both time columns, missing-value preservation, deterministic ordering, local-day/period bounds, duplicate local-day/grid rejection, and read-only behavior.

Do not add wet-day labels, scaling, anomaly features, rolling features, clustering semantics, or other modelling decisions to repository tests.

## Pipeline Progress and Pacing Rules

The durable operational tables are:

```text
climate_data.t_pipeline_progress
climate_data.t_pipeline_progress_detail
climate_data.t_submission_pacing
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

## Test Construction Principles

**Test boleh membuat data, tetapi jangan membuat ulang implementation-under-test.**

Tests may construct deterministic input data, fixtures, timestamps, DataFrames, xarray datasets, temporary files, and instances of production models. They must not recreate the production algorithm, persistence semantics, campaign policy, or other implementation that the test claims to verify and then assert against that recreated behavior.

Use production domain objects when an appropriate production type already exists. Do not introduce `SimpleNamespace`, ad-hoc dictionaries, or test-only domain classes merely to imitate a production model.

**Gunakan production configuration ketika configuration adalah subject-under-test. Gunakan explicit minimal fixtures ketika logic/algorithm adalah subject-under-test.**

In practice:

- configuration-contract tests should use the production configuration or the production factory that resolves it, so accidental configuration changes are observable;
- algorithm, scientific, validation, storage, and edge-case tests should use the smallest explicit fixture that makes the behavior understandable and independent from unrelated deployment configuration;
- deliberately invalid or alternate configurations should be written explicitly in the test;
- do not reuse deprecated project configuration values as generic fixtures because their historical meaning can make the test ambiguous;
- do not create one global test fixture merely to remove repeated literals when individual tests need different semantic inputs.

Examples:

- a test that verifies the configured Aceh retrieval extent should use `ACEH_BBOX_COORDINATES` or `build_aceh_bbox()`;
- a test that verifies a 0.1° two-by-two grid should use an explicit minimal two-by-two bounding box;
- a test that verifies different bounding boxes produce different artifact identities should define two intentionally different bounding boxes.

Mocks, fakes, stubs, and monkeypatches belong at justified external or collaborator boundaries. They may isolate CDS/network calls, PostgreSQL when database semantics are not the subject, Airflow runtime context, OS measurements, clocks, or failure injection. They must not mock away the scientific, persistence, scheduling, or policy logic being claimed by the test.

## Test File Documentation

Every new test module, and every existing test module materially revised during a test-quality refactor, must have a module-level docstring that states:

1. the production behavior or contract being tested;
2. the production object, function, class, or module under test;
3. any mocked, stubbed, monkeypatched, or otherwise isolated boundary and why that isolation is legitimate;
4. where stronger integration coverage owns behavior that the unit test intentionally does not prove.

Keep this docstring concise and specific to the file. Do not use it as a duplicate copy of `README.md` or this AGENTS file.

## Test Data and Paths

Prefer one to three local days, one or a few grid cells, explicit UTC/local timestamps, deterministic numeric values, explicit missing-value cases, and temporary directories for generated artifacts.

Do not write test artifacts into the real project `data/` directory.

Tests that need repository-level paths should use the shared path definition in `tests/_paths.py` rather than deriving the repository root from a fragile number of `Path.parents`.

Mock external systems where possible, but do not mock away the scientific logic that the test is intended to prove.

## Airflow Testing Rules

Airflow tests under `tests/unit/orchestration/` should cover orchestration only: DAG parsing/graph policy, run mode, schedule behavior, mapped-month construction, retry/deferral, campaign selection/reconciliation wiring, submission pacing policy, pools/concurrency/timeouts, reporting/monitoring, and small control metadata.

Do not duplicate climate transformation or daily-climate repository tests inside orchestration tests.

Scheduled tests should normally set or assume `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` unless schedule behavior itself is under test.

## Operations Testing Rules

Tests under `tests/unit/operations/` protect guarded maintenance commands.

They must verify that destructive actions are denied in inappropriate environments, inspect/dry modes do not mutate state, confirmation/preflight rules are respected, and composed reset operations stop safely after earlier failures.

Operations tests may execute repository scripts in subprocesses when direct-script behavior is part of the contract. Use `tests._paths.REPOSITORY_ROOT` for repository-level paths.

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

- place the test at the lowest appropriate level and correct domain;
- include negative cases where failure behavior is part of the contract;
- use integration coverage when correctness depends on real PostgreSQL behavior;
- avoid duplicating the same contract across multiple test levels/domains;
- keep unit assertions focused on behavior that the unit level can genuinely prove;
- prefer PostgreSQL integration coverage when transaction, locking, conflict, or persisted-state semantics are the contract;
- keep or add the required module-level test docstring when a test module is materially revised;
- update `README.md` when testing strategy or canonical commands change;
- update this file when a technical testing invariant changes.

Do not treat a manual successful ETL or Airflow run as a substitute for automated coverage.
