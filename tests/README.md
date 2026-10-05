# Testing

This document explains how testing is organized for the ERA5-Land ETL project, what each test layer protects, and how to run the current test suites.

For installation and environment setup, see [`/README.md`](../README.md). For Airflow operation and recovery, see [`/orchestration/README.md`](../orchestration/README.md). Detailed developer-facing testing rules are kept in [`tests/AGENTS.md`](./AGENTS.md).

## Testing Strategy

The project uses three main test layers:

```text
tests/unit/
      ↓
fast, offline behavior

tests/airflow_orchestration/
      ↓
scheduling and orchestration policy

tests/integration/
      ↓
real isolated PostgreSQL behavior
```

Unit tests and Airflow orchestration tests should not require live CDS requests. PostgreSQL integration tests use a dedicated test database configured through `TEST_POSTGRES_*`.

Scientific correctness is part of software correctness: tests must protect the meaning of local daily climate observations, not only whether code executes successfully.

## What the Tests Protect

The suite should give confidence that:

- ERA5-Land requests use the expected variables and spatial/temporal inputs;
- hourly source data is validated before transformation;
- UTC+7 daily observations are calculated consistently;
- daily units and missing values keep their intended meaning;
- RAW and transformed artifacts can be resumed safely;
- repeated processing does not create duplicate canonical observations;
- database failures do not leave partially committed climate data;
- read-only modelling access does not change canonical climate observations;
- Airflow scheduling, retry, pacing, progress, and recovery rules behave as intended.

Detailed invariants such as exact database identities, provenance fields, and persistence rules belong in `tests/AGENTS.md`.

## Unit Tests

Unit tests live in:

```text
tests/unit/
```

They cover pipeline and maintenance behavior that can be tested quickly and deterministically without live CDS or PostgreSQL.

Typical responsibilities include:

- request construction and validation;
- CDS normalization;
- local-day transformation and units;
- source and transformed-data validation;
- deterministic artifact storage and resume behavior;
- database schema/configuration logic without a live database;
- load/idempotency decisions through isolated test doubles;
- read-only climate-data access behavior;
- logging, pacing helpers, and local maintenance scripts.

Unit tests should be small, readable, and focused on one responsibility.

## Airflow Orchestration Tests

Airflow-specific tests live in:

```text
tests/airflow_orchestration/
```

They protect orchestration behavior rather than scientific calculations.

Typical responsibilities include:

- DAG parsing and task dependencies;
- scheduled versus manual mode;
- campaign selection and configuration;
- monthly mapping and start-slot policy;
- retry and deferral behavior;
- durable progress and reconciliation wiring;
- CDS submission pacing policy;
- reporting and monitoring behavior;
- concurrency, pools, and task metadata handoff.

Scientific transformation behavior should stay covered by unit tests instead of being duplicated in DAG tests.

## PostgreSQL Integration Tests

PostgreSQL integration tests live in:

```text
tests/integration/
```

These tests use real PostgreSQL behavior with the isolated `TEST_POSTGRES_*` configuration.

They protect areas that cannot be proven reliably with mocks alone, including:

- repeated loads remaining idempotent;
- transaction rollback after a database failure;
- campaign-progress reconciliation against persisted climate evidence;
- CDS submission pacing coordination using the PostgreSQL clock and locking;
- database bootstrap being safe to run on both a fresh and an already-initialized test database.

The integration database must be separate from development or production data. `TEST_POSTGRES_DATABASE` is expected to identify a test database.

## Idempotency and Recovery Examples

### Reprocessing the same climate partition

If January 2024 has already been loaded and the same logical request is processed again:

```text
first run  → canonical January partition stored
second run → existing partition validated/reused
result     → no duplicate daily observations
```

### Database failure during a load

If a database error occurs while writing a partition:

```text
metadata write + observation write
            ↓
         failure
            ↓
transaction rolled back
            ↓
no partial canonical partition
```

### Resuming artifacts

If a valid RAW or transformed artifact already exists, tests verify that the pipeline resumes and validates it rather than silently replacing it.

The exact technical rules behind these behaviors are documented in `tests/AGENTS.md`.

## Running Tests

Run unit and Airflow orchestration regression tests with scheduling disabled:

```bash
source activate-airflow.sh

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit tests/airflow_orchestration -q
```

Run the isolated PostgreSQL integration tests with:

```bash
source activate-airflow.sh

python -m pytest \
  tests/integration/test_postgresql_idempotency.py \
  tests/integration/test_pipeline_progress_reconciliation.py \
  tests/integration/test_submission_pacing.py \
  tests/integration/test_orchestration_database_bootstrap.py \
  -q
```

Integration tests use the `TEST_POSTGRES_*` variables from `env-example`.

The README intentionally does not keep a permanent "N tests passed" baseline because the number changes as the suite evolves. The reproducible command and a clean result are the important contract.

## Test Style

- Use `pytest`.
- Prefer small, deterministic fixtures.
- Keep routine unit tests offline.
- Mock external systems rather than internal scientific logic.
- Use explicit expected values for climate formulas, units, and timestamps.
- Include negative cases for validation, persistence, retry, and recovery behavior.
- Use temporary directories for generated artifacts.
- Never write test artifacts into the real project `data/` directory.

## Definition of Done

A behavior change is normally complete when it has:

```text
implementation
+ focused automated tests
+ negative cases where relevant
+ integration verification for real external I/O where needed
+ documentation update when a public contract changes
```

A manual successful run alone is not sufficient evidence for persistence or scientific behavior.
