# Testing

This document explains how testing is organized for the ERA5-Land ETL project, what each test level and domain protects, and how to run the current suites.

For installation and environment setup, see [`/README.md`](../README.md). For Airflow operation and recovery, see [`/orchestration/README.md`](../orchestration/README.md). Detailed developer-facing testing rules—including production-object use, fixture ownership, mocking boundaries, and test-module documentation—are kept in [`tests/AGENTS.md`](./AGENTS.md).

## Testing Structure

Tests are organized by **test level first** and **domain second**:

```text
tests/
├── unit/
│   ├── pipeline/
│   ├── orchestration/
│   └── operations/
└── integration/
    ├── pipeline/
    └── orchestration/
```

The three domains mean:

- **pipeline** — scientific ETL, artifacts, canonical climate persistence, and read-only climate access;
- **orchestration** — Airflow scheduling, campaign selection, progress, pacing, retry/deferral, reporting, and orchestration policy;
- **operations** — maintenance and administrative scripts such as reset, cleanup, and database recreation.

A domain directory is created only when tests actually exist for that level. There is currently no `tests/integration/operations/` directory.

Scientific correctness is part of software correctness: tests must protect the meaning of local daily climate observations, not only whether code executes successfully.

## Unit Tests

Unit tests live under:

```text
tests/unit/
```

They are expected to be fast, deterministic, and offline-first.

### Pipeline unit tests

```text
tests/unit/pipeline/
```

They cover request construction, CDS normalization, transformation, validation, artifacts, idempotency decisions, database contracts without a live database, runtime boundaries, and read-only climate-data access.

### Orchestration unit tests

```text
tests/unit/orchestration/
```

They cover DAG structure, scheduled/manual behavior, campaign selection, monthly mapping, retry/deferral, progress wiring, pacing policy, reporting, concurrency, pools, and task metadata handoff.

### Operations unit tests

```text
tests/unit/operations/
```

They cover maintenance and administrative commands such as data cleanup, Airflow runtime/log reset, database recreation, and combined reset behavior.

Routine unit tests should not require live CDS or live PostgreSQL.

## Integration Tests

Integration tests live under:

```text
tests/integration/
```

They use real external behavior only where mocks are not sufficient.

### Pipeline integration tests

```text
tests/integration/pipeline/
```

They protect real PostgreSQL climate persistence behavior such as idempotent repeated loads and transaction rollback.

### Orchestration integration tests

```text
tests/integration/orchestration/
```

They protect real PostgreSQL coordination for campaign-progress reconciliation, submission pacing/locking, and orchestration database bootstrap.

Integration tests use the isolated `TEST_POSTGRES_*` configuration and must not fall back to development or production data.

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
- Airflow scheduling, retry, pacing, progress, and recovery rules behave as intended;
- maintenance commands remain guarded and predictable.

Detailed invariants such as exact database identities, provenance fields, and persistence rules belong in `tests/AGENTS.md`.

## Idempotency and Recovery Examples

If January 2024 has already been loaded and the same logical request is processed again:

```text
first run  → canonical January partition stored
second run → existing partition validated/reused
result     → no duplicate daily observations
```

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

If a valid RAW or transformed artifact already exists, tests verify that the pipeline resumes and validates it rather than silently replacing it.

## Running Tests

Activate the project environment first:

```bash
source activate-airflow.sh -q
```

Run all unit tests:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

Run one unit domain when a focused check is useful:

```bash
python -m pytest tests/unit/pipeline -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit/orchestration -q

python -m pytest tests/unit/operations -q
```

Run all PostgreSQL integration tests:

```bash
python -m pytest tests/integration -q
```

Or run by integration domain:

```bash
python -m pytest tests/integration/pipeline -q
python -m pytest tests/integration/orchestration -q
python -m pytest tests/integration/geospatial -q
```

Integration tests use the `TEST_POSTGRES_*` variables from `env-example` and must target a dedicated isolated PostgreSQL test database. Root `database/` owns climate/spatial DDL and PostGIS bootstrap; see `tests/AGENTS.md` for fixture safeguards. Use `python -m pytest tests/integration --collect-only -q` to verify discovery without database writes. CLI setup commands are described in the root `README.md`.

The README intentionally does not keep a permanent numeric pass-count baseline because the suite changes over time.

## Test Style

- Use `pytest`.
- Prefer small, deterministic fixtures.
- Use production configuration when configuration itself is under test; use explicit minimal fixtures when testing logic or algorithms.
- Test data may be constructed, but do not recreate the implementation being tested.
- Keep routine unit tests offline.
- Mock external systems rather than internal scientific logic.
- Use explicit expected values for climate formulas, units, and timestamps.
- Include negative cases for validation, persistence, retry, recovery, and guarded operations.
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
