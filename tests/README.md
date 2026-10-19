# Testing

This document defines the test structure, isolation rules, and canonical test commands for the ERA5-Land ETL project.

For installation and environment setup, see [`/README.md`](../README.md). Detailed developer-facing test rules remain in [`tests/AGENTS.md`](./AGENTS.md).

## Structure

Tests are organized by test level first and domain second:

```text
tests/
├── unit/
│   ├── database/
│   ├── geospatial/
│   ├── operations/
│   ├── orchestration/
│   └── pipeline/
└── integration/
    ├── geospatial/
    ├── orchestration/
    └── pipeline/
```

## Domains

- **database** — configuration, identifiers, schema/setup behavior;
- **geospatial** — PostGIS schema, import, mapping, regional views;
- **operations** — reset, destroy, cleanup, and maintenance commands;
- **orchestration** — DAG structure, scheduling, campaign progress, pacing, retries, reporting;
- **pipeline** — request construction, extraction, validation, transformation, artifacts, persistence, and climate-data access.

A domain directory exists only where that test level is needed.

## Unit Tests

Unit tests should be fast, deterministic, and offline-first.

Run all unit tests:

```bash
python -m pytest tests/unit -q
```

Focused suites:

```bash
python -m pytest tests/unit/database -q
python -m pytest tests/unit/geospatial -q
python -m pytest tests/unit/operations -q
python -m pytest tests/unit/orchestration -q
python -m pytest tests/unit/pipeline -q
```

Routine unit tests must not require live CDS or live PostgreSQL.

## Integration Tests

Integration tests exercise real PostgreSQL behavior where mocks are not sufficient.

Run all integration tests:

```bash
python -m pytest tests/integration -q
```

Focused suites:

```bash
python -m pytest tests/integration/geospatial -q
python -m pytest tests/integration/orchestration -q
python -m pytest tests/integration/pipeline -q
```

Integration tests must use the dedicated `TEST_POSTGRES_*` configuration and must never fall back to the development or production application database.

Use collection-only when you want to verify discovery without database writes:

```bash
python -m pytest tests/integration --collect-only -q
```

## What the Tests Protect

The suite should protect:

- ERA5-Land request construction;
- hourly source validation;
- UTC+7 daily calculations;
- climate units and missing-value semantics;
- RAW and transformed checkpoint reuse;
- PostgreSQL idempotency and rollback;
- database/bootstrap contracts;
- PostGIS import and region mapping;
- regional views;
- Airflow scheduling, retry, pacing, progress, and recovery;
- guarded maintenance and destructive operations.

Scientific correctness is part of software correctness.

## Isolation Rules

- Never write test artifacts into the real project `data/` directory.
- Integration tests must target a dedicated test database.
- Do not point `TEST_POSTGRES_*` at `weather` or another shared database.
- Use temporary directories for generated files.
- Mock external systems rather than internal scientific logic.
- Use explicit expected values for formulas, units, and timestamps.

Geospatial integration tests require PostGIS to satisfy the same application-schema contract as the real database.

## Idempotency and Recovery

Repeated processing of the same logical request must not create duplicate canonical observations.

Database failures must roll back the affected transaction rather than leave partially committed climate data.

Existing valid RAW or transformed artifacts should be validated and reused rather than silently replaced.

## Test Style

- Use `pytest`.
- Prefer small deterministic fixtures.
- Include negative cases where behavior can fail.
- Keep routine unit tests offline.
- Use real external I/O only where integration coverage is required.
- Do not duplicate implementation logic inside fixtures or expected-value helpers.

## Experimental Monthly Climate

The separate `scripts.run_monthly_climate` path remains experimental.

It requires focused scientific completeness tests and isolated PostgreSQL integration coverage before database writes are accepted for normal use.

## Definition of Done

A behavior change is normally complete when it has:

```text
implementation
+ focused automated tests
+ negative cases where relevant
+ integration verification for real external I/O where needed
+ documentation update when the public contract changes
```

A successful manual run alone is not sufficient evidence for persistence or scientific behavior.
