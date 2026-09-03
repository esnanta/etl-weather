# Tests AGENTS

This document defines the testing rules for the weather ETL project.
It applies to code under `tests/` and complements the project-level `AGENTS.md`.

Authoritative references:

- `../AGENTS.md`
- `../docs/AIRFLOW_BEST_PRACTICE.md`
- `../docs/AIRFLOW_CODING_GUIDELINES.md`
- `README_TESTING.md`

If this file and the project-level rules disagree, follow the more specific rule for the testing context.

## Scope

- Testing is for the weather ETL pipeline.
- Testing covers pipeline behavior first.
- Airflow orchestration testing is part of the testing strategy, but it is kept separate from the core ETL tests.
- Tests should remain readable, deterministic, and maintainable.

## Testing Structure

Use this structure for the test suite:

- `tests/unit/`
- `tests/integration/`

Keep the structure formal and stable.

- `tests/unit/` contains isolated tests for one module or one responsibility at a time.
- `tests/integration/` contains tests that verify multiple pipeline components working together.

## Testing Style

- Prefer `pytest`.
- Prefer test functions over test classes unless there is a clear reason to use classes.
- Keep one module aligned with one test file where practical.
- Prefer real implementations whenever possible.
- Mock only external dependencies.
- Keep tests independent and repeatable.
- Prefer offline execution.
- Avoid network access in tests unless there is a strong reason and the test explicitly documents it.

## Fixtures

- Use a combination of:
  - small synthetic data
  - small sample data
- Fixtures should be small enough to understand at a glance.
- Fixtures should represent realistic pipeline cases.
- Fixtures should support fast local execution.

## Unit Tests

- Unit tests validate a single pipeline component in isolation.
- A unit test should test one behavior, not the whole pipeline.
- Unit tests should not depend on other pipeline modules unless that dependency is the subject under test.
- Unit tests should not require Airflow.
- Unit tests should not require network access.
- Unit tests should use small datasets.
- Unit tests should be fast and deterministic.

Typical unit test targets include:

- extraction helpers
- validation logic
- transformation logic
- storage helpers
- load/export helpers

## Integration Tests

- Integration tests verify that multiple pipeline modules work together.
- Integration tests should use the real pipeline implementation whenever practical.
- Integration tests should prefer temporary directories and isolated local resources.
- Integration tests should mock only external systems when needed.
- Integration tests should verify that artifacts are created and consumed correctly across stages.

## Airflow Testing

- Airflow testing should include DAG import and parse checks.
- Airflow testing should include task wiring checks.
- Airflow tests should stay focused on orchestration behavior, not ETL transformation logic.
- Airflow tests should verify that DAG structure, dependencies, and task relationships are correct.

## Test Data Rules

- Use the smallest data that proves the behavior.
- Prefer clear sample cases over large real datasets.
- Keep raw fixture data separate from generated test artifacts.
- Do not depend on live external data providers in routine tests.

## Execution Expectations

- Tests should be runnable offline by default.
- Tests should not require manual environment state unless the test explicitly documents it.
- When a test needs a temporary directory, keep the scope isolated.
- Keep test failures easy to diagnose.

## Current Priority

- Focus first on stable unit tests for pipeline modules.
- Keep integration tests small and representative.
- Keep Airflow testing separate and explicit.

