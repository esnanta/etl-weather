# Test Audit and Refactor TODO

References:

- [Root AGENTS.md](../../AGENTS.md)
- [Root README.md](../../README.md)
- [Orchestration README.md](../README.md)
- [Airflow best practices](../../docs/AIRFLOW_BEST_PRACTICE.md)
- [Airflow coding guidelines](../../docs/AIRFLOW_CODING_GUIDELINES.md)
- [Testing rules](../../tests/AGENTS.md)

## Purpose

This document tracks the audit and refactor of the test suite under `tests/`.

The central rule is:

> Tests may construct deterministic input data and fixtures, but they must not reimplement the production behavior they claim to verify.

A test should exercise objects, functions, models, and contracts defined by production code. Mocks, fakes, stubs, and monkeypatching are appropriate at external or deliberately isolated boundaries, but they must not replace the scientific, persistence, campaign, scheduling, or other production logic that is the subject of the test.

This work is a test-quality refactor. Production behavior must not be changed merely to make existing tests pass. If the audit reveals a production API that cannot be tested cleanly without reproducing its implementation in the test suite, record that separately as a design issue before changing production code.

## Current Findings

Initial review of branch `dev` identified several areas requiring deeper audit:

- `tests/unit/pipeline/test_runtime.py` defines test-side extractor, storage, transformer, and loader implementations. These can be useful as collaborators for narrowly scoped `WeatherPipeline` orchestration tests, but they must not be treated as proof that the concrete production implementations integrate correctly.
- `tests/unit/pipeline/test_load.py` contains test-side SQLAlchemy-like result, connection, engine, and transaction objects. Pure input/statement construction can remain unit tested, while PostgreSQL idempotency, conflict handling, rollback, row counts, and persistence correctness should primarily be proven against real PostgreSQL integration tests.
- orchestration tests contain `SimpleNamespace`, custom result/connection objects, and extensive monkeypatching. Where production domain objects already exist, tests should instantiate those production objects instead of inventing look-alike objects.
- `tests/unit/orchestration/test_submission_log.py` is a concrete candidate for replacing a `SimpleNamespace` submission result with the production submission-decision model.
- CDS/network fakes are not automatically defects. Tests such as extractor and paced HTTP submission tests may legitimately replace the external CDS/network boundary while continuing to execute the production extraction and pacing logic.

These are audit findings, not instructions to delete all mocks.

## Required Test-File Documentation

Every test file that is **created or revised during this work** must contain a module-level docstring at the very top of the file.

The docstring must explain, in concise terms:

1. the production behavior or contract being tested;
2. the production object/function/module that is the subject under test;
3. any external or collaborator boundary intentionally mocked, stubbed, or monkeypatched and why;
4. when relevant, whether stronger behavior is verified by an integration test.

The docstring must describe the test's purpose rather than merely restating the filename.

Example structure:

```python
"""Verify RAW checkpoint reuse through the production pipeline runtime.

The tests exercise WeatherPipeline checkpoint decisions using production request
and artifact models. External extraction/storage side effects are isolated so
the tests can prove runtime orchestration without contacting CDS or writing
project data. Concrete storage and persistence behavior is covered separately
by storage and integration tests.
"""
```

A module docstring does not replace focused test names or function docstrings when an individual test has non-obvious reasoning.

## Audit Classification

Each reviewed test or closely related group of tests must be classified as one of:

- **VALID** — production behavior is actually exercised; test-created values are inputs/fixtures or legitimate boundary substitutes.
- **WEAK** — production code is exercised, but excessive mocking or implementation-detail assertions reduce confidence.
- **SELF-FULFILLING** — the test recreates material production behavior and then primarily proves the recreated test implementation.
- **REDUNDANT** — the same contract is already proven more strongly elsewhere and this test adds no meaningful failure signal.

Do not classify a test as invalid merely because it constructs a `WeatherRequest`, DataFrame, xarray Dataset, timestamp, temporary artifact, or production model instance. Those are legitimate test inputs.

## Traceability Record

For every file reviewed, record at minimum:

```text
test/file
  -> production subject under test
  -> production objects used
  -> mocked/fake boundary
  -> observable behavior asserted
  -> classification
  -> proposed action
```

The purpose is to make it possible to answer: **what production behavior would fail this test if the production implementation were broken?**

## Work Checklist

### T1 — Complete inventory

- [x] Inventory every file under `tests/unit/pipeline/`.
- [x] Inventory every file under `tests/unit/orchestration/`.
- [x] Inventory every file under `tests/unit/operations/`.
- [x] Inventory every file under `tests/integration/pipeline/`.
- [x] Inventory every file under `tests/integration/orchestration/`.
- [x] Locate custom fake/stub/dummy classes and helper implementations.
- [x] Locate `SimpleNamespace`, `Mock`, `MagicMock`, monkeypatching, and replacement functions.
- [x] Locate tests calling private production functions and record whether the private API is genuinely the intended contract.
- [x] Locate test-side SQL/database implementations.
- [x] Locate duplicated calculations, algorithms, or policy logic that may reproduce production behavior.

#### T1 inventory result — 2026-10-07

Audit scope: branch `dev`, repository tree under `tests/`.

Python inventory:

- 48 Python files total under `tests/`, including package `__init__.py` files and shared support files;
- 37 executable `test_*.py` modules;
- 13 pipeline unit-test modules;
- 15 orchestration unit-test modules;
- 5 operations unit-test modules;
- 1 pipeline PostgreSQL integration-test module;
- 3 orchestration PostgreSQL integration-test modules;
- shared support is provided by `tests/conftest.py` and `tests/_paths.py`.

No `Mock` or `MagicMock` usage was found in the current test modules. Isolation is implemented primarily through pytest `monkeypatch`, local replacement functions, `SimpleNamespace`, custom test-double classes, SQLite/in-memory engines, and real isolated PostgreSQL integration fixtures.

##### Pipeline unit-test inventory

| File | Inventory markers requiring T2 review |
| --- | --- |
| `test_cds_pacing_chunks.py` | monkeypatch; `SimpleNamespace`; HTTP/CDS boundary replacement; direct private production calls `_download_cds_period`, `_download_cds`, and `_PacedSubmissionSession` |
| `test_config_requests.py` | monkeypatch; otherwise production request/config objects |
| `test_daily_climate_repository.py` | replacement function `fake_execute_modelling_query`; SQLite engine; private `_build_modelling_statement` accessed through `getattr` |
| `test_database.py` | custom `_Connection`, `_BeginContext`, and `_Engine` implementing a test-side database execution surface |
| `test_extract.py` | `FakeCdsClient`; `SimpleNamespace`; monkeypatch of external CDS module; private retrieval/context helpers accessed directly |
| `test_extract_cds_normalization.py` | no fake/mock markers detected; production normalization behavior is called directly |
| `test_load.py` | custom SQLAlchemy-like `_ScalarResult`, `_MappingResult`, `_CountResult`, `_PartitionLookupConnection`, `_BeginContext`, `_Engine`, and `_UpsertConnection`; monkeypatch of provenance/filesystem collaborators |
| `test_logging.py` | direct testing of private `_build_key_value_block` and `_log_dataframe_preview` |
| `test_request.py` | deterministic production-model fixture helpers; no mock/fake marker detected |
| `test_runtime.py` | custom `_Extractor`, `_Storage`, `_Transformer`, and `_Loader` implementations; validation monkeypatches |
| `test_storage.py` | filesystem/data-directory monkeypatching and deterministic artifact helper functions |
| `test_transform.py` | direct production `transform_data`; helper `_era5_land_cumulative_value` reconstructs ERA5-Land cumulative source values and therefore requires T2 independence review |
| `test_validation.py` | production validators with deterministic test datasets; no mock/fake marker detected |

##### Orchestration unit-test inventory

| File | Inventory markers requiring T2 review |
| --- | --- |
| `test_campaign.py` | `SimpleNamespace`; monkeypatched progress repository functions; SQLite dummy engine; helper `_month_end` independently computes expected calendar month end |
| `test_campaign_config.py` | monkeypatch; direct calls to private configuration validation helpers |
| `test_dag.py` | DAG production object inspection; no fake/mock marker detected |
| `test_database.py` | `SimpleNamespace`; monkeypatch of engine/config creation; database lifecycle surface |
| `test_mode.py` | local `RunType` enum used as input compatibility case; no monkeypatch/fake marker detected |
| `test_pipeline_progress.py` | substantial custom database result/connection/context/engine hierarchy, plus a second custom barrier result/connection/engine hierarchy |
| `test_progress.py` | deterministic production progress-definition helpers; no mock/fake marker detected |
| `test_progress_wiring.py` | extensive monkeypatching of task/repository collaborators; `SimpleNamespace`; local connection/result replacements in persisted-load proof tests |
| `test_raw_checkpoint_operator.py` | monkeypatch of Airflow/runtime collaborators; database engine involved |
| `test_reporting.py` | SQL/database-facing reporting tests; no general mock framework |
| `test_schedule.py` | monkeypatch of scheduling configuration/environment |
| `test_scheduled_selection_pilot.py` | in-memory dictionaries reproduce progress/claim state; `SimpleNamespace` progress object; repository methods monkeypatched; CDS download explicitly replaced with failure guard |
| `test_submission_log.py` | `SimpleNamespace` used as submission-decision look-alike; pacing repository and runtime engine monkeypatched |
| `test_submission_pacing.py` | monkeypatch around pacing configuration/collaborators |
| `test_tasks.py` | task/runtime monkeypatches; `SimpleNamespace` used for disk-usage result; task boundary dictionaries used as deterministic inputs |

##### Operations unit-test inventory

| File | Inventory markers requiring T2 review |
| --- | --- |
| `test_recreate_postgres_database.py` | `SimpleNamespace` configuration object; monkeypatch; direct private guard/confirmation helper calls |
| `test_reset_airflow_logs.py` | monkeypatch around filesystem/process operational boundaries |
| `test_reset_airflow_runtime.py` | monkeypatch around filesystem/process operational boundaries |
| `test_reset_all.py` | monkeypatch; direct private confirmation helper call |
| `test_reset_data.py` | monkeypatch around destructive filesystem boundary |

##### Integration-test inventory

| File | Inventory markers requiring T2 review |
| --- | --- |
| `integration/pipeline/test_postgresql_idempotency.py` | real isolated PostgreSQL behavior; monkeypatch used for controlled failure injection; no test-side SQL engine implementation |
| `integration/orchestration/test_orchestration_database_bootstrap.py` | real isolated PostgreSQL bootstrap/database behavior |
| `integration/orchestration/test_pipeline_progress_reconciliation.py` | real isolated PostgreSQL progress/reconciliation behavior |
| `integration/orchestration/test_submission_pacing.py` | real isolated PostgreSQL pacing/locking behavior |

##### Custom test-double and replacement hotspots

The strongest test-side implementation hotspots found during T1 are:

1. `tests/unit/pipeline/test_load.py` — recreates a meaningful subset of SQLAlchemy connection/result behavior.
2. `tests/unit/orchestration/test_pipeline_progress.py` — recreates database execution/result behavior for progress SQL and barrier queries.
3. `tests/unit/pipeline/test_runtime.py` — supplies complete test implementations of all four `WeatherPipeline` collaborators.
4. `tests/unit/pipeline/test_database.py` — supplies test-side engine/transaction/connection behavior.
5. `tests/unit/orchestration/test_progress_wiring.py` — extensive replacement of task/repository boundaries, including local persisted-load connection/result objects.
6. `tests/unit/orchestration/test_scheduled_selection_pilot.py` — implements progress status and claim state with local dictionaries and replacement functions.
7. `tests/unit/orchestration/test_submission_log.py` — returns a `SimpleNamespace` with the same field shape as a production submission decision.
8. `tests/unit/pipeline/test_extract.py` and `test_cds_pacing_chunks.py` — contain CDS/HTTP fakes, currently recorded as external-boundary candidates rather than defects.

##### Private production API access

Direct/private production API access found during T1 includes:

- `test_extract.py`: `_build_era5_land_retrieval_request`, `_format_context_period`, and `_retrieval_chunk_purpose`;
- `test_cds_pacing_chunks.py`: `_download_cds_period`, `_download_cds`, and `_PacedSubmissionSession`;
- `test_daily_climate_repository.py`: `_build_modelling_statement`;
- `test_logging.py`: `_build_key_value_block` and `_log_dataframe_preview`;
- `test_campaign_config.py`: private campaign/environment validation helpers;
- `test_recreate_postgres_database.py`: private static/runtime guard and confirmation helpers;
- `test_reset_all.py`: private confirmation helper.

T1 does not decide whether these private APIs should be public or whether their tests should be removed. That decision belongs to T2/T3 after the intended production contract is mapped.

##### Potential duplicated calculation/policy candidates

The following helpers or test-local state machines require explicit T2 review because they may reproduce part of the production rule being asserted:

- `test_transform.py::_era5_land_cumulative_value()` — reproduces the ERA5-Land cumulative value shape used as scientific source input;
- `test_campaign.py::_month_end()` — independently calculates calendar month end used in expected request periods;
- `test_scheduled_selection_pilot.py` local `statuses`, `claims`, `get_progress()`, and `claim()` — reproduce enough progress/claim state behavior to drive scheduled selection;
- custom SQLAlchemy result/connection objects in `test_load.py`, `test_pipeline_progress.py`, and `test_database.py` — reproduce database API behavior and route responses according to SQL text.

These are inventory candidates only. They are not yet classified as VALID, WEAK, SELF-FULFILLING, or REDUNDANT.

##### Module-level docstring baseline

Because the new rule applies whenever a test file is created or revised, T1 also recorded the current baseline. Several existing test modules already contain module docstrings, but many older modules do not. Missing module docstrings are **not changed during T1**; each file revised in later phases must receive the required top-level explanatory docstring as part of that same revision.

##### T1 gate result

**T1 is complete. No test or production code was changed.**

The inventory is sufficient to begin T2 traceability work. T2 must determine whether each recorded test double is a legitimate collaborator/external-boundary substitute or whether it replaces the production behavior the test claims to prove. No broad test rewrite is authorized by T1 alone.

### T2 — Build the traceability matrix

- [ ] Map every relevant test to its production subject.
- [ ] Record which production models are actually instantiated.
- [ ] Record every mocked boundary and its justification.
- [ ] Record the observable behavior/assertion being proven.
- [ ] Classify each test/group as VALID, WEAK, SELF-FULFILLING, or REDUNDANT.
- [ ] Identify behavioral contracts with no direct production-backed test.

**Gate:** no broad test rewrite begins until T1 and T2 provide enough evidence to distinguish legitimate isolation from self-fulfilling tests.

### T3 — Replace test-created domain look-alikes

- [ ] Prefer production domain models over `SimpleNamespace` or ad-hoc dictionaries where an appropriate production type exists.
- [ ] Replace fake submission-decision objects with the production submission-decision model where applicable.
- [ ] Replace campaign/progress look-alikes with production campaign/progress objects where practical.
- [ ] Preserve small deterministic input builders when they construct real production types.
- [ ] Do not create a new test-only domain model merely to make assertions easier.
- [ ] Add/update the required module-level docstring in every revised test file.

### T4 — Review pipeline runtime tests

- [ ] Review `tests/unit/pipeline/test_runtime.py` test by test.
- [ ] State explicitly which tests are only proving `WeatherPipeline` orchestration.
- [ ] Reduce collaborator implementations where simpler boundary doubles provide the same isolation.
- [ ] Use concrete production implementations where the claimed behavior requires concrete integration.
- [ ] Ensure runtime tests do not imply concrete extractor/storage/transformer/loader coverage that they do not provide.
- [ ] Confirm corresponding concrete-component tests exist.
- [ ] Add/update the required module-level docstring.

### T5 — Review persistence unit tests

- [ ] Review `tests/unit/pipeline/test_load.py` and related repository/loader tests.
- [ ] Keep pure dataframe preparation and deterministic statement/parameter construction at unit level when useful.
- [ ] Identify assertions that mainly prove custom fake SQLAlchemy behavior.
- [ ] Move or consolidate PostgreSQL-dependent behavioral proof into integration tests.
- [ ] Verify idempotent repeated load against real PostgreSQL.
- [ ] Verify transaction rollback against real PostgreSQL.
- [ ] Verify conflict/update behavior against real PostgreSQL where it is part of the contract.
- [ ] Verify persisted row counts and canonical observation identity against real PostgreSQL where appropriate.
- [ ] Remove fake database machinery only after equivalent behavioral coverage is confirmed.
- [ ] Add/update the required module-level docstring.

### T6 — Review orchestration tests

- [ ] Review campaign policy and selection tests.
- [ ] Review progress wiring and reconciliation tests.
- [ ] Review scheduled-selection and mapped-month tests.
- [ ] Review submission logging and pacing tests.
- [ ] Review task boundary tests.
- [ ] Replace domain look-alikes with production objects where available.
- [ ] Keep repository/Airflow/external side effects isolated when the unit test is intentionally testing policy or task orchestration.
- [ ] Avoid monkeypatching the exact production decision logic that the test claims to verify.
- [ ] Add/update the required module-level docstring in every revised file.

### T7 — Preserve legitimate external-boundary isolation

- [ ] Keep CDS client/network fakes where they prevent real provider calls while production extraction logic remains under test.
- [ ] Keep HTTP transport interception where it proves pacing occurs before provider submission.
- [ ] Keep deterministic clock/time injection where timing is not the subject under test.
- [ ] Keep temporary filesystem boundaries and failure injection where required to prove atomicity/recovery.
- [ ] Document why each non-obvious fake/mock is legitimate.
- [ ] Do not mock away scientific transformation logic in scientific correctness tests.

### T8 — Verify scientific tests independently

- [ ] Confirm transformation tests call production transformation functions/classes.
- [ ] Confirm expected values are derived from explicit scientific contracts rather than by calling/reimplementing the production algorithm.
- [ ] Protect UTC and UTC+7 timestamp semantics.
- [ ] Protect accumulation versus mean behavior.
- [ ] Protect unit conversions.
- [ ] Protect grid-point behavior.
- [ ] Protect missing values as missing values, not zero.
- [ ] Use small deterministic datasets with independently understandable expected results.
- [ ] Add/update module-level docstrings where files are revised.

### T9 — Remove redundant or misleading tests

- [ ] Identify duplicate contract coverage across unit and integration levels.
- [ ] Prefer the lowest test level that can genuinely prove the behavior.
- [ ] Prefer integration coverage when correctness fundamentally depends on PostgreSQL behavior.
- [ ] Remove tests that only prove a fake/stub implementation after stronger coverage exists.
- [ ] Remove assertions tied only to incidental implementation details when an observable contract can be asserted instead.
- [ ] Do not use total test count as the success criterion.

### T10 — Update testing rules and documentation

- [ ] Update `tests/AGENTS.md` after the audit conclusions are confirmed.
- [ ] Add the explicit rule: test data may be constructed, but production behavior must not be reimplemented in tests.
- [ ] Add the rule: prefer production domain objects over test-created look-alikes.
- [ ] Add the rule: mocks/fakes belong at justified external or collaborator boundaries.
- [ ] Add the module-level docstring requirement for new or revised test files.
- [ ] Update `tests/README.md` only if the human-facing testing strategy or commands change.
- [ ] Avoid duplicating detailed developer rules between `tests/README.md` and `tests/AGENTS.md`.

### T11 — Regression verification

- [ ] Run focused tests after each coherent refactor.
- [ ] Run all pipeline unit tests.
- [ ] Run all orchestration unit tests with campaign scheduling disabled unless schedule behavior is under test.
- [ ] Run all operations unit tests.
- [ ] Run the complete unit suite.
- [ ] Run PostgreSQL pipeline integration tests.
- [ ] Run PostgreSQL orchestration integration tests.
- [ ] Run the complete integration suite.
- [ ] Investigate any test that passes only after weakening an assertion.
- [ ] Record final pass results in this document when the work is complete.

## Regression Commands

Use the project environment:

```bash
source activate-airflow.sh
```

Focused unit domains:

```bash
python -m pytest tests/unit/pipeline -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit/orchestration -q

python -m pytest tests/unit/operations -q
```

Complete unit suite:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

Integration suite:

```bash
python -m pytest tests/integration -q
```

Integration tests must continue to use isolated `TEST_POSTGRES_*` configuration and must never silently fall back to development or production PostgreSQL.

## Completion Criteria

This TODO is complete only when:

- every material test has a clear production subject;
- test-created implementations do not substitute for the production behavior being claimed;
- production domain objects are used where suitable;
- external-boundary mocks remain only where justified;
- scientific expected results are independent and explicit;
- PostgreSQL behavior is proven at the integration level where required;
- every test file created or revised by this work has the required top-level module docstring;
- test documentation reflects the final rules;
- the complete unit and integration suites pass without weakening production contracts.

A lower final test count is acceptable if redundant or self-fulfilling tests are removed and the resulting suite provides stronger evidence of actual production behavior.
