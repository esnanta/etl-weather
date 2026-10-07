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

- [x] Map every relevant test to its production subject.
- [x] Record which production models are actually instantiated.
- [x] Record every mocked boundary and its justification.
- [x] Record the observable behavior/assertion being proven.
- [x] Classify each test/group as VALID, WEAK, SELF-FULFILLING, or REDUNDANT.
- [x] Identify behavioral contracts with no direct production-backed test.

#### T2 traceability result — 2026-10-07

Classification below is at **file / closely related behavior-group level**. A file marked WEAK can still contain individually valid tests; the classification means that at least one material claim in that file is supported by test-side behavior or implementation-detail coupling that should be improved.

##### Pipeline unit tests

| Test file | Production subject and production objects | Mocked / fake boundary | Observable behavior asserted | Classification | Proposed action |
| --- | --- | --- | --- | --- | --- |
| `test_cds_pacing_chunks.py` | `pipeline.extraction.cds_era5_land`, `pipeline.providers.cds_client`; real `WeatherRequest`, `BoundingBox`, `SubmissionDeferred` | HTTP transport, `cdsapi` client/module, submission callback, one internal downloader replacement | chunk cache reuse after deferral; corrupt cache identity fails; reservation precedes POST; every retry requires a new slot | **VALID** | Keep external-boundary fakes. During revision, add docstring detail explaining why private pacing/session APIs are intentionally tested; reconsider private API exposure only if production design changes. |
| `test_config_requests.py` | production config resolvers and `build_cds_request`; real `WeatherRequest` and `DataSource` | environment and dotenv only | required API key, canonical dataset/variables/bbox, explicit period, APP_ENV and CDS period validation | **VALID** | Keep; add module docstring when revised. |
| `test_daily_climate_repository.py` | `DailyClimateSelection`, selection/dataframe validators, repository loader, private statement builder | query execution replaced; SQLite engine only carries call boundary | canonical SQL filters, input validation, schema/order/missing-value preservation, query params | **WEAK** | Keep pure validators. Replace or supplement loader/query behavior with a real PostgreSQL repository integration test; private SQL-string assertions should not be sole proof of repository correctness. |
| `test_database.py` | production `create_schema`, `create_daily_climate_table`, `create_daily_climate_detail_table`, real `DatabaseConfig` | custom `_Engine/_Connection/_BeginContext` implements SQLAlchemy surface | emitted DDL contains expected schema/table/constraint definitions | **WEAK** | Retain only if treated explicitly as SQL-construction contract; rely on real PostgreSQL bootstrap integration for actual DDL correctness. Simplify fake engine if possible. |
| `test_extract.py` | `CdsEra5LandExtractor`, CDS payload builder and normalization; real request/data models | fake `cdsapi.Client`, environment, temporary NetCDF | retrieval context, payload, normalization, variable selection, log periods, no live CDS | **VALID** | Keep fake provider boundary. Review private context-format helpers separately; public extractor behavior remains valid. |
| `test_extract_cds_normalization.py` | production CDS normalization helpers/functions | none material | source variable/time normalization | **VALID** | Keep. |
| `test_load.py` | production PostgreSQL loader functions, real `TransformedWeatherDataset`, `DatasetArtifact`, `DatabaseConfig` | extensive SQLAlchemy-like result/connection/engine classes; provenance/hash collaborators | generated metadata/upsert SQL, unchanged-partition decision, observation conflict key, skip path | **WEAK** | Keep pure dataframe preparation and small deterministic parameter/SQL construction tests. Consolidate idempotency, unchanged-partition persistence, conflicts, and transactions into real PostgreSQL integration coverage; reduce custom SQLAlchemy machinery. |
| `test_logging.py` | private production logging formatters | none | key/value formatting and dataframe preview logging | **VALID** | Behavior is real but private. Keep if log format is an intentional contract; otherwise migrate toward public logging behavior. |
| `test_request.py` | request validation/serialization/key/signature/month split; real `WeatherRequest`, `BoundingBox`, enums | none | identity, normalization, splitting, validation | **VALID** | Keep. |
| `test_runtime.py` | real `WeatherPipeline`, request/dataset/artifact models and ETL interfaces | complete custom implementations of extractor, storage, transformer, loader; validators replaced | runtime call order, checkpoint branch selection, delegation, resume behavior | **WEAK** | Keep as narrow orchestration tests only. Rename/describe scope clearly in module docstring. Ensure concrete extractor/storage/transformer/loader behavior remains covered elsewhere; reduce collaborator code where simpler spies suffice. |
| `test_storage.py` | production artifact storage, NetCDF/Parquet provenance and checkpoint functions; real request/data/artifact models | temporary data root, failure injection | round-trip storage, provenance, overwrite refusal, atomic write cleanup, missing artifact | **VALID** | Keep temporary filesystem isolation. |
| `test_transform.py` | production `transform_data`; real `LoadedWeatherDataset`, request/bbox models | none | UTC+7 daily boundary, accumulated totals, means, unit conversion, multi-grid independence, missing/null/negative handling | **VALID** | Keep. `_era5_land_cumulative_value` is an input generator, not the production daily-total algorithm; nevertheless document the source-series construction clearly so expected outputs remain independently understandable. |
| `test_validation.py` | production raw/transformed validators; real raw/transformed dataset models | none | temporal continuity, grid resolution/membership, UTC+7 relation, duplicate/off-grid/value constraints, missing-value allowance | **VALID** | Keep. |

##### Pipeline integration tests

| Test file | Production subject and production objects | Boundary | Observable behavior asserted | Classification | Proposed action |
| --- | --- | --- | --- | --- | --- |
| `integration/pipeline/test_postgresql_idempotency.py` | real storage functions + `load_to_postgresql` + PostgreSQL; real models/artifacts | isolated `TEST_POSTGRES_*`; temporary artifact root; controlled invalid detail | repeated partition load creates no duplicate rows; failed detail insert rolls back metadata and details | **VALID** | Keep as authoritative persistence proof. Expand here if unit loader tests are reduced. |

##### Orchestration unit tests

| Test file | Production subject and production objects | Mocked / fake boundary | Observable behavior asserted | Classification | Proposed action |
| --- | --- | --- | --- | --- | --- |
| `test_campaign.py` | production `CampaignQuarter`, `CampaignSelection`, `CampaignBlocked`, campaign builders/selector | progress repository replaced; returned progress is `SimpleNamespace`; SQLite dummy engine | quarter boundaries, monthly mapping, initialization, earliest incomplete selection, blocking, matching manual repair | **WEAK** | Calendar and pure campaign tests are valid. Replace progress look-alike with production `PipelineProgress` where feasible; keep repository calls isolated for policy tests. |
| `test_campaign_config.py` | production orchestration config module | environment | required campaign key/year parsing/range | **VALID** | Keep; private helpers are acceptable only if intentionally treated as config contract. |
| `test_dag.py` | actual Airflow `DagBag` and production DAG | none beyond local DAG parse | DAG graph, task IDs, pools/retries/timeouts/mapping, no Airflow import from pipeline | **VALID** | Keep. |
| `test_database.py` | production orchestration DB lifecycle/bootstrap functions | engine/config creation and bootstrap child functions replaced; `SimpleNamespace` URL/engine | engine disposal and initialization ordering | **VALID** | Replacement is collaborator-level and assertions target composition/order. Prefer real `DatabaseConfig` over look-alike where easy; integration bootstrap remains authoritative for actual DDL. |
| `test_mode.py` | production `RunMode` and `resolve_run_mode` | local Enum only as input-shape compatibility | scheduled/manual resolution and invalid-input rejection | **VALID** | Keep. |
| `test_pipeline_progress.py` | production progress repository functions and real `PipelineProgressDefinition/ProgressPartition` | extensive custom SQLAlchemy execution/result hierarchy | emitted DDL/SQL, status parameter validation, reconciliation query shape, claim/release SQL, barrier interpretation | **WEAK** | Keep pure validation and possibly SQL-construction checks, but actual progress semantics must be owned by PostgreSQL integration tests. Reduce fake database surface substantially. |
| `test_progress.py` | production progress-definition construction and request identity | none material | cadence-neutral identity/partitions/configuration identity | **VALID** | Keep. |
| `test_progress_wiring.py` | production task/progress/repair functions and some production request builders | many runtime/repository/artifact collaborators replaced; some local connection/result objects; `SimpleNamespace` artifact look-alike | progress status transitions around raw/transform/load, failure recording, persisted-load evidence, manual repair, slot metadata | **WEAK** | Split by responsibility. Use production domain objects (`PipelineProgress`, real artifact/request models) where available; retain side-effect isolation but avoid reproducing persistence proof in local fake connections when integration tests exist. |
| `test_raw_checkpoint_operator.py` | production Airflow raw checkpoint operator/task behavior | runtime/progress collaborators and deferral timing isolated | first attempt / defer / retry-pass behavior and metadata handoff | **VALID** | Keep; mocks isolate Airflow/runtime boundaries rather than replace operator policy. |
| `test_reporting.py` | production campaign reporting/query functions | database query surface only as needed | campaign status/report aggregation and presentation contract | **VALID** | Keep if SQL result shaping is small; integration coverage is not required for formatting-only behavior. |
| `test_schedule.py` | production schedule/config resolution | environment/config monkeypatch | disabled/enabled schedule interpretation | **VALID** | Keep. |
| `test_scheduled_selection_pilot.py` | production `prepare_monthly_jobs`, campaign selector/policy code | local dictionaries implement progress status + claim persistence; `SimpleNamespace` progress; CDS hard-fail guard | scheduled Q1→Q2→Q3 movement, competing claim block, failed-quarter block, terminal no-op | **WEAK** | Retain offline end-to-end policy shape, but replace progress look-alike with `PipelineProgress`; avoid making local claim state the only proof of concurrency/atomicity because PostgreSQL integration already tests real claims. |
| `test_submission_log.py` | production `reserve_or_defer`, `WeatherRequest` | pacing repository and engine isolated; `SimpleNamespace` mimics decision | granted/deferred log fields and exception behavior | **WEAK** | Replace look-alike with production `SubmissionDecision`. The rest of the isolation is legitimate. |
| `test_submission_pacing.py` | production pacing interval/config functions | environment/time/config collaborators | interval validation and pacing policy edge cases | **VALID** | Keep; real atomic reservation is covered by integration. |
| `test_tasks.py` | production request builders and task wrappers around pipeline/runtime/monitoring | runtime ETL functions and disk-usage OS call isolated; task boundary dicts as inputs | request splitting, task boundary validation, memory logging, failure logging, disk warnings | **VALID** | Keep. `SimpleNamespace` for `shutil.disk_usage` result is an external OS-result substitute, not a domain look-alike. |

##### Orchestration integration tests

| Test file | Production subject | Boundary | Observable behavior asserted | Classification | Proposed action |
| --- | --- | --- | --- | --- | --- |
| `integration/orchestration/test_orchestration_database_bootstrap.py` | production application storage bootstrap against PostgreSQL | isolated `TEST_POSTGRES_*` | all required tables are created and repeated bootstrap is idempotent | **VALID** | Keep as authoritative DDL/bootstrap proof. |
| `integration/orchestration/test_pipeline_progress_reconciliation.py` | production progress repository + canonical climate tables against PostgreSQL | isolated `TEST_POSTGRES_*`; unique test identities | completion requires matching persisted evidence; claim exclusivity; failure block; manual repair completes only with all partitions; checksum/row-count mismatch reopens progress | **VALID** | Keep as authoritative progress/reconciliation proof. Consider factoring fixture setup later for readability only, not changing behavior. |
| `integration/orchestration/test_submission_pacing.py` | production `reserve_submission` against PostgreSQL | isolated `TEST_POSTGRES_*` and two threads | durable atomic global reservation and persistence across calls | **VALID** | Keep as authoritative pacing/locking proof. |

##### Operations unit tests

| Test file | Production subject | Mocked / fake boundary | Observable behavior asserted | Classification | Proposed action |
| --- | --- | --- | --- | --- | --- |
| `test_recreate_postgres_database.py` | production maintenance script guards/main/confirmation | DB recreation and environment/preflight side effects isolated; one `SimpleNamespace` config helper plus real `DatabaseConfig` elsewhere | prod denial, missing env, inspect safety, system DB guard, confirmation loop, preflight ordering, direct-script import behavior | **VALID** | Prefer real `DatabaseConfig` consistently and remove unused/look-alike config helper if possible. Private guard testing is justified for destructive safety code. |
| `test_reset_airflow_logs.py` | production log cleanup/main | environment and destructive call isolated for guard tests; real temp filesystem for cleanup | root preserved, children removed, prod/inspect safe | **VALID** | Keep. |
| `test_reset_airflow_runtime.py` | production runtime-state cleanup/main | environment/confirmation/destructive call isolated; real temp filesystem | tracked config/DAGs preserved, generated state removed, prod/inspect/no-confirm safety | **VALID** | Keep. |
| `test_reset_all.py` | production composed reset/main | individual destructive components and preflight/confirmation isolated | safe ordering, fail-stop behavior, prod/inspect/confirmation guards, direct script behavior | **VALID** | Keep; composition mocks are appropriate because component behavior is tested separately. |
| `test_reset_data.py` | production data cleanup/main | environment/confirmation isolated; real temp filesystem | generated contents and symlink removed without deleting target; guard behavior | **VALID** | Keep. |

#### T2 classification summary

At current file/group granularity:

- **VALID:** 29 modules;
- **WEAK:** 8 modules;
- **SELF-FULFILLING:** 0 modules conclusively classified;
- **REDUNDANT:** 0 modules conclusively classified.

The absence of a SELF-FULFILLING classification is intentional. T1 identified several suspicious test doubles, but T2 found that production functions are still executed in those files. The main problem is **weak evidence and overbuilt collaborator fakes**, not tests that completely bypass production code.

The eight WEAK modules are:

1. `tests/unit/pipeline/test_daily_climate_repository.py`
2. `tests/unit/pipeline/test_database.py`
3. `tests/unit/pipeline/test_load.py`
4. `tests/unit/pipeline/test_runtime.py`
5. `tests/unit/orchestration/test_campaign.py`
6. `tests/unit/orchestration/test_pipeline_progress.py`
7. `tests/unit/orchestration/test_progress_wiring.py`
8. `tests/unit/orchestration/test_scheduled_selection_pilot.py`
9. `tests/unit/orchestration/test_submission_log.py`

This list contains **9** WEAK modules; therefore the numeric summary above is corrected to:

- **VALID:** 28 modules;
- **WEAK:** 9 modules;
- **SELF-FULFILLING:** 0 modules;
- **REDUNDANT:** 0 modules.

##### Behavioral contracts currently stronger at integration level than unit level

The audit confirms that these behaviors should not be judged primarily from fake database unit tests because real PostgreSQL coverage already exists:

- repeated daily-climate partition idempotency;
- transaction rollback when detail persistence fails;
- orchestration database bootstrap / DDL execution;
- progress reconciliation against canonical persisted climate evidence;
- atomic progress claim behavior;
- durable global submission pacing / locking.

Unit tests around these areas should therefore focus on pure validation, deterministic input transformation, SQL/parameter construction where valuable, and composition—not attempt to recreate PostgreSQL semantics.

##### Behavioral gaps / under-proven boundaries identified by T2

No critical production behavior was found to be completely untested, but several boundaries are under-proven or only indirectly proven:

- `WeatherPipeline` has good orchestration tests, while concrete extractor/storage/transformer/loader behavior is tested in separate modules rather than through one concrete offline pipeline composition. This is acceptable architecture, but the runtime test docstring must not imply concrete-component integration coverage.
- daily-climate read repository behavior is validated heavily at unit level, but the actual PostgreSQL SELECT path is not covered by a dedicated repository integration test. If this repository is considered a stable public data-access boundary, add one focused integration test rather than expanding fake SQL execution.
- some private helpers are directly asserted as if they were stable contracts. T3/T6 should decide case by case whether the behavior should be exercised through a public boundary or the helper is intentionally testable private implementation.
- scheduled selection policy is well covered offline, and real progress claim atomicity is covered in PostgreSQL integration, but there is no need to reproduce database claim semantics inside the offline pilot.

##### Domain-object replacement candidates confirmed by production code

Production models exist and should replace look-alikes when affected files are revised:

- `orchestration.repositories.submission_pacing.SubmissionDecision` should replace the `SimpleNamespace(granted, checked_at, next_allowed_at)` in `test_submission_log.py`.
- `orchestration.repositories.pipeline_progress.PipelineProgress` should replace progress-shaped `SimpleNamespace` objects in campaign/scheduled-selection tests where only persisted progress state is being represented.
- real `DatabaseConfig`, `WeatherRequest`, `DatasetArtifact`, and campaign dataclasses should remain preferred over ad-hoc objects.

##### Scientific independence conclusion

`test_transform.py::_era5_land_cumulative_value()` is **not classified as a reimplementation of the daily transformation algorithm**. It constructs deterministic cumulative hourly source values. The assertions independently expect daily totals, means, timestamps, and unit conversions from production `transform_data`. It should remain, but its future module docstring should explain that it is a source-fixture generator.

Likewise, `test_campaign.py::_month_end()` computes independent expected calendar boundaries and is acceptable as expected-value construction, not a substitute for campaign selection logic.

##### T2 gate result

**T2 is complete. No test or production code was changed.**

The audit now distinguishes legitimate boundary isolation from weak test-side implementations. The next phase may begin with T3, but revisions should be incremental and should start with the lowest-risk confirmed domain look-alikes before touching database-heavy test architecture.

Recommended T3 starting order:

1. `test_submission_log.py` — replace `SimpleNamespace` with production `SubmissionDecision` and add the required top-level docstring/update existing docstring as needed;
2. `test_campaign.py` and `test_scheduled_selection_pilot.py` — replace progress-shaped `SimpleNamespace` with production `PipelineProgress`;
3. then reassess `test_progress_wiring.py`;
4. leave `test_load.py`, `test_pipeline_progress.py`, `test_database.py`, and `test_runtime.py` for their dedicated T4/T5/T6 work rather than doing broad rewrites in T3.

**Gate:** T1 and T2 are now complete. Broad test rewrites remain prohibited; subsequent changes should follow the staged T3–T9 checklist and preserve production behavior.

### T3 — Replace test-created domain look-alikes

- [x] Prefer production domain models over `SimpleNamespace` or ad-hoc dictionaries where an appropriate production type exists.
- [x] Replace fake submission-decision objects with the production submission-decision model where applicable.
- [x] Replace campaign/progress look-alikes with production campaign/progress objects where practical.
- [x] Preserve small deterministic input builders when they construct real production types.
- [x] Do not create a new test-only domain model merely to make assertions easier.
- [x] Add/update the required module-level docstring in every revised test file.

#### T3 implementation result — 2026-10-07

T3 was intentionally limited to confirmed **domain look-alikes**. Database-heavy test doubles remain assigned to T4/T5/T6 rather than being rewritten opportunistically.

Changed files:

- `tests/unit/orchestration/test_submission_log.py`
  - replaced `SimpleNamespace(granted, checked_at, next_allowed_at)` with the production `SubmissionDecision`;
  - kept database-engine and pacing-repository monkeypatches because the test verifies submission-gate/log behavior, not PostgreSQL locking;
  - expanded the module-level docstring to name the production subject and integration boundary.

- `tests/unit/orchestration/test_campaign.py`
  - replaced progress-shaped `SimpleNamespace` with production `PipelineProgress`;
  - kept repository functions isolated so campaign policy remains a focused unit test;
  - added a module-level docstring explaining that PostgreSQL progress semantics are covered by integration tests.

- `tests/unit/orchestration/test_scheduled_selection_pilot.py`
  - replaced progress-shaped `SimpleNamespace` with production `PipelineProgress`;
  - retained the local `statuses` and `claims` dictionaries only as deterministic offline repository state;
  - explicitly kept real claim atomicity out of this unit test because PostgreSQL integration tests already prove it;
  - revised the module-level docstring accordingly.

- `tests/unit/orchestration/test_progress_wiring.py`
  - replaced selected-quarter `SimpleNamespace` objects with production `CampaignSelection`;
  - replaced the nested raw-artifact look-alike with production `DatasetArtifact` + `DataLayer.RAW`;
  - retained task/repository monkeypatches that isolate side effects;
  - revised the module-level docstring to state production subjects and integration ownership.

Not changed during T3:

- `tests/unit/orchestration/test_database.py` still uses `SimpleNamespace` for a disposable engine/config collaborator. This is not a persisted domain-state look-alike and was classified VALID in T2.
- `tests/unit/orchestration/test_tasks.py` still uses `SimpleNamespace(free=...)` as an OS `shutil.disk_usage` result substitute. This is an external-system result, not a production domain model.
- SQLAlchemy/database result/connection doubles in `test_load.py`, `test_pipeline_progress.py`, and related files remain untouched for their dedicated later phases.

T3 does not change production code or intended behavior.

Focused regression to run locally:

```bash
source activate-airflow.sh

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest \
  tests/unit/orchestration/test_submission_log.py \
  tests/unit/orchestration/test_campaign.py \
  tests/unit/orchestration/test_scheduled_selection_pilot.py \
  tests/unit/orchestration/test_progress_wiring.py \
  -q
```

**T3 is complete.** Local focused regression on 2026-10-07:

```text
36 passed in 1.11s
```

Command used:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest \
  tests/unit/orchestration/test_submission_log.py \
  tests/unit/orchestration/test_campaign.py \
  tests/unit/orchestration/test_scheduled_selection_pilot.py \
  tests/unit/orchestration/test_progress_wiring.py \
  -q
```

### T4 — Review pipeline runtime tests

- [x] Review `tests/unit/pipeline/test_runtime.py` test by test.
- [x] State explicitly which tests are only proving `WeatherPipeline` orchestration.
- [x] Reduce collaborator implementations where simpler boundary doubles provide the same isolation.
- [x] Use concrete production implementations where the claimed behavior requires concrete integration.
- [x] Ensure runtime tests do not imply concrete extractor/storage/transformer/loader coverage that they do not provide.
- [x] Confirm corresponding concrete-component tests exist.
- [x] Add/update the required module-level docstring.

#### T4 review result — 2026-10-07

The runtime tests were reviewed against `pipeline/runtime.py`, the production ETL interfaces, and the dedicated concrete-component test modules.

Decision: **retain the injected recording collaborators in `test_runtime.py`.**

This is intentional rather than a failure to eliminate test doubles. `WeatherPipeline` is itself an orchestration/composition object whose constructor accepts `WeatherExtractor`, `DataStorage`, `DataTransformer`, and `DataLoader`. The unit-test responsibility is therefore to prove which collaborator is invoked, when checkpoint branches are selected, and whether validation/delegation happen at the correct boundary. Replacing those collaborators with concrete CDS/storage/transform/PostgreSQL implementations would change the test level and duplicate dedicated component coverage.

Test-by-test scope:

- `test_ensure_raw_checkpoint_creates_raw_checkpoint_when_missing`
  - proves only WeatherPipeline orchestration: missing RAW checkpoint -> extractor -> save -> validate -> dataframe conversion;
  - does not prove CDS retrieval or NetCDF storage correctness.

- `test_ensure_raw_checkpoint_uses_and_validates_existing_raw_checkpoint`
  - proves resume branch selection and RAW validation;
  - does not prove concrete checkpoint deserialization.

- `test_ensure_transformed_checkpoint_uses_and_validates_existing_checkpoint`
  - proves transformed-checkpoint resume path and that transformation is skipped;
  - does not prove Parquet/provenance rehydration.

- `test_ensure_transformed_checkpoint_validates_before_saving_new_checkpoint`
  - proves transform -> validate -> save ordering at the WeatherPipeline boundary;
  - does not prove the scientific transformation formula.

- `test_load_delegates_dataset_and_artifact_to_idempotent_loader`
  - proves delegation of the exact transformed dataset/artifact to the loader;
  - does not prove PostgreSQL idempotency.

- `test_transform_from_checkpoint_rehydrates_and_validates_raw`
  - proves RAW rehydration is validated before transformation and extraction is not repeated;
  - concrete RAW storage behavior remains owned by storage tests.

- `test_load_from_checkpoint_rehydrates_and_validates_transformed`
  - proves transformed checkpoint rehydration is validated before loader delegation;
  - concrete persistence remains owned by loader/integration tests.

Concrete-component coverage confirmed:

- CDS extraction/provider behavior -> `tests/unit/pipeline/test_extract.py` and `test_cds_pacing_chunks.py`;
- local RAW/Parquet storage, checkpoint/provenance, atomic writes -> `tests/unit/pipeline/test_storage.py`;
- ERA5-Land daily scientific transformation -> `tests/unit/pipeline/test_transform.py`;
- raw/transformed validation -> `tests/unit/pipeline/test_validation.py`;
- PostgreSQL loader construction/decision behavior -> `tests/unit/pipeline/test_load.py`;
- actual PostgreSQL idempotency and rollback -> `tests/integration/pipeline/test_postgresql_idempotency.py`.

The current collaborator classes are larger than trivial mocks because `DataStorage` exposes several coordinated methods and the tests need observable call state across checkpoint branches. Replacing them with many independent monkeypatch lambdas would make the tests less readable and less faithful to the injected interface contract. Therefore no structural rewrite was made.

The required top-level module docstring was added to `test_runtime.py`. It now explicitly states that the file verifies WeatherPipeline orchestration through production interfaces and does **not** claim concrete CDS/storage/scientific/PostgreSQL integration coverage.

No production code was changed.

Focused regression to run locally:

```bash
source activate-airflow.sh

python -m pytest tests/unit/pipeline/test_runtime.py -q
```

**T4 is complete.** Local focused regression on 2026-10-07:

```text
7 passed in 0.71s
```

Command used:

```bash
source activate-airflow.sh -q

python -m pytest tests/unit/pipeline/test_runtime.py -q
```

### T5 — Review persistence unit tests

- [x] Review `tests/unit/pipeline/test_load.py` and related repository/loader tests.
- [x] Keep pure dataframe preparation and deterministic statement/parameter construction at unit level when useful.
- [x] Identify assertions that mainly prove custom fake SQLAlchemy behavior.
- [x] Move or consolidate PostgreSQL-dependent behavioral proof into integration tests.
- [x] Verify idempotent repeated load against real PostgreSQL.
- [x] Verify transaction rollback against real PostgreSQL.
- [x] Verify conflict/update behavior against real PostgreSQL where it is part of the contract.
- [x] Verify persisted row counts and canonical observation identity against real PostgreSQL where appropriate.
- [x] Remove fake database machinery only after equivalent behavioral coverage is confirmed.
- [x] Add/update the required module-level docstring.

#### T5 implementation result — 2026-10-07

T5 narrows unit persistence tests to behavior that can be proven without pretending to be PostgreSQL, and moves persistence semantics to the existing isolated PostgreSQL integration suite.

Changes to `tests/unit/pipeline/test_load.py`:

- removed custom partition lookup/result/engine/transaction classes that routed behavior by inspecting SQL text;
- removed unit tests that treated those fake database objects as proof of unchanged-partition/idempotency behavior;
- retained pure `_prepare_observation_dataframe` tests for canonical database column names and missing-value-to-NULL conversion;
- retained deterministic metadata INSERT/UPSERT statement and bound-parameter construction;
- retained deterministic detail UPSERT statement construction, canonical conflict key, timestamp conversion, and empty-dataset behavior;
- replaced the previous database machinery with one minimal recording connection whose only purpose is to capture emitted SQL and parameters;
- added the required module-level docstring explaining that real PostgreSQL semantics belong to integration tests.

Changes to `tests/integration/pipeline/test_postgresql_idempotency.py`:

- added a top-level module docstring making the real isolated PostgreSQL suite authoritative for persistence behavior;
- retained the existing repeated-load idempotency test;
- retained the existing transaction rollback test;
- added a real PostgreSQL case where one persisted observation is deleted, proving that row-count mismatch prevents the unchanged-partition shortcut and a repeated load restores the complete partition;
- added a real PostgreSQL provenance-mismatch case where metadata path/checksum and persisted observation values are deliberately made stale, proving that the loader takes the normal UPSERT path, restores canonical provenance, and updates the conflicting observation values.

Changes to `tests/unit/pipeline/test_database.py`:

- kept the recording-engine DDL construction tests because they assert the emitted schema/table/index contract;
- added the required module-level docstring explicitly stating that these tests do not emulate PostgreSQL execution semantics;
- actual PostgreSQL DDL/bootstrap behavior remains owned by integration coverage.

Persistence responsibility after T5:

- **unit tests**: dataframe preparation, NULL conversion, SQL/parameter construction, DDL construction;
- **integration tests**: actual idempotency, row-count evidence, provenance mismatch recovery, conflict UPDATE behavior, transaction rollback, and persisted canonical observations.

No production code was changed.

Focused regression to run locally:

```bash
source activate-airflow.sh -q

python -m pytest \
  tests/unit/pipeline/test_load.py \
  tests/unit/pipeline/test_database.py \
  -q
```

Then run the real PostgreSQL persistence suite:

```bash
source activate-airflow.sh -q

python -m pytest \
  tests/integration/pipeline/test_postgresql_idempotency.py \
  -q
```

**T5 is complete.** Local verification on 2026-10-07:

```text
tests/unit/pipeline/test_load.py + test_database.py
9 passed in 0.72s

tests/integration/pipeline/test_postgresql_idempotency.py
4 passed in 1.27s
```

Commands used:

```bash
source activate-airflow.sh -q

python -m pytest \
  tests/unit/pipeline/test_load.py \
  tests/unit/pipeline/test_database.py \
  -q

python -m pytest \
  tests/integration/pipeline/test_postgresql_idempotency.py \
  -q
```

### T6 — Review orchestration tests

- [x] Review campaign policy and selection tests.
- [x] Review progress wiring and reconciliation tests.
- [x] Review scheduled-selection and mapped-month tests.
- [x] Review submission logging and pacing tests.
- [x] Review task boundary tests.
- [x] Replace domain look-alikes with production objects where available.
- [x] Keep repository/Airflow/external side effects isolated when the unit test is intentionally testing policy or task orchestration.
- [x] Avoid monkeypatching the exact production decision logic that the test claims to verify.
- [x] Add/update the required module-level docstring in every revised file.

#### T6 implementation result — 2026-10-07

The orchestration suite was reviewed by responsibility rather than applying a blanket rule against monkeypatching.

##### Campaign policy and scheduled selection

`test_campaign.py` and `test_scheduled_selection_pilot.py` remain focused unit tests.

They execute the production campaign policy / monthly-job selection code while repository I/O is replaced. This is appropriate because the behavior under test is ordering, eligibility, failed-quarter blocking, claim-result handling, and monthly mapping—not PostgreSQL transaction mechanics.

T3 already replaced progress-shaped look-alikes with production `PipelineProgress`. No further structural rewrite is required.

Real claim atomicity remains owned by `tests/integration/orchestration/test_pipeline_progress_reconciliation.py`.

##### Progress wiring

`test_progress_wiring.py` remains a task-wiring unit test.

It verifies:

- manual and scheduled request mapping;
- status updates around load evidence;
- failure propagation;
- campaign repair handoff;
- pacing deferral not being misclassified as extraction failure;
- scheduled slot metadata.

Runtime/database/artifact side effects remain isolated. T3 already replaced the material domain look-alikes with production `CampaignSelection` and `DatasetArtifact`.

The local result/connection objects used specifically by `persisted_row_count()` remain candidates for later simplification only if that repository receives dedicated integration coverage. They are not expanded during T6.

##### Pipeline progress repository

`tests/unit/orchestration/test_pipeline_progress.py` was narrowed substantially.

Removed from unit-level behavioral proof:

- fake claim/release semantics;
- fake extraction-attempt barrier result interpretation;
- custom barrier result/connection/engine hierarchy.

Retained at unit level:

- cadence-neutral progress DDL construction;
- parent/detail INSERT construction and seeded request-key parameters;
- progress-detail update SQL/parameter construction;
- status validation;
- reconciliation SQL contract requiring matching canonical persisted evidence.

The file now uses one minimal recording engine only to capture generated SQL and parameters, and its top-level docstring explicitly states that it does not emulate PostgreSQL state.

##### Real PostgreSQL progress semantics

`tests/integration/orchestration/test_pipeline_progress_reconciliation.py` is now explicitly documented as authoritative for:

- persisted-evidence reconciliation;
- atomic claims;
- release behavior;
- failed-partition recovery;
- manual repair;
- extraction-attempt barrier behavior.

A new PostgreSQL integration test verifies the extraction-attempt barrier:

1. three expected detail rows initially pending -> barrier false;
2. one complete + one failed + one pending -> barrier false;
3. all three details terminal (`complete` or `failed`) -> barrier true;
4. unknown progress identity -> explicit error.

This removes the need for the former test-side barrier database implementation.

##### Submission logging and pacing

`test_submission_log.py` remains valid after T3 because it executes production `reserve_or_defer()` with production `SubmissionDecision`. Database acquisition and durable reservation are isolated because the test asserts log/deferral behavior.

`test_submission_pacing.py` remains unit coverage for interval/config validation.

Actual atomic/durable pacing continues to be proven by
`tests/integration/orchestration/test_submission_pacing.py`.

##### Raw checkpoint / Airflow operator tests

`test_raw_checkpoint_operator.py` remains valid.

Airflow/runtime/progress collaborators are intentionally isolated so the tests can prove task deferral/retry policy without making CDS calls or relying on live PostgreSQL. The production operator/task decision logic itself remains under test.

##### Task boundary tests

`test_tasks.py` remains valid.

Runtime ETL calls, disk-usage OS calls, and monitoring side effects are collaborator/external boundaries. The task wrapper functions themselves are executed. The `SimpleNamespace(free=...)` values model `shutil.disk_usage` output and are not production domain-state look-alikes.

##### Orchestration database/bootstrap tests

`test_database.py` remains valid as composition/order coverage. Its actual DDL/bootstrap result is already verified against PostgreSQL in
`tests/integration/orchestration/test_orchestration_database_bootstrap.py`.

##### Reporting tests

`test_reporting.py` remains focused on reporting/query shaping and does not need live PostgreSQL merely to verify formatting/aggregation behavior.

##### Files changed in T6

- `tests/unit/orchestration/test_pipeline_progress.py`
  - reduced database emulation;
  - removed fake claim/barrier semantics;
  - added required top-level module docstring.

- `tests/integration/orchestration/test_pipeline_progress_reconciliation.py`
  - added required top-level module docstring;
  - added real PostgreSQL extraction-attempt barrier coverage.

No production code was changed.

Focused unit regression:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest \
  tests/unit/orchestration/test_campaign.py \
  tests/unit/orchestration/test_pipeline_progress.py \
  tests/unit/orchestration/test_progress_wiring.py \
  tests/unit/orchestration/test_raw_checkpoint_operator.py \
  tests/unit/orchestration/test_scheduled_selection_pilot.py \
  tests/unit/orchestration/test_submission_log.py \
  tests/unit/orchestration/test_submission_pacing.py \
  tests/unit/orchestration/test_tasks.py \
  -q
```

PostgreSQL orchestration verification:

```bash
source activate-airflow.sh -q

python -m pytest \
  tests/integration/orchestration/test_orchestration_database_bootstrap.py \
  tests/integration/orchestration/test_pipeline_progress_reconciliation.py \
  tests/integration/orchestration/test_submission_pacing.py \
  -q
```

**T6 is complete.** Local verification on 2026-10-07:

```text
focused orchestration unit suite
69 passed in 4.34s

orchestration PostgreSQL integration suite
5 passed in 0.92s
```

Commands used:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest \
  tests/unit/orchestration/test_campaign.py \
  tests/unit/orchestration/test_pipeline_progress.py \
  tests/unit/orchestration/test_progress_wiring.py \
  tests/unit/orchestration/test_raw_checkpoint_operator.py \
  tests/unit/orchestration/test_scheduled_selection_pilot.py \
  tests/unit/orchestration/test_submission_log.py \
  tests/unit/orchestration/test_submission_pacing.py \
  tests/unit/orchestration/test_tasks.py \
  -q

python -m pytest \
  tests/integration/orchestration/test_orchestration_database_bootstrap.py \
  tests/integration/orchestration/test_pipeline_progress_reconciliation.py \
  tests/integration/orchestration/test_submission_pacing.py \
  -q
```

### T7 — Preserve legitimate external-boundary isolation

- [x] Keep CDS client/network fakes where they prevent real provider calls while production extraction logic remains under test.
- [x] Keep HTTP transport interception where it proves pacing occurs before provider submission.
- [x] Keep deterministic clock/time injection where timing is not the subject under test.
- [x] Keep temporary filesystem boundaries and failure injection where required to prove atomicity/recovery.
- [x] Document why each non-obvious fake/mock is legitimate.
- [x] Do not mock away scientific transformation logic in scientific correctness tests.

#### T7 implementation result — 2026-10-07

T7 confirms that the remaining external-boundary isolation is intentional and should **not** be removed merely to reduce the number of monkeypatches/fakes.

##### CDS provider/network boundary

`tests/unit/pipeline/test_extract.py` keeps a fake `cdsapi.Client` because a unit test must not submit live Copernicus requests.

The production behavior that remains under test includes:

- ERA5-Land retrieval-context construction;
- normalized request identity;
- CDS payload generation;
- variable selection;
- provider-name normalization;
- temporary NetCDF handling;
- extraction logging.

The fake provider only supplies a deterministic network response artifact.

A module-level docstring was added to state this boundary explicitly.

##### CDS pacing / HTTP transport boundary

`tests/unit/pipeline/test_cds_pacing_chunks.py` keeps HTTP interception and provider fakes.

These are required to prove that:

- completed retrieval chunks are reused after a pacing deferral;
- corrupt cached chunk identity fails closed;
- the submission reservation occurs before an actual provider POST;
- internal provider retry attempts must acquire a new pacing slot;
- the pinned `cdsapi` retry path is intercepted before a second provider POST.

The production paced-session, chunk-cache, request-identity, and deferral logic remain under test.

The module-level docstring was expanded to explain this explicitly.

##### Clock and deterministic time inputs

Fixed datetimes in pacing, campaign, and task tests remain appropriate where elapsed wall-clock time is not itself the subject under test.

No test was found where replacing real wall-clock time hides the production timing policy being asserted.

##### Filesystem boundary and failure injection

`tests/unit/pipeline/test_storage.py` continues to use `tmp_path` and a redirected `DATA_DIRECTORY`.

This is legitimate because the production storage functions still perform real:

- NetCDF writes/reads;
- Parquet writes/reads;
- artifact path generation;
- provenance handling;
- SHA-256 calculation;
- atomic replacement/cleanup.

Injected write failures are retained because they prove that failed writes do not leave partial artifacts.

A module-level docstring was added to distinguish filesystem isolation from fake storage behavior.

Operations reset tests also retain temporary filesystem roots and destructive-operation guards because the production cleanup functions execute against real temporary directories while environment/confirmation boundaries are isolated.

##### Airflow/runtime/OS boundaries

`tests/unit/orchestration/test_raw_checkpoint_operator.py` continues to isolate the runtime extraction function while exercising the production Airflow operator deferral/resume behavior.

`tests/unit/orchestration/test_tasks.py` continues to replace pipeline runtime calls only for wrapper observability/failure tests and replaces `shutil.disk_usage` only as an operating-system measurement boundary.

The `SimpleNamespace(free=...)` object is therefore retained: it represents an external OS result, not a production domain model.

A module-level docstring was added to `test_tasks.py` to document this boundary.

##### Scientific transformation boundary

No mock/fake was found inside the scientific correctness path in `tests/unit/pipeline/test_transform.py`.

The tests call production `transform_data()` directly with deterministic hourly source datasets. Expected daily totals, means, timestamps, units, missing-value behavior, and multi-grid independence are asserted independently.

Therefore T7 makes **no change** to scientific transformation logic or its tests. Scientific independence receives its dedicated review in T8.

##### Files changed in T7

- `tests/unit/pipeline/test_extract.py` — module-level boundary-isolation docstring added.
- `tests/unit/pipeline/test_cds_pacing_chunks.py` — module-level docstring expanded.
- `tests/unit/pipeline/test_storage.py` — module-level filesystem-isolation docstring added.
- `tests/unit/orchestration/test_tasks.py` — module-level runtime/OS-boundary docstring added.

No production code and no test assertions were changed.

Focused regression to run locally:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest \
  tests/unit/pipeline/test_extract.py \
  tests/unit/pipeline/test_cds_pacing_chunks.py \
  tests/unit/pipeline/test_storage.py \
  tests/unit/orchestration/test_raw_checkpoint_operator.py \
  tests/unit/orchestration/test_tasks.py \
  tests/unit/operations/test_reset_airflow_logs.py \
  tests/unit/operations/test_reset_airflow_runtime.py \
  tests/unit/operations/test_reset_data.py \
  tests/unit/operations/test_reset_all.py \
  -q
```

**T7 is complete.** Local focused regression on 2026-10-07:

```text
80 passed in 5.03s
```

The user command included the intended T7 files and completed successfully.

### T8 — Verify scientific tests independently

- [x] Confirm transformation tests call production transformation functions/classes.
- [x] Confirm expected values are derived from explicit scientific contracts rather than by calling/reimplementing the production algorithm.
- [x] Protect UTC and UTC+7 timestamp semantics.
- [x] Protect accumulation versus mean behavior.
- [x] Protect unit conversions.
- [x] Protect grid-point behavior.
- [x] Protect missing values as missing values, not zero.
- [x] Use small deterministic datasets with independently understandable expected results.
- [x] Add/update module-level docstrings where files are revised.

#### T8 implementation result — 2026-10-07

The scientific transformation and validation tests were reviewed directly against the production transformation/validation modules and the ERA5-Land scientific contracts.

##### Production subject under test

`tests/unit/pipeline/test_transform.py` calls production `transform_data()` directly.

`tests/unit/pipeline/test_validation.py` calls production `validate_raw_dataset()` and `validate_transformed_dataset()` directly.

No scientific correctness assertion is satisfied by a fake transformer or fake validator.

##### Independent expected values

The transformation tests use explicit expected outputs that are understandable without calling production helpers:

- local day midnight 2022-01-01 maps to `valid_time=2021-12-31 17:00:00` and `valid_time_local=2022-01-01 00:00:00`;
- 24 hourly precipitation increments of 0.001 m produce 24.0 mm/day;
- the deterministic SSRD source profile produces 86,400 J/m²/day;
- Celsius conversion is asserted explicitly as 300 K -> 26.85 °C and 295 K -> 21.85 °C;
- a 0..23 °C local-hour profile has an independently obvious arithmetic mean of 11.5 °C;
- the two-grid case has explicitly different totals/means per grid point;
- missing accumulated boundary input remains missing rather than becoming zero;
- negative accumulated output remains negative and emits the required warning.

##### Cumulative source fixture review

`_era5_land_cumulative_value()` is retained.

It is **not** the implementation-under-test. It generates a deterministic ERA5-Land-style accumulated source series that resets on the UTC day boundary. The production function still has to combine UTC-midnight and local-midnight boundaries to derive a UTC+7 local-day total.

A detailed docstring was added to this helper so its role cannot be confused with a copied daily-aggregation algorithm.

The SSRD expected assertion was changed from the expression `24 * 3600.0` to the explicit independently readable value `86_400.0`.

##### Fixture ownership and deprecated bbox cleanup

`test_transform.py` previously used the historical/deprecated project bbox `N5.2/W96.1/S3.4/E97.6` even though production configuration was not under test.

That bbox was replaced with an explicit minimal scientific fixture:

```text
north = 4.6
south = 4.5
west  = 96.8
east  = 96.9
```

This fixture directly contains the one/two grid points used by the scientific tests and makes the test independent from deployment configuration.

`test_validation.py` already used explicit minimal point/two-by-two grid fixtures, which is appropriate because validator logic—not the configured Aceh extent—is the subject under test.

##### UTC and local-day semantics

Scientific tests explicitly protect both required timestamps:

- `valid_time` remains the UTC instant corresponding to the local-day boundary;
- `valid_time_local` remains UTC+7 local midnight.

Tests also reject incomplete local days and the first ERA5-Land local day when the required previous UTC context would predate dataset availability.

##### Accumulation versus state/mean behavior

The suite distinguishes:

- accumulated variables: `tp`, `ssrd`;
- daily mean/state variables: `t2m`, `d2m`, `swvl1`.

Accumulated totals and daily means are asserted separately and are also verified across multiple grid points.

##### Units

The suite explicitly protects:

- `tp` -> mm/day;
- `t2m`, `d2m` -> °C;
- `swvl1` -> m³/m³;
- `ssrd` -> J/m²/day.

##### Grid behavior

The multi-grid transformation test proves that two grid points with different source profiles retain independent daily totals/means.

Validation tests independently protect regular 0.1° grid spacing, stable grid membership, rejection of off-grid coordinates, and duplicate grid/time observations.

##### Missing values

Scientific tests retain missing observations as missing values:

- null accumulated boundary -> missing daily accumulation;
- missing state sample invalidates a supposedly complete daily mean;
- transformed validation allows missing climate values where the contract permits them;
- missing values are never replaced with zero.

##### Files changed in T8

- `tests/unit/pipeline/test_transform.py`
  - added module-level scientific-independence docstring;
  - replaced deprecated project bbox with an explicit minimal scientific fixture;
  - documented the cumulative source-fixture helper;
  - made the SSRD expected total explicit.

- `tests/unit/pipeline/test_validation.py`
  - added module-level docstring explaining minimal fixture ownership and direct production validation.

No production scientific code was changed.

Focused regression to run locally:

```bash
source activate-airflow.sh -q

python -m pytest \
  tests/unit/pipeline/test_transform.py \
  tests/unit/pipeline/test_validation.py \
  -q
```

**T8 is implementation-complete pending focused local pytest confirmation.**

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
source activate-airflow.sh -q
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
