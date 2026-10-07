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

**T3 is implementation-complete pending local pytest confirmation.**

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
