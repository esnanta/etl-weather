# Tests AGENTS

This document defines the testing rules for code under `tests/` and complements the project-level `../AGENTS.md`.

Authoritative references:

- `../AGENTS.md`
- `../Readme-pipeline.md`
- `../docs/AIRFLOW_BEST_PRACTICE.md`
- `../docs/AIRFLOW_CODING_GUIDELINES.md`
- `README_TESTING.md`

If this file conflicts with a broader project rule, follow the more specific testing rule here unless the project-level file explicitly defines a stricter scientific contract.

## Scope

Testing protects the Weather ETL pipeline before Airflow orchestration is introduced.

Current active development context:

```text
CDS ERA5-Land hourly RAW
→ local daily transformed dataset
→ PostgreSQL
```

Airflow will later orchestrate pipeline boundaries, but core tests must remain runnable without an Airflow scheduler or DAG runtime.

## Current Locked Contracts

Tests must preserve the currently accepted pipeline contracts unless a task explicitly revises them.

### Provider variable identity

Primary variable names are:

```text
tp
t2m
d2m
swvl1
ssrd
```

Do not rewrite tests to make CDS long API names the internal canonical names.

Long names may appear only where testing API mapping, normalization, human-readable descriptions, or legacy migration behavior.

### ERA5-Land daily semantics

Timezone:

```text
Asia/Jakarta (UTC+07:00)
```

Local midnight boundary:

```text
17:00 UTC
```

Accumulated variables:

```text
tp
ssrd
```

Daily mean variables:

```text
t2m
d2m
swvl1
```

Output units:

```text
tp    = mm/day
t2m   = degC
d2m   = degC
swvl1 = m3/m3
ssrd  = J/m2/day
```

Do not replace the established accumulated-variable boundary formula with hourly differencing in tests or fixtures.

### Transformed schema

Expected core transformed columns:

```text
valid_time
valid_time_local
latitude
longitude
tp
t2m
swvl1
ssrd
d2m
```

### Checkpoint and idempotency

P10 and P11 are completed contracts:

```text
same logical request → deterministic artifact path
RAW checkpoint       → immutable
existing checkpoint  → resume, then validate
same request_key     → same dataset identity
same observation key → UPSERT, not duplicate append
```

Observation identity:

```text
dataset_id + valid_time + latitude + longitude
```

## Testing Structure

Use the formal structure:

```text
tests/
├── unit/
└── integration/
```

Do not move ordinary core-pipeline tests into Airflow DAG tests.

### Unit tests

`tests/unit/` contains isolated tests for one module or one responsibility.

Typical responsibilities include:

- request/config validation;
- CDS normalization;
- scientific transformation;
- temporal validation;
- spatial validation;
- storage/checkpoint behavior;
- database-load preparation and idempotency.

### Integration tests

`tests/integration/` verifies multiple real pipeline components working together.

Use isolated resources and mock only external systems where necessary.

## Testing Style

- Use `pytest`.
- Prefer test functions over test classes unless a class materially improves organization.
- Keep tests small, deterministic, readable, and offline-first.
- Prefer real pipeline functions and domain objects.
- Mock external network/provider boundaries, not internal business logic.
- Keep one implementation responsibility aligned with one test module where practical.
- Do not rely on test execution order.
- Do not share mutable state between tests.
- Use explicit expected values rather than broad truthy assertions when scientific correctness is involved.

## Fixtures

Fixtures should be the smallest realistic data needed to prove a rule.

Prefer:

```text
1–3 local days
1–a few grid cells
small xarray.Dataset objects
small pandas.DataFrame objects
explicit timestamps
manually verifiable numeric values
```

Use temporary directories for generated artifacts.

Do not write test artifacts into the repository's real `data/` directory.

Do not require live CDS or ARCO downloads in routine tests.

## Scientific Test Requirements

Scientific correctness is part of software correctness.

When testing transformations, make expected values traceable to an explicit rule:

- aggregation formula;
- unit conversion;
- timezone offset;
- provider data contract;
- spatial grid rule.

Avoid tests that merely assert that a function returns a non-empty DataFrame.

For accumulated variables, choose fixture values whose expected daily result can be calculated manually.

For UTC/local tests, choose timestamps that visibly exercise the 17:00 UTC local-midnight boundary.

For missing values, preserve the distinction:

```text
0   = observed zero
NaN = missing/invalid observation
```

Do not globally fill NaNs with zero in fixtures or expected outputs unless the specific behavior being tested explicitly requires it.

## Positive and Negative Cases

Validation behavior should normally have both acceptance and rejection tests.

Important rejection cases include:

- missing hourly timestep;
- wrong spatial resolution;
- wrong UTC/local offset;
- missing requested local day;
- duplicate transformed grid record;
- off-grid coordinate;
- unsupported provider variable;
- CDS API long name used as an internal request variable;
- duplicate requested variables;
- attempted overwrite of an existing RAW checkpoint.

Do not weaken negative tests simply to make a new implementation pass.

## P10 Checkpoint/Resume Rules

Tests for storage/runtime resume behavior must protect these semantics:

```text
RAW missing  → create checkpoint
RAW exists   → resume checkpoint
RAW exists   → do not overwrite silently
```

Likewise for transformed Parquet:

```text
Parquet missing → transform and create
Parquet exists  → resume and validate
```

Artifact naming must remain deterministic for the same logical request.

A checkpoint being present does not remove the requirement to validate it before downstream use.

## P11 Database Idempotency Rules

Tests must treat `request_key` as the logical dataset-partition identity.

The request key must account for:

```text
source
dataset
period
variable set
bbox
```

Variable ordering alone must not produce a different key.

Database tests should distinguish:

```text
rows_processed
```

from:

```text
rows_inserted
```

because a retry may process rows through UPSERT without creating new rows.

When a real/isolated PostgreSQL integration test is used, loading the same transformed partition twice should result in:

```text
same logical dataset identity
0 duplicate observation keys
```

## Unit Test Module Guidance

### `test_config_requests.py`

Protect default typed request construction and supported source/dataset configuration.

### `test_extract.py`

Test extraction helpers that do not require live provider access.

### `test_extract_cds_normalization.py`

Protect normalization from CDS response naming to provider codes and `valid_time` semantics.

### `test_utils_request_validation.py`

Protect provider-name request validation, deterministic variable signatures, and deterministic request keys.

### `test_transform.py`

Protect local-day aggregation, accumulated-variable formula, daily means, unit conversion, and removal of source-only metadata.

### `test_validation.py`

Protect temporal and spatial contracts with both pass and fail cases.

### `test_storage.py`

Protect deterministic artifact paths, RAW immutability, NetCDF persistence, and checkpoint resume behavior.

### `test_load.py`

Protect provider-to-database daily-column mapping and idempotent dataset metadata behavior.

Do not describe `test_load.py` as CSV export testing; the active load target is PostgreSQL.

## Integration Test Rules

Integration tests should verify pipeline contracts across boundaries, for example:

```text
synthetic RAW
→ validation
→ RAW checkpoint
→ reload
→ transform
→ transformed validation
→ Parquet checkpoint
```

or:

```text
transformed fixture
→ PostgreSQL load
→ repeated load
→ duplicate check
```

Prefer isolated test infrastructure.

Do not make ARCO the sole integration entrypoint. CDS ERA5-Land is currently the active development path and must be covered as a first-class source.

## Airflow Testing

Airflow-specific testing begins when DAGs are implemented.

Airflow tests should cover orchestration only:

- DAG import/parse;
- task IDs;
- dependency graph;
- schedule/data interval behavior;
- retry configuration;
- concurrency/pools where configured;
- small metadata/artifact handoff.

Do not duplicate transformation, validation, unit-conversion, checkpoint, or database-idempotency logic inside DAG tests.

The intended responsibility split is:

```text
Airflow decides when/what order
Pipeline decides how data is processed
```

## P15 Guardrail

P15 — Daily Feature Dataset is the next stage.

When adding P15 tests:

- treat `tp`, `t2m`, `d2m`, `swvl1`, and `ssrd` as candidate variables;
- do not encode an assumption that all five must be clustering features;
- keep daily feature-dataset construction separate from P16 wet-day definition;
- keep feature contribution/subset evaluation separate for P17;
- do not let modelling-oriented tests redefine locked RAW/transformed scientific semantics.

## Execution Expectations

Routine tests should run offline:

```bash
pytest tests/unit
```

Use integration tests separately when needed:

```bash
pytest tests/integration
```

The currently verified focused baseline before P15 is:

```text
31 passed
3 external dependency deprecation warnings
```

A warning from an external dependency is not automatically a pipeline failure, but new warnings should be reviewed rather than ignored blindly.

## Definition of Done for Tests

For a new pipeline behavior, prefer this completion standard:

```text
focused unit test
+ negative case when meaningful
+ deterministic fixture
+ no unnecessary external dependency
+ integration/runtime verification when external I/O behavior matters
```

Do not mark a scientific or persistence behavior complete based only on a successful manual run.

## Current Priority

Protect the completed baseline while extending toward P15:

```text
[x] P9  provider/native variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[ ] P15 daily feature dataset
```

New tests should extend this baseline, not silently alter it.
