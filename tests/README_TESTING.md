# Pipeline Testing

This document defines the testing strategy for the Weather ETL pipeline in its current ERA5-Land development stage.

The active scientific and operational path is:

```text
CDS ERA5-Land hourly RAW
        ↓
RAW validation
        ↓
RAW NetCDF checkpoint
        ↓
local-day transformation (Asia/Jakarta, UTC+07:00)
        ↓
transformed validation
        ↓
Parquet checkpoint
        ↓
idempotent PostgreSQL load
```

The pipeline is designed to remain executable and testable without Apache Airflow. Airflow will later orchestrate these pipeline boundaries, but core data-processing behavior must be protected by ordinary pytest tests first.

---

## Testing Goals

Testing must protect both software correctness and scientific correctness.

A passing test suite should give confidence that the pipeline does not silently break any of the following contracts:

- provider variable naming;
- CDS API name normalization;
- hourly RAW data integrity;
- temporal continuity;
- ERA5-Land spatial grid expectations;
- local-day UTC+7 semantics;
- accumulated-variable aggregation rules;
- daily mean aggregation rules;
- unit conversion;
- transformed schema;
- deterministic artifact naming;
- RAW immutability;
- checkpoint/resume behavior;
- deterministic request identity;
- idempotent PostgreSQL loading.

---

## Testing Philosophy

The project follows these principles:

- Prefer `pytest`.
- Prefer small test functions over large test classes.
- Keep tests deterministic and offline-first.
- Use the smallest fixture that proves the behavior.
- Prefer real pipeline functions and data structures.
- Mock only external systems or expensive I/O boundaries.
- Do not require Airflow for core ETL tests.
- Do not require live CDS or ARCO access for routine unit tests.
- Keep scientific assumptions explicit in test names and expected values.
- A regression in units, timestamps, aggregation, or missing-value semantics is a correctness failure, even if the Python code still runs.

The test suite is divided conceptually into three layers:

```text
Unit tests
    ↓
Pipeline integration tests
    ↓
Airflow orchestration tests
```

Airflow tests are intentionally separate because DAG behavior and ETL business logic have different responsibilities.

---

## Current Scientific Contract Under Test

### Active ERA5-Land variables

Provider/native variable codes are the primary pipeline contract:

```text
tp
 t2m
 d2m
 swvl1
 ssrd
```

Long CDS API names are external API details and may only appear where API mapping or normalization is being tested.

Expected meanings:

```text
tp    = total precipitation
t2m   = 2 metre temperature
d2m   = 2 metre dewpoint temperature
swvl1 = volumetric soil water layer 1
ssrd  = surface solar radiation downwards
```

### Daily aggregation contract

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

Local-day timezone:

```text
Asia/Jakarta
UTC+07:00
```

Local midnight corresponds to:

```text
17:00 UTC on the previous UTC date boundary
```

For ERA5-Land accumulated fields, tests must protect the boundary-value formula rather than replacing it with hourly differencing.

### Output units

```text
tp    → mm/day
t2m   → degC
d2m   → degC
swvl1 → m3/m3
ssrd  → J/m2/day
```

### Transformed schema

The expected transformed DataFrame columns are:

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

Provider variable codes remain primary in transformed data.

---

## Directory Structure

The formal test layout is:

```text
tests/
├── unit/
└── integration/
```

Current unit test modules include:

```text
test_config_requests.py
test_extract.py
test_extract_cds_normalization.py
test_load.py
test_storage.py
test_transform.py
test_utils_request_validation.py
test_validation.py
```

Keep one implementation responsibility aligned with one test module where practical.

---

## Unit Test Responsibilities

### `test_config_requests.py`

Protects default request configuration and source-specific request construction.

Tests should verify that configuration remains typed and uses supported provider variables.

### `test_extract.py`

Protects extraction helpers that can be tested without live network access.

Do not make routine unit tests depend on the availability of CDS or ARCO.

### `test_extract_cds_normalization.py`

Protects the CDS API boundary.

Important behaviors include:

- normalizing `time` to `valid_time` where necessary;
- converting CDS API long variable names back to provider codes;
- preserving provider codes when the downloaded dataset already exposes them.

### `test_utils_request_validation.py`

Protects request validation and deterministic request identity.

Current important contracts include:

- provider variable names are accepted;
- CDS long API names are rejected as internal request variables;
- duplicate variables are rejected;
- variable signatures are deterministic;
- request keys are stable when variable order changes;
- request keys change when the logical partition identity changes.

### `test_transform.py`

Protects scientific transformation behavior.

Tests must cover, where applicable:

- UTC and local-day timestamps;
- accumulated-variable boundary formula;
- temperature conversion from Kelvin to Celsius;
- `swvl1` remaining in m3/m3;
- source metadata such as `expver` and `number` not leaking into transformed observations.

When P15 introduces feature-dataset logic, add new tests without weakening these transformation contracts.

### `test_validation.py`

Protects RAW and transformed data contracts.

Current coverage includes:

- continuous hourly RAW time steps;
- rejection of missing hourly steps;
- ERA5/ERA5-Land grid resolution expectations;
- UTC+7 transformed temporal contract;
- rejection of missing local days;
- rejection of duplicate grid records;
- stable transformed spatial membership;
- rejection of off-grid coordinates.

Validation tests should contain both positive and negative cases.

### `test_storage.py`

Protects artifact persistence and P10 checkpoint/resume behavior.

Current important contracts include:

- RAW NetCDF can be saved and converted back to a DataFrame;
- artifact paths are deterministic;
- RAW checkpoints are immutable and cannot be silently overwritten;
- an existing RAW checkpoint can be loaded for resume.

Storage tests should use temporary directories and must never write into the repository's real `data/` directory.

### `test_load.py`

Protects P11 idempotent database-load preparation and metadata identity.

Current important contracts include:

- provider columns are mapped to descriptive daily database columns;
- an existing `request_key` reuses the existing `dataset_id`;
- database-load semantics are UPSERT-oriented rather than blind append.

Routine unit tests should not require a live PostgreSQL server unless explicitly written as database integration tests.

---

## P10 Testing Contract — Checkpoint / Resume

P10 is considered protected only when tests preserve these behaviors:

```text
same logical request
→ same RAW artifact path
→ same transformed artifact path
```

RAW rules:

```text
RAW does not exist → create it
RAW exists         → resume it
RAW exists         → never silently overwrite it
```

Resume must not bypass validation.

Tests should distinguish artifact existence from artifact validity.

Atomic-write implementation details may be tested where useful, but tests should primarily protect the externally observable behavior: incomplete writes must not become valid final checkpoints.

---

## P11 Testing Contract — Idempotent Database Load

P11 protects two levels of idempotency.

### Metadata identity

`request_key` represents the logical partition using:

```text
source
dataset
start_date
end_date
variable set
bbox
```

Variable ordering must not create a different logical identity.

### Observation identity

Database detail rows are logically unique by:

```text
dataset_id
+ valid_time
+ latitude
+ longitude
```

A retry must update/reuse the same logical observation rather than append a duplicate.

For database integration tests, the strongest verification is that running the same load twice leaves no duplicates under this key.

---

## Integration Tests

Integration tests should verify boundaries between real pipeline components while keeping external dependencies controlled.

Preferred examples:

```text
synthetic RAW dataset
→ validate
→ save RAW checkpoint
→ load checkpoint
→ transform
→ validate transformed data
→ save Parquet
```

or, with an isolated test database:

```text
transformed fixture
→ load
→ load same fixture again
→ verify same dataset identity
→ verify no duplicate observations
```

Integration tests should:

- use temporary directories;
- use isolated database resources when database behavior is under test;
- avoid live provider downloads by default;
- verify stage handoff contracts rather than merely checking that no exception was raised.

Do not define ARCO as the only integration entrypoint. CDS ERA5-Land is the active development path and must be treated as a first-class source.

---

## Airflow Testing

Airflow is not required for core pipeline testing.

When DAGs are added, Airflow-specific tests should focus on:

- DAG import/parse success;
- task IDs;
- dependency wiring;
- schedule/data interval configuration;
- retry configuration;
- pools/concurrency where applicable;
- passing artifact references or small control metadata between tasks.

Airflow tests must not duplicate scientific transformation tests.

The intended boundary is:

```text
Airflow
  ↓ orchestrates
pipeline.extract_or_resume()
  ↓
pipeline.transform_or_resume()
  ↓
pipeline.load()
```

Airflow decides *when* and *in what order* these run. Pipeline tests verify *what they do*.

---

## P15 Guardrail

P15 — Daily Feature Dataset is the next modelling-data stage.

Testing must preserve an important methodological distinction:

> The five available ERA5-Land variables are candidate variables, not a preselected clustering feature set.

Do not write tests that assume all five variables must become clustering inputs.

P15 tests should initially focus on creating a correct daily modelling dataset from validated transformed observations. Selection of the useful feature subset belongs to later empirical evaluation, especially P17.

P16 wet-day identification and P17 parameter-contribution evaluation must remain separate concerns.

---

## Test Data Rules

Use the smallest fixture that proves the behavior.

Prefer:

- one to three local days;
- one or a few grid cells;
- deterministic numeric values;
- explicit NaN cases when missing-data semantics are under test;
- timestamp values chosen to make UTC/local-day boundaries obvious.

Do not globally replace missing weather values with zero in test fixtures unless the specific test is intentionally proving such behavior.

When testing accumulated ERA5-Land variables, fixture values should make the expected boundary formula easy to calculate manually.

---

## Negative Testing

Scientific validation requires failure tests as well as success tests.

Examples of conditions that should be rejected when relevant:

```text
missing hourly timestep
wrong spatial resolution
wrong UTC/local offset
missing requested local day
duplicate transformed grid record
off-grid coordinate
unsupported variable
long CDS API name used as internal provider variable
duplicate request variables
attempted RAW overwrite
```

A test suite containing only happy-path cases is insufficient for this project.

---

## Running Tests

Run all tests:

```bash
pytest
```

Run unit tests only:

```bash
pytest tests/unit
```

Run integration tests only:

```bash
pytest tests/integration
```

Run the current ERA5-Land/P9-P14/P10-P11 focused suite:

```bash
pytest \
  tests/unit/test_utils_request_validation.py \
  tests/unit/test_extract_cds_normalization.py \
  tests/unit/test_transform.py \
  tests/unit/test_validation.py \
  tests/unit/test_storage.py \
  tests/unit/test_load.py -v
```

The latest verified result for that suite before P15 work was:

```text
31 passed
3 dependency deprecation warnings
```

Warnings from external dependencies should be reviewed, but they are not equivalent to test failures unless they affect pipeline correctness.

---

## Definition of Done for New Pipeline Behavior

A new pipeline behavior should not be considered complete solely because a manual run succeeds.

Where practical, completion requires:

```text
implementation
+ focused unit tests
+ negative cases where relevant
+ runtime sanity check for external I/O behavior
+ documentation update when the data contract changes
```

For scientific changes, expected values should be traceable to an explicit formula, unit conversion, timestamp rule, or dataset contract.

---

## Current Testing Baseline

The completed pipeline foundations currently protected by tests are:

```text
[x] P9  provider/native variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
```

P15 and later modelling-data behavior must extend this baseline without changing its scientific meaning unless the underlying contract is deliberately revised and documented.
