# Pipeline Testing

> This document describes the testing strategy for the Weather ETL Pipeline. The primary goal is to ensure that every pipeline component behaves correctly in isolation and that the complete ETL workflow functions correctly before introducing Apache Airflow orchestration.

The project prioritizes simple, readable, and maintainable tests. Most tests are unit tests because they are fast, deterministic, and isolate failures effectively. A small number of integration tests are used to verify end-to-end pipeline behavior.

---

## Testing Philosophy

The project follows the following principles:

* Prefer Pytest over unittest.
* Tests are functions until they have a good reason not to be.
* One module should have one test file.
* Prefer real implementations whenever possible.
* Mock only external dependencies.
* Keep tests independent, readable, and repeatable.
* Unit tests should significantly outnumber integration tests.
* Apache Airflow orchestration will be tested separately after the Python ETL pipeline is stable.

The project follows a two-layer testing strategy:

* Unit Tests validate each pipeline module independently.
* Integration Tests verify that all pipeline modules work together as a complete ETL pipeline.

```text
                  Integration Tests
                           │
                           ▼

        Extract → Validate → Transform → Export
                           │
                           ▼
                        SUCCESS

        ▲            ▲             ▲            ▲
        │            │             │            │
     Unit         Unit          Unit         Unit
     Tests        Tests         Tests        Tests
```

---

## Directory Structure

```text
tests/
│
├── unit/
│   ├── test_extract.py
│   ├── test_validation.py
│   ├── test_transform.py
│   ├── test_storage.py
│   └── test_load.py
│
└── integration/
    └── test_run_pipeline.py
```

---

## Unit Tests

Each unit test validates a single pipeline component in isolation.

| Test File          | Purpose                                                                          |
| ------------------ | -------------------------------------------------------------------------------- |
| test_extract.py    | Verifies dataset downloading, spatial subsetting, and temporal subsetting logic. |
| test_validation.py | Validates dataset dimensions, variables, and required metadata.                  |
| test_transform.py  | Verifies dataframe transformation and aggregation logic.                         |
| test_storage.py    | Verifies artifact creation, persistence, loading, and deletion.                  |
| test_load.py       | Verifies export functionality such as CSV and Parquet generation.                |

### Unit Testing Principles

Unit tests should:

* Validate only one component at a time.
* Avoid depending on other pipeline modules.
* Prefer using small sample datasets.
* Avoid external network access whenever possible.
* Be fast and deterministic.
* Be readable and easy to maintain.

Example:

```text
save_raw_dataset()
        │
        ▼
   create artifact
        │
        ▼
   save NetCDF file
        │
        ▼
      SUCCESS
```

Only the behavior of `save_raw_dataset()` should be validated. Other pipeline modules are outside the scope of the test.

---

## Integration Tests

### test_run_pipeline.py

Integration tests execute the complete ETL workflow using the actual pipeline implementation.

### Pipeline Flow

```text
      Extract
         │
         ▼
      Validate
         │
         ▼
      Transform
         │
         ▼
    Save Artifacts
         │
         ▼
        Export
         │
         ▼
       SUCCESS
```

### Testing Strategy

Integration tests should:

* Execute the real pipeline implementation.
* Use temporary directories whenever possible.
* Mock only external dependencies when necessary.
* Verify that all artifacts are created successfully.
* Verify that every pipeline stage communicates correctly with the next stage.

External services may be mocked when required.

Examples:

* Remote dataset providers.
* Cloud storage services.
* Third-party APIs.

Internal pipeline modules should use their real implementations.

---

## Acceptance Criteria

The integration test succeeds only if all of the following conditions are satisfied:

* The pipeline completes without exceptions.
* The raw dataset artifact is created successfully.
* The processed dataset artifact is created successfully.
* The export artifact is created successfully.
* Dataset validation succeeds.
* Dataset transformation succeeds.
* The exported dataset contains the expected output.
* All generated artifacts can be loaded successfully.
* The complete pipeline executes successfully from Extract through Export.

```text
            Extract
               │
               ▼
            Validate
               │
               ▼
            Transform
               │
               ▼
             Export
               │
               ▼
            SUCCESS
               │
               ▼
       All artifacts exist
               │
               ▼
       All artifacts are valid
```

---

## Running Tests

Execute all tests:

```bash
pytest
```

Execute unit tests only:

```bash
pytest tests/unit
```

Execute integration tests only:

```bash
pytest tests/integration
```

Execute a specific test file:

```bash
pytest tests/unit/test_storage.py
```

Execute tests with coverage:

```bash
pytest --cov
```

---

## Future Direction

Apache Airflow will be introduced only after the Python ETL pipeline is considered stable.

The Airflow project will follow the Thin Orchestrator principle:

```text
            Apache Airflow
                   │
                   ▼
             Orchestration
                   │
                   ▼
          Metadata Communication
                   │
                   ▼
               ETL Pipeline
                   │
                   ▼
       Extract → Validate → Transform → Export
```

The ETL Pipeline remains responsible for all business logic and data processing, while Apache Airflow is responsible only for orchestration, scheduling, metadata communication, and task execution management.
