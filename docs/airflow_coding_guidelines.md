# Apache Airflow Coding Guidelines

## Documentation Role: HOW

This document explains **how to implement** the Airflow policies in DAGs, task wrappers, configurations, and tests. It owns coding examples and conventions, not application scope or operational policy decisions.

**Document owner:** Reusable implementation conventions (**HOW**).  
**Audience:** Developers writing DAGs, operators, task wrappers, and tests.  
**Scope:** Project-agnostic Airflow code patterns. For the *policy behind a rule*, reference [Airflow Best Practices](airflow_best_practice.md). Domain ETL, NLP, and persistence contracts are defined by each project.

## 1. Project structure

Suggested, adaptable layout:

```text
orchestration/
  dags/         # DAG definitions and dependency wiring
  tasks/        # Small Airflow adapters
  config/       # Non-secret validated settings
src/            # Framework-independent pipeline and domain code
tests/
  orchestration/
  pipeline/
```

Keep DAG definitions declarative. Place parsing, transformation, persistence, inference, and domain validation in `src/` or dedicated services. Thin adapters resolve execution context and call those components.

## 2. Naming conventions

- Use stable, descriptive snake_case IDs for DAGs and tasks.
- Name tasks by observable action (e.g., `extract_source`, `validate_output`, `load_records`), not implementation mechanics.
- Use meaningful tags and owners; avoid creating separate almost-identical DAGs for every input partition.
- Never build business identifiers from task attempts or execution IDs.

## 3. DAG implementation pattern

Declare a fixed timezone-aware start date, explicit schedule and `catchup`, and sensible active-run limits. Choose operator/task definitions compatible with the project's pinned Airflow version.

**Version compatibility rule:** Every nontrivial code example and project DAG template must state which Airflow major version and provider package versions it supports. Review examples when upgrading. The following pattern targets **Apache Airflow 2.x with the TaskFlow decorator API**; it is illustrative and uses placeholder runtime functions, not a copy-paste-ready application.

```python
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task  # Airflow 2.x TaskFlow API


@dag(
    dag_id="example_incremental_workflow",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["example"],
)
def example_incremental_workflow():
    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20),
    )
    def extract(**context) -> dict:
        from project_pipeline.runtime import extract_interval

        artifact = extract_interval(
            start=context["data_interval_start"],
            end=context["data_interval_end"],
        )
        return {"artifact_uri": artifact.uri}

    @task(execution_timeout=timedelta(minutes=20))
    def transform(metadata: dict) -> dict:
        from project_pipeline.runtime import transform_artifact

        result = transform_artifact(metadata["artifact_uri"])
        return {"artifact_uri": result.uri}

    transform(extract())


example_incremental_workflow()
```

**Note:** `project_pipeline` is a placeholder. The example assumes compatible Airflow 2.x releases that support the shown `schedule` argument. For Airflow 3.x, update decorators/imports against the installed public Task SDK and test context behavior.

## 4. Operators, TaskFlow, sensors, and dependencies

- Prefer supported TaskFlow APIs when exchanging small typed metadata; use established operators for specialized integrations.
- Wire dependencies explicitly and use TaskGroups to improve readability.
- Apply mapped-task bounds; avoid generating unbounded operator graphs.
- Implement long waits using bounded sensors, preferably deferrable or rescheduling where supported.
- Do not implement polling via long `sleep()` calls inside worker tasks.

## 5. Task inputs, outputs, and XCom

Return small JSON-serializable metadata: artifact URI, partition key, row count, or processing status. Persist large DataFrames, model outputs, binaries, and datasets outside Airflow, and pass stable pointers. Do not return database connections, API clients, credentials, or opaque Python objects.

A downstream task retrieves its input through domain-runtime code rather than trying to deserialize a large upstream object. Use domain-defined idempotency keys for writes and reruns.

## 6. Configuration patterns

- Resolve secrets via an Airflow Connection or approved secrets backend at task runtime.
- Keep small operational settings in validated configuration; avoid network lookups during DAG parse.
- Convert logical data intervals to domain date/partition formats in pure, tested helpers.
- Validate `dag_run.conf` or other manual parameters, including bounds on date ranges and source fan-out.
- Never make reusable pipeline libraries import Airflow configuration classes.

## 7. Exception handling

Configure retries, timeout, pools, and resource limits at the relevant task boundary following [Airflow policy](airflow_best_practice.md). In wrappers, preserve failure propagation:

```python
def invoke_pipeline(operation):
    try:
        return operation()
    except OSError as exc:
        # Preserve the root cause and let Airflow classify task failure.
        raise RuntimeError("Pipeline boundary failed") from exc
```

Use this wrapper only if added context helps; otherwise allow the original exception through. Do not broadly catch exceptions merely to return a success-shaped value.

## 8. Logging implementation

Use task-aware structured logging and concise identifiers. Log logical interval, source/partition key, output references, item counts, timing, and error class where available. Never log credentials, auth headers, raw sensitive records, or unredacted confidential prompts. Prefer log context over dumping complete payloads.

## 9. Testing implementation

Suggested test cases (use the test runner and utilities supported by the pinned version):

- Parse all DAGs in a controlled test environment and assert no import errors.
- Verify stable DAG/task IDs, dependencies, schedule, timezone, and configured resource controls.
- Assert no external I/O occurs during DAG import.
- Unit test pure interval conversion, partition helpers, and parameter validators.
- Mock domain-runtime entrypoints to check task delegation, output metadata, and error propagation.
- Assert XCom payloads are small and serializable.
- Exercise idempotent reruns and partial failure through integration tests against controlled storage.
- Keep domain pipeline tests independent of Airflow.

## 10. Code review checklist

- [ ] Implementation matches the project's pinned Airflow/provider versions; example compatibility is declared.
- [ ] DAG files remain thin; domain logic stays outside Airflow.
- [ ] Names, inputs, outputs, and dependencies are clear.
- [ ] Import is side-effect free.
- [ ] Inter-task metadata is small and durable outputs use external storage.
- [ ] Exceptions propagate and configured resource constraints are deliberate.
- [ ] Secrets and private data cannot leak into code or logs.
- [ ] DAG, wrapper, and integration tests cover the changed behavior.

## Documentation boundary

This document demonstrates **how to code** the [Airflow policies](airflow_best_practice.md); it does not redefine them. Application features, tool selection, and data contracts belong in the application architecture document. Resolve mismatches by updating the owning document and its references.
