# AIRFLOW CODING GUIDELINES

This document defines how Airflow code is written for this repository. It is
deliberately limited to DAGs, task wrappers, Airflow context, configuration,
logging integration, and DAG tests.

Use [`../AGENTS.md`](../AGENTS.md) for pipeline architecture and scientific,
artifact, validation, provenance, persistence, and testing contracts. Use
[`../README.md`](../README.md) for current runtime behavior. Use
[`AIRFLOW_BEST_PRACTICE.md`](AIRFLOW_BEST_PRACTICE.md) for orchestration policy.
Do not copy those contracts into DAG code or redefine them here.

## 1. Keep DAG Files Thin

DAG files belong in `orchestration/dags/`, and thin Airflow task wrappers belong
in `orchestration/tasks/`. They should contain only:

- DAG metadata and schedule configuration;
- Airflow task definitions;
- dependency wiring;
- the interval-to-request adapter;
- retry, timeout, pool, and alert configuration;
- thin calls into the existing pipeline runtime.

Do not put xarray/Pandas transformations, NetCDF/Parquet manipulation,
scientific formulas, validation rules, SQL persistence, checkpoint logic, or
provider clients in a DAG file.

## 2. Use the Existing Runtime Boundaries

Task wrappers must delegate to the methods on `pipeline.runtime.WeatherPipeline`:

```text
ensure_raw_checkpoint()
ensure_transformed_checkpoint()
load()
```

Do not translate every internal function into an Airflow task. In particular,
do not add separate validation tasks, because validation is part of the resume
contract of the first two boundaries.

A wrapper should do only four things:

1. obtain the logical request or artifact reference;
2. add Airflow logging context;
3. build or call the Airflow-neutral runtime entrypoint for one boundary;
4. return small control metadata or allow an exception to propagate.

`ensure_raw_checkpoint()` currently returns `RawWeatherDataset`, `DatasetArtifact`,
and `LoadedWeatherDataset`; `ensure_transformed_checkpoint()` returns
`TransformedWeatherDataset` and `DatasetArtifact`. These values contain xarray
or Pandas data and must not be passed through XCom.

Each downstream wrapper must reconstruct its required in-memory input from the
deterministic checkpoint through pipeline-owned storage and validation. If the
current method signature is not directly suitable for a separate worker
process, add a small Airflow-neutral entrypoint to `pipeline/runtime.py` that
accepts a `WeatherRequest` or artifact reference and performs that rehydration.
Do not put rehydration, checkpoint, or validation logic in the DAG, and do not
create a competing Airflow-specific pipeline model.

## 3. Convert Airflow Intervals in One Pure Helper

Define one tested pure helper for converting Airflow's half-open interval into
the pipeline's inclusive date period. Keep timezone conversion explicit.

```python
from datetime import date, timedelta

import pendulum

PIPELINE_TIMEZONE = pendulum.timezone("Asia/Jakarta")


def inclusive_local_dates(
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
) -> tuple[date, date]:
    start = data_interval_start.in_timezone(PIPELINE_TIMEZONE)
    end = data_interval_end.in_timezone(PIPELINE_TIMEZONE)

    if any((start.hour, start.minute, start.second, start.microsecond)) or any(
        (end.hour, end.minute, end.second, end.microsecond)
    ):
        raise ValueError("Airflow data interval must use local-midnight boundaries")
    if end <= start:
        raise ValueError("Airflow data interval must be non-empty")

    return start.date(), end.date() - timedelta(days=1)
```

The helper must not use `logical_date` as a substitute for the data interval,
must not use the task's wall-clock execution date, and must not calculate the
UTC boundaries needed by ERA5-Land transformation.

Tests must cover at least a one-day interval, a multi-day interval, conversion
from a non-local input timezone, a non-midnight boundary, and an empty or
reversed interval.

## 4. Build Requests from Explicit Inputs

Construct the pipeline request from:

- the converted inclusive period;
- the explicitly configured active provider and dataset;
- the configured variable set;
- the configured bounding box.

Do not derive request identity from `run_id`, `dag_run_id`, task instance,
attempt number, current time, or a random value. Do not maintain a second
request-key algorithm in Airflow; call the pipeline's request model and identity
logic.

If the same request is needed by several tasks, pass a small serialized request
specification or `request_key`, then reconstruct deterministic references using
pipeline code.

## 5. Prefer TaskFlow Without Hiding Data Movement

The TaskFlow API is appropriate for small typed orchestration values. Function
signatures and return values should make task dependencies visible.

Do not rely on implicit XCom serialization for large Python objects. A task
return value must be a small JSON-serializable mapping containing only the
fields needed downstream.

Avoid returning:

- Pandas or Polars DataFrames;
- xarray Datasets;
- bytes or open file handles;
- NetCDF/Parquet contents;
- database connections or clients;
- arbitrary pipeline objects whose serialization contract is unclear.

## 6. Keep Imports Safe

At DAG module import time, do not:

- call CDS or any other network service;
- connect to PostgreSQL;
- inspect or create artifacts;
- load `.env` with side effects;
- enumerate large directories;
- instantiate heavyweight clients;
- execute a pipeline boundary.

Imports may define constants, pure helper functions, DAG/task declarations,
and lightweight configuration references. Create clients and perform I/O only
during task execution.

Where heavy optional dependencies materially slow scheduler parsing, import
them inside the runtime wrapper. Do not use local imports to conceal an
architectural dependency from `pipeline/` back to Airflow.

## 7. Configure DAGs Explicitly

Every operational DAG must declare, rather than inherit ambiguously:

- timezone-aware `start_date` in `Asia/Jakarta`;
- schedule;
- `catchup=False`;
- `max_active_runs`;
- task retries and retry delays;
- task execution timeouts;
- `cds_api` pool for extraction;
- owner/tags and failure callback when the deployment uses one.

Avoid one global `default_args["retries"]` when task failure modes differ. It
is acceptable to share genuinely common metadata, but retry behavior should be
visible at each boundary.

Do not place a dynamic `datetime.now()` in DAG metadata. Use a fixed,
timezone-aware start date.

## 8. Keep Configuration and Secrets at the Boundary

Read credentials from Airflow Connections or the deployment's approved secrets
backend. Small non-secret orchestration settings, such as availability lag or
pool name, may come from environment-backed configuration or Airflow Variables
when justified.

Validate required configuration at task runtime with a clear error that names
the missing setting but never its secret value. Pass ordinary pipeline
configuration explicitly after resolving Airflow-specific sources.

Do not make core pipeline modules import `airflow.models.Variable`, hooks, or
other Airflow classes.

## 9. Let Failures Reach Airflow

Catch an exception only when the wrapper can add useful orchestration context,
perform safe cleanup, or translate a known transient infrastructure error.
Preserve the original chain:

```python
try:
    return call_pipeline_boundary()
except KnownInfrastructureError as exc:
    raise AirflowBoundaryError(
        f"Extraction failed for request_key={request_key}"
    ) from exc
```

Never use `except Exception: pass`, return `None` to represent failure, or mark
a task successful after incomplete processing. Do not convert scientific or
contract failures into retryable Airflow exceptions.

## 10. Add Orchestration Context to Logs

Use Python logging and allow Airflow to capture it. Log identifiers and
summaries, not datasets.

Recommended wrapper context includes:

```text
dag_id
task_id
run_id
try_number
data_interval_start
data_interval_end
request_key
artifact path
processed/skipped count
```

`run_id` is useful for correlation only. Its presence in a log must not affect
the request, artifact path, or persistence identity.

Do not log credentials, Airflow Connection payloads, CDS responses, full
DataFrames, NetCDF/Parquet contents, or excessively large request structures.

## 11. Name DAGs and Tasks by Runtime Intent

Use stable names that communicate orchestration intent and checkpoint
postconditions. For the current five-task graph, prefer:

```text
build_monthly_requests
attempt_raw_checkpoint [YYYY-MM]
resolve_raw_checkpoint [YYYY-MM]
ensure_transformed_checkpoint [YYYY-MM]
load [YYYY-MM]
```

The pipeline runtime API beneath these tasks uses
`ensure_raw_checkpoint()`, `ensure_transformed_checkpoint()`, and `load()`.
The initial attempt task is intentionally named `attempt_raw_checkpoint`
because a first-pass failure is converted to bounded control metadata instead
of immediately failing the mapped task. Successful first results continue
directly through `resolve_raw_checkpoint` as pass-through results. Only failed
first results wait for every expected first attempt to reach terminal extraction
state before retrying. A successful `resolve_raw_checkpoint` guarantees that
downstream work receives valid RAW checkpoint metadata.

Avoid generic or version-churn names such as `task1`, `process_data`,
`final_task`, or `fixed_v2`. Do not encode a run date in a DAG or task ID.

Use one parameterized DAG when the workflow is the same. Do not generate many
nearly identical DAGs for dates, variables, or bounding boxes unless an
explicit design decision justifies distinct operational ownership.

## 12. Use Sensors Only for Readiness

If an availability sensor is introduced, it must:

- be bounded by a timeout;
- use deferrable or `reschedule` behavior for long waits;
- have a deliberate polling interval;
- return only readiness metadata;
- avoid downloading or transforming data;
- fail clearly when the source remains unavailable.

Do not add a sensor while the conservative availability-lag policy is
sufficient.

## 13. Test DAG Code Without Running the Scheduler

Airflow-specific unit tests should load the DAG in an isolated test environment
and verify:

1. the DagBag has no import errors;
2. the expected DAG ID and six task identities exist;
3. dependencies preserve the initial RAW-checkpoint pass, retry barrier,
   RAW-checkpoint resolver, transformed-checkpoint boundary, and load;
4. monthly mapped tasks expose the expected `YYYY-MM` named-map label;
5. timezone, schedule, fixed start date, `catchup`, and `max_active_runs` match
   policy;
6. both RAW-checkpoint operator passes use the `cds_api` pool;
7. retry and timeout values are correct per task;
8. interval conversion produces the intended inclusive dates;
9. task wrappers delegate to the intended runtime entrypoint and propagate errors;
10. XCom payloads contain only approved metadata;
11. importing the DAG performs no network, database, or artifact I/O.

Do not retest scientific formulas, data schemas, checkpoint validation, or
database UPSERT behavior through Airflow. Those remain core unit/integration
tests and must continue to pass in an environment without Airflow.

## 14. Airflow Code Review Checklist

- [ ] The change is orchestration-specific and belongs in `dags/`.
- [ ] The wrapper delegates to a pipeline runtime boundary or its thin,
      Airflow-neutral rehydration entrypoint.
- [ ] No scientific, validation, storage, or persistence logic was copied.
- [ ] The interval mapping uses `Asia/Jakarta` and inclusive pipeline dates.
- [ ] Request/artifact identity is independent of the Airflow execution.
- [ ] XCom contains only small JSON-serializable metadata.
- [ ] Importing the DAG causes no external I/O.
- [ ] Retryable and deterministic failures remain distinguishable.
- [ ] Exceptions are not swallowed and preserve their cause.
- [ ] Credentials and datasets cannot leak through arguments or logs.
- [ ] DAG structure and policy tests were added or updated.
- [ ] Core pipeline tests still run without importing Airflow.
