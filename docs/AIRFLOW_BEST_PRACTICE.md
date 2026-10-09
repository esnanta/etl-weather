# AIRFLOW BEST PRACTICES

This document defines the Airflow orchestration policy for this repository.
It extends, but does not redefine, the pipeline architecture and runtime
contracts in [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md).

When the documents differ, use the following authority order:

1. `AGENTS.md` for architecture and engineering contracts;
2. `README.md` for current pipeline behavior and usage;
3. this document for Airflow orchestration policy;
4. `AIRFLOW_CODING_GUIDELINES.md` for Airflow-specific implementation rules.

Scientific formulas, schemas, units, artifact layout, checkpoint semantics,
provenance, and PostgreSQL idempotency are intentionally not repeated here.

## 1. Airflow's Responsibility

Airflow owns:

- scheduling and logical data intervals;
- task dependencies and workflow state;
- retry, timeout, pool, and concurrency policy;
- Connections, secrets, sensors, alerting, and operational metadata.

The pipeline owns all ETL behavior. DAG code must call the existing pipeline
runtime rather than reimplement extraction, validation, transformation,
storage, or loading.

## 2. Task Boundaries

The Airflow DAG keeps retry control separate from pipeline checkpoint
postconditions and maps one depth-first TaskGroup per calendar month:

```text
build_monthly_requests
        |
        +--> monthly_etl[YYYY-MM]
                  |
                  v
        attempt_raw_checkpoint
                  |
                  v
        resolve_raw_checkpoint
                  |
                  v
        ensure_transformed_checkpoint
                  |
                  v
                 load
```

A RAW-complete month may transform and load while sibling months are still
waiting for an earliest-start limit, CDS pacing, provider queueing, or bounded
extraction retry. Successful RAW attempts pass through the resolver immediately.
Only failed first attempts wait for the durable first-pass extraction barrier
before retry.
The pipeline runtime beneath those orchestration tasks still owns the three
postcondition boundaries:

```text
ensure_raw_checkpoint
        ↓
ensure_transformed_checkpoint
        ↓
load
```

Validation stays inside these runtime operations:

```text
ensure_raw_checkpoint   → RAW checkpoint validation
ensure_transformed_checkpoint → transformed checkpoint validation
load                → transactional idempotency checks and persistence
```

Do not create separate `validate_raw` or `validate_transformed` Airflow tasks.
Doing so would allow resumed checkpoints to bypass the validation guaranteed
by the runtime boundary.

An interval-to-request adapter may be a small pure helper or a lightweight
orchestration task. It must not become an alternative implementation of
pipeline request partitioning.

The current runtime methods use in-memory xarray/Pandas objects within one
process. Those objects must not cross Airflow task boundaries. A downstream
task must rehydrate its input from the deterministic checkpoint through
Airflow-neutral pipeline code. If a small runtime entrypoint is needed for that
handoff, add it to `pipeline/`; do not implement checkpoint loading or
validation in the DAG.

## 3. Scheduling Timezone and Data Intervals

Every DAG must declare `Asia/Jakarta` explicitly. Do not depend on the Airflow
default timezone, the host timezone, or `datetime.now()`.

Airflow supplies half-open intervals:

```text
[data_interval_start, data_interval_end)
```

The pipeline accepts an inclusive date range. The adapter must therefore map a
local-midnight interval as follows:

```text
pipeline_start_date = local(data_interval_start).date()
pipeline_end_date   = local(data_interval_end).date() - 1 calendar day
```

For example:

```text
Airflow interval : [2026-09-01 00:00, 2026-09-02 00:00) Asia/Jakarta
Pipeline period  : 2026-09-01 through 2026-09-01 inclusive
```

The adapter must reject an empty interval and any scheduled boundary that is
not local midnight. It must not calculate ERA5-Land UTC accumulation
boundaries; that remains pipeline logic.

## 4. ERA5-Land Availability Policy

ERA5-Land-T daily updates are documented as available about five days behind
real time, and CDS updates do not occur at a fixed time of day. Consequently,
an operational run must not assume that yesterday's data is available.

The initial operational policy is a conservative six-completed-local-day lag.
Keep this value explicit in Airflow configuration and observable in logs; do
not bury it in scientific pipeline code. A run may process only an interval
whose end is no later than the configured eligibility boundary.

If the project later adopts availability polling, use a bounded, deferrable or
rescheduling sensor with a timeout. A sensor must check source readiness; it
must not duplicate extraction or hold a worker slot for a long wait.

## 5. Operational Scheduling and Historical Backfill

The operational DAG must use `catchup=False`. Airflow scheduler catchup is not
the historical bootstrap mechanism for this project.

Historical loading must be initiated explicitly with a reviewed date range.
The existing pipeline remains responsible for splitting long ranges into its
deterministic monthly request partitions.

Before a backfill, verify:

- the requested inclusive period and bounding box;
- expected request-partition count;
- CDS pool capacity and database capacity;
- available disk space;
- the intended behavior for failed partitions and reruns.

Do not create thousands of daily DagRuns merely to populate historical data.

## 6. Retry and Failure Policy

Retry only failures that can recover without a code or data change. Retry
configuration must be explicit per task rather than inherited blindly from one
global value.

| Boundary | Retry candidates | Fail without repeated retry |
| --- | --- | --- |
| `ensure_raw_checkpoint` | CDS throttling, network timeout, temporary service failure | invalid request, invalid/resumed RAW checkpoint, deterministic validation failure |
| `ensure_transformed_checkpoint` | only a demonstrated transient infrastructure failure | scientific validation failure, changed RAW checksum, code/configuration error |
| `load` | temporary PostgreSQL connectivity or lock contention | schema/contract violation, invalid transformed artifact, deterministic data error |

A sensible initial baseline is three extraction attempts, no transformation
retry, and two load attempts, with bounded exponential backoff for retryable
failures. Treat these as deployment defaults, not as permission to catch and
retry every exception.

Every task must also have a finite execution timeout appropriate to its
workload. Airflow must receive the original failure: task wrappers must not
swallow exceptions or return a success status after an error.

## 7. Pools and Concurrency

All CDS retrieval tasks must use a dedicated `cds_api` pool. Start with one
slot unless measured behavior and CDS service policy justify a higher value.

Use `max_active_runs=1` for the first operational DAG. Increase it only after
confirming that deterministic artifacts, local storage, memory, database
connections, and source-service capacity remain safe under overlap.

Pool and concurrency limits complement each other:

- the CDS pool protects the external service;
- `max_active_runs` limits overlapping logical runs;
- executor/worker limits protect local compute;
- a database pool may be added if concurrent loaders can exhaust PostgreSQL.

## 8. Artifact Identity and XCom

Airflow execution identity must never determine scientific artifact identity.
In particular, do not put `run_id`, `dag_run_id`, task attempt, or wall-clock
execution time into `request_key` or artifact paths.

These values are useful only as orchestration and logging metadata. Pipeline
artifact identity continues to be derived from the logical request contract in
`AGENTS.md`.

XCom may contain only small JSON-serializable control metadata, such as:

- the request specification or deterministic `request_key`;
- RAW or transformed artifact references;
- small row-count or status summaries.

Never place a DataFrame, xarray Dataset, NetCDF/Parquet bytes, complete API
response, credentials, or another large payload in XCom. When downstream code
can deterministically reconstruct a reference from the request, pass only the
request metadata.

## 9. DAG Parsing, Configuration, and Secrets

DAG import must remain lightweight and deterministic. Top-level DAG code must
not perform network calls, database queries, artifact reads, directory scans,
or pipeline execution.

Environment-dependent orchestration values belong in Airflow configuration,
Connections, environment variables, or an approved secrets backend. Airflow
Variables are suitable for small operational settings, not dataset storage or
a substitute application database.

Credentials must never appear in DAG source, task arguments, XCom, or logs.

## 10. Observability and Alerting

Airflow logs should add orchestration context without duplicating the detailed
scientific logs produced by the pipeline. At minimum, make these values easy to
correlate when available:

```text
dag_id
task_id
run_id
try_number
data_interval_start
data_interval_end
request_key
artifact reference
processed/skipped row count
duration
```

Important operational signals include task duration, retry count, last
successful eligible interval, freshness lag, processed/skipped counts, and
failure reason. Alerts should identify the failed boundary and logical period
and must not expose secrets.

## 11. Partial Failure and Recovery

Persistent deterministic checkpoints allow a failed downstream boundary to be
retried without needlessly recreating valid upstream artifacts. For example, a
failed `load` retry should consume the already validated transformed checkpoint.

Airflow clearing or rerunning a task does not authorize overwriting an
artifact. Resume, validation, overwrite refusal, and database idempotency remain
pipeline responsibilities.

## 12. Airflow Test Scope

Airflow tests should verify orchestration behavior only:

- DAGs import without errors or external I/O;
- task IDs and dependencies match the three runtime boundaries;
- timezone, schedule, `catchup`, and `max_active_runs` are explicit;
- interval-to-inclusive-date mapping covers normal and invalid boundaries;
- retry, timeout, and `cds_api` pool assignments are correct;
- XCom values remain small control metadata;
- task wrappers propagate failures;
- no Airflow dependency leaks into `pipeline/`.

Core pipeline tests remain Airflow-independent and follow `tests/AGENTS.md`.

## Implementation Gate

Before enabling a DAG, confirm all of the following:

- [ ] The Airflow graph maps one monthly TaskGroup with `attempt_raw_checkpoint → resolve_raw_checkpoint → ensure_transformed_checkpoint → load`, while pipeline runtime retains `ensure_raw_checkpoint → ensure_transformed_checkpoint → load`.
- [ ] Validation has not been split out of pipeline runtime boundaries.
- [ ] `Asia/Jakarta` and interval conversion are explicit and tested.
- [ ] The six-day availability lag is configurable and logged.
- [ ] The operational DAG uses `catchup=False`.
- [ ] Historical backfill is explicit and range-controlled.
- [ ] Extraction uses the `cds_api` pool.
- [ ] Retry and timeout settings are defined per task.
- [ ] `run_id` does not affect request or artifact identity.
- [ ] XCom contains only small control metadata.
- [ ] DAG parsing performs no external work.
- [ ] Secrets use approved Airflow facilities.
- [ ] Airflow-specific tests pass while core tests still run without Airflow.
