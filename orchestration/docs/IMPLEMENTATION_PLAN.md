# Apache Airflow Implementation Plan

This document tracks the staged introduction of Apache Airflow orchestration.
It follows the architecture and engineering contracts in
[`../../AGENTS.md`](../../AGENTS.md), the current pipeline behavior in
[`../../README.md`](../../README.md), and the Airflow-specific policies in:

- [`../../docs/AIRFLOW_BEST_PRACTICE.md`](../../docs/AIRFLOW_BEST_PRACTICE.md)
- [`../../docs/AIRFLOW_CODING_GUIDELINES.md`](../../docs/AIRFLOW_CODING_GUIDELINES.md)

Airflow must orchestrate the existing pipeline without replacing its scientific
transformation, validation, checkpoint, provenance, or persistence behavior.

## Target Structure

The initial orchestration layout is:

```text
orchestration/
├── dags/
├── docs/
│   └── IMPLEMENTATION_PLAN.md
├── logs/                 # local runtime state; ignored by Git
├── tasks/
├── airflow.cfg           # local Airflow configuration
├── airflow.db            # local metadata database; ignored by Git
└── README.md
```

The structure may grow when a demonstrated operational need requires it.

Responsibilities:

- `orchestration/dags/`: thin DAG declarations, scheduling, dependency wiring,
  retries, timeouts, pools, concurrency, and small XCom metadata.
- `orchestration/tasks/`: thin Airflow task wrappers that call Airflow-neutral
  runtime entrypoints.
- `orchestration/docs/`: implementation notes, operating procedures, and
  checklists.
- `pipeline/`: all extraction, scientific transformation, validation, storage,
  provenance, and database logic.
- `scripts/`: local command-line entrypoints that remain usable without Airflow.

## Initial Historical Backfill Design

The first DAG is a manually triggered ERA5-Land historical backfill. The user
must provide an explicit inclusive period, for example:

```text
2016-01-01 through 2025-12-31
```

or:

```text
2014-01-01 through 2024-12-31
```

Airflow must not infer a ten-year range from the wall clock. The selected range
is a runtime input and must pass the existing request validation.

The historical range is split by the pipeline into deterministic monthly
partitions. A ten-year range is therefore approximately 120 partitions in one
reviewed DagRun, rather than thousands of daily DagRuns.

```text
explicit inclusive range
          |
          v
validate and split by month
          |
          v
monthly request specifications
          |
          v
extract_or_resume (mapped)
          |
          v
transform_or_resume (mapped)
          |
          v
load (mapped)
```

Validation remains inside the existing runtime boundaries. There are no
separate Airflow validation tasks.

## Artifact and Worker Assumption

The current `LocalDataStorage` implementation requires every task worker to
see the same `data/` filesystem. The initial local deployment must therefore
use a single host or a genuinely shared data mount.

A distributed executor with isolated worker filesystems is out of scope until
the project adopts shared storage through an explicit design decision.

## Stage 0 — Branch and Baseline Guard

- [ ] Confirm that all reads and writes target branch `dev`.
- [ ] Record the current `dev` commit before implementation.
- [ ] Confirm that the working tree or remote branch contains no unexpected
      overlapping changes.
- [ ] Run the existing core unit-test baseline.
- [ ] Run the isolated PostgreSQL integration baseline when the configured test
      database is available.
- [ ] Do not modify scientific contracts, units, timestamps, request identity,
      artifact layout, or database identity as part of Airflow setup.

Exit condition: the pre-Airflow baseline is known and passing.

## Stage 1 — Airflow-Neutral Boundary Handoff

The existing runtime methods return xarray and Pandas-backed objects. Those
objects must not cross Airflow task boundaries or enter XCom.

- [ ] Add a JSON-safe request specification serializer and parser.
- [ ] Reuse `WeatherRequest`, `build_request()`, `validate_request()`, and
      `build_request_key()`; do not create a second identity algorithm.
- [ ] Add an Airflow-neutral extraction entrypoint for one monthly request.
- [ ] Add an Airflow-neutral transformation entrypoint that rehydrates and
      validates the deterministic RAW checkpoint.
- [ ] Add an Airflow-neutral load entrypoint that rehydrates and validates the
      deterministic transformed checkpoint.
- [ ] Delegate internally to `extract_or_resume()`,
      `transform_or_resume()`, and `load()`.
- [ ] Return only small status/control metadata from each boundary.
- [ ] Preserve the existing `run_monthly_batch()` CLI behavior.
- [ ] Add unit tests for fresh and resumed cross-process handoff.
- [ ] Confirm that core tests still run without importing Airflow.

Exit condition: each runtime boundary can execute in a separate process using
only a request specification or deterministic artifact reference.

## Stage 2 — Orchestration Configuration Helpers

- [ ] Centralize reusable non-secret request constants without coupling
      `pipeline/` to Airflow.
- [ ] Preserve the active dataset `reanalysis-era5-land`.
- [ ] Preserve provider variables `tp`, `t2m`, `swvl1`, `ssrd`, and
      `d2m`.
- [ ] Preserve the Aceh bounding box: north `6.1`, south `1.9`, west
      `94.9`, east `98.3`.
- [ ] Implement one pure `Asia/Jakarta` interval-to-inclusive-date helper.
- [ ] Reject empty, reversed, and non-local-midnight intervals.
- [ ] Keep historical start and end dates explicit runtime inputs.
- [ ] Use the existing monthly request splitter.
- [ ] Test one-day, multi-day, cross-timezone, invalid-boundary, and ten-year
      partition cases.

Exit condition: explicit historical inputs deterministically produce the
expected monthly request specifications.

## Stage 3 — Historical Backfill DAG

Create one manual DAG, initially named
`cds_era5_land_historical_backfill`.

- [ ] Place the DAG in `orchestration/dags/`.
- [ ] Place task wrappers in `orchestration/tasks/`.
- [ ] Use `schedule=None`.
- [ ] Set `catchup=False`.
- [ ] Set a fixed timezone-aware start date in `Asia/Jakarta`.
- [ ] Set `max_active_runs=1`.
- [ ] Accept explicit inclusive start and end dates.
- [ ] Validate the requested range before expansion.
- [ ] Split the range into monthly requests with pipeline-owned code.
- [ ] Use dynamic task mapping for monthly partitions.
- [ ] Keep the dependency graph
      `extract_or_resume -> transform_or_resume -> load`.
- [ ] Assign extraction to the `cds_api` pool.
- [ ] Start the `cds_api` pool with one slot.
- [ ] Configure extraction for at most three attempts with bounded backoff.
- [ ] Configure transformation with no automatic retry.
- [ ] Configure loading for at most two attempts with bounded backoff.
- [ ] Set finite execution timeouts for every boundary.
- [ ] Let original failures reach Airflow.
- [ ] Keep XCom payloads small and JSON-serializable.
- [ ] Ensure `run_id`, task attempts, and wall-clock time never affect
      `request_key` or artifact paths.

Exit condition: a single DagRun can manage a reviewed ten-year range while
exposing the status of each monthly partition.

## Stage 4 — Local Airflow Environment

Airflow installation must use a separately pinned requirement/constraint
strategy rather than being appended blindly to the pipeline dependency set.

- [ ] Confirm the deployment Python version.
- [ ] Select and pin a compatible Apache Airflow version.
- [ ] Install Airflow using its matching official constraints file.
- [ ] Configure `AIRFLOW_HOME` to the repository's `orchestration/`
      directory through a small activation script or documented environment
      setup.
- [ ] Set `dags_folder` to `orchestration/dags/`.
- [ ] Set `load_examples=False`.
- [ ] Configure the initial local executor/development mode.
- [ ] Create the `cds_api` pool with one slot.
- [ ] Resolve CDS and PostgreSQL credentials through Airflow Connections,
      environment-backed secrets, or the approved local secret boundary.
- [ ] Never commit credentials, logs, `airflow.db`, generated PID files, or
      serialized task data.
- [ ] Update `.gitignore` for Airflow runtime state.
- [ ] Verify the exact CLI commands against the pinned Airflow version.

Exit condition: the local Airflow environment parses the repository DAG
without examples or committed runtime state.

## Stage 5 — Orchestration README

Use the supplied README only as a document-structure reference. Rewrite its
project-specific content for this repository.

- [ ] Explain that commands run from the repository root.
- [ ] Document supported Python and Airflow versions.
- [ ] Document installation with the matching official constraint URL.
- [ ] Document `AIRFLOW_HOME` activation.
- [ ] Document metadata database initialization/migration.
- [ ] Document local startup and shutdown commands for the pinned version.
- [ ] Document the orchestration directory structure.
- [ ] Document the three-boundary task graph.
- [ ] Document explicit historical date parameters with examples.
- [ ] Document the `cds_api` pool and shared-filesystem assumption.
- [ ] Document credentials without exposing secret values.
- [ ] Document safe rerun/resume behavior.
- [ ] Avoid an unguarded destructive database-reset command.

Exit condition: a developer can install, start, trigger, inspect, stop, and
safely rerun the backfill from `orchestration/README.md`.

## Stage 6 — Airflow Tests

- [ ] Verify DagBag import has no errors.
- [ ] Verify DAG import performs no CDS, database, or artifact I/O.
- [ ] Verify the DAG ID and three runtime boundary task IDs.
- [ ] Verify dependency order.
- [ ] Verify timezone, fixed start date, schedule, `catchup`, and
      `max_active_runs`.
- [ ] Verify extraction pool, retries, retry delays, and timeouts.
- [ ] Verify monthly dynamic mapping inputs.
- [ ] Verify XCom payload shape and size.
- [ ] Verify wrappers delegate to pipeline runtime entrypoints.
- [ ] Verify wrapper exceptions propagate.
- [ ] Verify no Airflow import leaks into `pipeline/`.
- [ ] Run all existing core unit tests without Airflow.
- [ ] Run PostgreSQL integration tests when the isolated database is available.

Exit condition: orchestration tests and the unchanged core baseline pass.

## Stage 7 — Controlled Backfill Rollout

- [ ] Run one monthly partition.
- [ ] Rerun the same month and confirm RAW/Parquet resume and database
      idempotency.
- [ ] Run three consecutive monthly partitions.
- [ ] Check artifact count, provenance, row counts, disk usage, memory, task
      duration, and database load.
- [ ] Estimate storage and runtime for the selected ten-year period.
- [ ] Review the exact ten-year start and end dates.
- [ ] Trigger the complete historical DagRun.
- [ ] Monitor failed mapped partitions.
- [ ] Rerun only failed partitions after diagnosing the failure.
- [ ] Record the completed range and any operational limits discovered.

Exit condition: the selected explicit ten-year period is complete and
verifiably resumable.

## Stage 8 — Later Operational DAG

The daily operational DAG is deliberately deferred until the historical
backfill is stable.

- [ ] Resolve the exact relationship between the scheduled Airflow interval and
      the configurable six-completed-local-day availability lag.
- [ ] Keep `catchup=False` and `max_active_runs=1`.
- [ ] Reuse the same three runtime boundaries and small metadata handoff.
- [ ] Add freshness and last-success observability.
- [ ] Add bounded alerts that identify the failed logical period without
      exposing secrets.
- [ ] Test delayed availability, retry, resume, and idempotency.

Exit condition: recurring ingestion is enabled without changing the historical
or scientific data contracts.

## Explicit Non-Goals

- No separate Airflow validation tasks.
- No DataFrame, xarray Dataset, NetCDF bytes, or Parquet bytes in XCom.
- No scientific transformation or SQL persistence in DAG/task-wrapper files.
- No Airflow imports in `pipeline/`.
- No scheduler catchup as the ten-year bootstrap mechanism.
- No implicit rolling ten-year range.
- No distributed workers with isolated local artifact storage.
- No restoration of inactive ARCO paths.
