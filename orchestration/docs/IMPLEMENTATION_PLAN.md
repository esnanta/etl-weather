# Apache Airflow Implementation Plan

This document tracks the staged introduction of Apache Airflow orchestration.
It follows the architecture and engineering contracts in
[`../../AGENTS.md`](../../AGENTS.md), the current pipeline behavior in
[`../../README.md`](../../README.md), and the Airflow-specific policies in:

- [`../../docs/AIRFLOW_BEST_PRACTICE.md`](../../docs/AIRFLOW_BEST_PRACTICE.md)
- [`../../docs/AIRFLOW_CODING_GUIDELINES.md`](../../docs/AIRFLOW_CODING_GUIDELINES.md)

Airflow must orchestrate the existing pipeline without replacing its scientific
transformation, validation, checkpoint, provenance, or persistence behavior.

## Current Implementation Status

Implementation progress through 2026-09-25:

- JSON-safe request specifications and checkpoint-backed runtime entrypoints;
- 105 core unit tests after the runtime-boundary change;
- manual date-range ERA5-Land ETL DAG and thin task wrappers;
- Airflow 3.3.2 dependency boundary and local activation/configuration;
- DagBag, task graph, retry, timeout, pool, interval, and ten-year mapping tests;
- 119 tests passed in the combined Airflow/pipeline environment;
- two PostgreSQL integration tests passed against a configured isolated test
  database on 2026-09-23;
- local metadata migration and `cds_api` pool command verified without running
  an external CDS request or historical ETL run;
- local standalone startup verified, followed by diagnosis of the first smoke
  run's pre-execution failure and configuration of a shared Airflow JWT secret;
- Airflow DAG constructor types made explicit for IDE/static inspection:
  mutable `tags` and `ParamsDict` parameters;
- one-month live ETL run completed successfully on 2026-09-24;
- same-month rerun completed successfully in approximately ten seconds with
  validated RAW and transformed checkpoints resumed;
- PostgreSQL confirmed the unchanged partition using matching request key,
  artifact path, checksum, and 16,430-row count, then skipped the database
  write with `rows_processed=0`;
- task-process peak RSS instrumentation added to all three mapped ETL boundaries;
  measurements are logged on success and failure without changing XCom;
- a three-month January-March 2024 DagRun completed successfully in about
  20 minutes 26 seconds, producing three RAW and three transformed artifacts;
- PostgreSQL contained the expected 48,230 observations across the three
  monthly partitions, with complete local-day ranges and recorded checksums;
- the three-month artifacts occupied approximately 16 MB RAW and 1.5 MB
  transformed storage. Peak task memory was not captured by this run;
- an April 2024 fresh run completed on 2026-09-25 in approximately 33 minutes
  36 seconds, despite seven recovered CDS download read timeouts;
- April task-process peak RSS was 517.59 MiB for extraction, 572.22 MiB for
  transformation, and 307.30 MiB for loading; the load processed 15,900 rows;
- subsequent April DagRuns succeeded; two extraction logs confirm RAW checkpoint
  resume, with one resumed extraction peak RSS of 457.25 MiB.

Still pending:

- final capacity review and selected full-range ETL run;
- the later recurring operational DAG.

## Target Structure

The initial orchestration layout is:

```text
etl-weather/
├── activate-airflow.sh
├── airflow-jwt-key.txt         # generated locally; ignored by Git
├── generate-airflow-jwt-secret.sh
└── orchestration/
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

- `activate-airflow.sh`: activates `myvenv` and anchors Airflow runtime state
  to the repository's `orchestration/` directory.
- `generate-airflow-jwt-secret.sh`: creates or atomically rotates the protected
  Airflow JWT secret file without placing the value in `.env`.
- `orchestration/dags/`: thin DAG declarations, scheduling, dependency wiring,
  retries, timeouts, pools, concurrency, and small XCom metadata.
- `orchestration/tasks/`: thin Airflow task wrappers that call Airflow-neutral
  runtime entrypoints.
- `orchestration/docs/`: implementation notes, operating procedures, and
  checklists.
- `pipeline/`: all extraction, scientific transformation, validation, storage,
  provenance, and database logic.
- `scripts/`: local command-line entrypoints that remain usable without Airflow.

## Initial Historical ETL Design

The first DAG is a manually triggered ERA5-Land ETL. The user
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

- [x] Confirm that all reads and writes target branch `dev`.
- [x] Record the current `dev` commit before implementation.
- [x] Confirm that the working tree or remote branch contains no unexpected
      overlapping changes.
- [x] Run the existing core unit-test baseline.
- [x] Run the isolated PostgreSQL integration baseline when the configured test
      database is available.
- [x] Do not modify scientific contracts, units, timestamps, request identity,
      artifact layout, or database identity as part of Airflow setup.

Exit condition: the pre-Airflow baseline is known and passing.

## Stage 1 — Airflow-Neutral Boundary Handoff

The existing runtime methods return xarray and Pandas-backed objects. Those
objects must not cross Airflow task boundaries or enter XCom.

- [x] Add a JSON-safe request specification serializer and parser.
- [x] Reuse `WeatherRequest`, `build_request()`, `validate_request()`, and
      `build_request_key()`; do not create a second identity algorithm.
- [x] Add an Airflow-neutral extraction entrypoint for one monthly request.
- [x] Add an Airflow-neutral transformation entrypoint that rehydrates and
      validates the deterministic RAW checkpoint.
- [x] Add an Airflow-neutral load entrypoint that rehydrates and validates the
      deterministic transformed checkpoint.
- [x] Delegate internally to `extract_or_resume()`,
      `transform_or_resume()`, and `load()`.
- [x] Return only small status/control metadata from each boundary.
- [x] Preserve the existing `run_monthly_batch()` CLI behavior.
- [x] Add unit tests for fresh and resumed cross-process handoff.
- [x] Confirm that core tests still run without importing Airflow.

Exit condition: each runtime boundary can execute in a separate process using
only a request specification or deterministic artifact reference.

## Stage 2 — Orchestration Configuration Helpers

- [x] Centralize reusable non-secret request constants without coupling
      `pipeline/` to Airflow.
- [x] Preserve the active dataset `reanalysis-era5-land`.
- [x] Preserve provider variables `tp`, `t2m`, `swvl1`, `ssrd`, and
      `d2m`.
- [x] Preserve the Aceh bounding box: north `6.1`, south `1.9`, west
      `94.9`, east `98.3`.
- [x] Implement one pure `Asia/Jakarta` interval-to-inclusive-date helper.
- [x] Reject empty, reversed, and non-local-midnight intervals.
- [x] Keep historical start and end dates explicit runtime inputs.
- [x] Use the existing monthly request splitter.
- [x] Test one-day, multi-day, cross-timezone, invalid-boundary, and ten-year
      partition cases.

Exit condition: explicit historical inputs deterministically produce the
expected monthly request specifications.

## Stage 3 — Historical ETL DAG

Create one manual DAG named `cds_era5_land_etl`.

- [x] Place the DAG in `orchestration/dags/`.
- [x] Place task wrappers in `orchestration/tasks/`.
- [x] Use `schedule=None`.
- [x] Set `catchup=False`.
- [x] Set a fixed timezone-aware start date in `Asia/Jakarta`.
- [x] Set `max_active_runs=1`.
- [x] Accept explicit inclusive start and end dates.
- [x] Validate the requested range before expansion.
- [x] Split the range into monthly requests with pipeline-owned code.
- [x] Use dynamic task mapping for monthly partitions.
- [x] Keep the dependency graph
      `extract_or_resume -> transform_or_resume -> load`.
- [x] Assign extraction to the `cds_api` pool.
- [x] Start the `cds_api` pool with one slot.
- [x] Configure extraction for at most three attempts with bounded backoff.
- [x] Configure transformation with no automatic retry.
- [x] Configure loading for at most two attempts with bounded backoff.
- [x] Set finite execution timeouts for every boundary.
- [x] Let original failures reach Airflow.
- [x] Keep XCom payloads small and JSON-serializable.
- [x] Ensure `run_id`, task attempts, and wall-clock time never affect
      `request_key` or artifact paths.

Exit condition: a single DagRun can manage a reviewed ten-year range while
exposing the status of each monthly partition.

## Stage 4 — Local Airflow Environment

Airflow and pipeline dependencies use the single `requirements.txt` manifest.
The pinned Airflow version must still be installed first with its matching
official constraints, because those constraints are specific to the Airflow
installation step and do not govern every project package.

- [x] Confirm the deployment Python version.
- [x] Select and pin a compatible Apache Airflow version.
- [x] Keep the Airflow and pipeline dependency pins in one `requirements.txt`.
- [x] Install Airflow using its matching official constraints file.
- [x] Configure `AIRFLOW_HOME` to the repository's `orchestration/`
      directory through the root `activate-airflow.sh` script.
- [x] Set `dags_folder` to `orchestration/dags/`.
- [x] Set `load_examples=False`.
- [x] Configure the initial local executor/development mode.
- [x] Create the `cds_api` pool with one slot.
- [x] Resolve CDS and PostgreSQL credentials through Airflow Connections,
      environment-backed secrets, or the approved local secret boundary.
- [x] Require one file-backed `api_auth/jwt_secret` shared by the scheduler,
      task runner, and API server through the supported `_CMD` configuration.
- [x] Provide a separate script that securely creates or rotates the shared
      JWT secret file without printing its value.
- [x] Never commit credentials, logs, `airflow.db`, generated PID files, or
      serialized task data.
- [x] Update `.gitignore` for Airflow runtime state.
- [x] Verify the exact CLI commands against the pinned Airflow version.

Exit condition: the local Airflow environment parses the repository DAG
without examples or committed runtime state.

## Stage 5 — Orchestration README

Use the supplied README only as a document-structure reference. Rewrite its
project-specific content for this repository.

- [x] Explain that commands run from the repository root.
- [x] Document supported Python and Airflow versions.
- [x] Document installation with the matching official constraint URL.
- [x] Document `AIRFLOW_HOME` activation.
- [x] Document metadata database initialization/migration.
- [x] Document local startup and shutdown commands for the pinned version.
- [x] Document the orchestration directory structure.
- [x] Document the three-boundary task graph.
- [x] Document explicit historical date parameters with examples.
- [x] Document the `cds_api` pool and shared-filesystem assumption.
- [x] Document credentials without exposing secret values.
- [x] Document safe rerun/resume behavior.
- [x] Avoid an unguarded destructive database-reset command.

Exit condition: a developer can install, start, trigger, inspect, stop, and
safely rerun the historical ETL from `orchestration/README.md`.

## Stage 6 — Airflow Tests

- [x] Verify DagBag import has no errors.
- [x] Verify DAG import performs no CDS, database, or artifact I/O.
- [x] Verify the DAG ID and three runtime boundary task IDs.
- [x] Verify dependency order.
- [x] Verify timezone, fixed start date, schedule, `catchup`, and
      `max_active_runs`.
- [x] Verify extraction pool, retries, retry delays, and timeouts.
- [x] Verify monthly dynamic mapping inputs.
- [x] Verify XCom payload shape and size.
- [x] Verify wrappers delegate to pipeline runtime entrypoints.
- [x] Verify wrapper exceptions propagate.
- [x] Verify no Airflow import leaks into `pipeline/`.
- [x] Run all existing core unit tests without Airflow.
- [x] Run PostgreSQL integration tests when the isolated database is available.

Exit condition: orchestration tests and the unchanged core baseline pass.

## Capacity Snapshot — 2026-09-25

On the local host, `free -h` reported 15 GiB total RAM, 6.6 GiB available,
and no swap in use. The measured peak RSS for one fresh monthly task process
was 572.22 MiB during transformation. With `max_active_tasks=2`, concurrent
task memory, Airflow services, PostgreSQL, and the operating system must be
considered together; these per-process peaks do not measure host-wide peak RAM.

`df -h data` reported the `/home` filesystem at 99% usage with only 3.6 GiB
available. Existing `data/` occupies 23 MiB for January–April 2024, while
`orchestration/airflow.db` is about 1 MiB. A simple 120/4 scaling of those
four months suggests roughly 690 MiB of NetCDF/Parquet artifacts for 120
months, **excluding** PostgreSQL tables/indexes/WAL, CDS temporary downloads,
Airflow logs/metadata, and normal filesystem headroom. Network read timeouts
during April make elapsed-time extrapolation unreliable.

These measurements describe the local development host; the full pipeline
is intended to run on a cloud server. The cloud host's artifact and temporary
filesystems and the PostgreSQL storage volume must be measured independently.
Do not run the full range on the current local /home filesystem at 99% usage.
Mapped tasks now log a non-blocking warning below 5 GiB available on their
local artifact or temporary filesystem; this does not measure remote
PostgreSQL storage. Keep the final date range explicit. The local Airflow
metadata database uses SQLite for development; review migration to an
external database before increasing concurrency for a full-range run.

## Stage 7 — Controlled Historical ETL Rollout

- [x] Run one monthly partition.
- [x] Rerun the same month and confirm RAW/Parquet resume and database
      idempotency.
- [x] Run three consecutive monthly partitions.
- [x] Check artifact count, provenance, row counts, disk usage, task duration,
      and database load.
- [x] Instrument extraction, transformation, and loading to log process peak RSS.
- [x] Capture peak memory from fresh and resumed live task runs.
- [x] Warn in mapped-task logs when local artifact or temporary disk space
      falls below 5 GiB or cannot be measured.
- [ ] Estimate storage and runtime for the selected ten-year period.
- [ ] Review the exact ten-year start and end dates.
- [ ] Trigger the complete historical DagRun.
- [ ] Monitor failed mapped partitions.
- [ ] Rerun only failed partitions after diagnosing the failure.
- [ ] Record the completed range and any operational limits discovered.

Exit condition: the selected explicit ten-year period is complete and
verifiably resumable.

## Stage 8 — Later Operational DAG

The daily operational DAG is deliberately deferred until the historical ETL
is stable.

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
