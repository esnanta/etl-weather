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

Implementation progress through 2026-09-26:

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
- idempotent initialization for all 40 quarter batches and database-backed parent reconciliation are implemented as Airflow-neutral helpers; their 45 orchestration tests and isolated PostgreSQL reconciliation test passed locally on 2026-09-26;
- the Airflow-neutral selector now seeds exact campaign identities, reconciles
  in calendar order, and chooses the earliest eligible incomplete quarter;
- the existing DAG now maps monthly ETL boundaries through progress-aware wrappers;
- validated whole-month manual loads now update exact campaign detail identities;
- one persisted failed detail blocks subsequent automatic CDS submissions, and
  an atomic owner claim prevents competing selectors from choosing the same
  quarter; release follows verified completion or manual recovery;
- CDS pacing uses a PostgreSQL submission clock, a 30-minute default interval,
  a deferrable extraction operator, and resumable retrieval chunks. CI passed
  186 tests with five PostgreSQL integration tests skipped; the isolated local
  pacing integration test passed separately;
- the manual May 2024 live pilot completed successfully on 2026-09-26. The
  context request was submitted at 17:30:06 Asia/Jakarta, the May request was
  deferred at 17:31:19, and the triggerer woke the task at 18:00:06. The
  context chunk resumed from its checkpoint, and May was submitted at 18:00:13
  (about 30 minutes 7 seconds after the first submission). RAW validation
  passed with 768 hourly steps, 1,505 grid points, and all five variables;
- the manual April 2024 recovery on 2026-09-27 advanced 2024 Q2 to 3/3
  verified months and the campaign report to 1/40 complete quarters. The
  latest recorded valid load was April at 10:36:10 Asia/Jakarta. The
  DagRun reached `success` at 10:36:11, about 12 seconds after it started;
- the read-only campaign report on 2026-09-26 showed 0/40 complete
  quarters and a last valid load at 20:20:09 Asia/Jakarta. A valid monthly
  load does not complete its three-month quarter; the report now makes
  partial month counts and last-loaded month explicit;
- a post-change June 2024 manual extraction pilot submitted the May 31 context
  POST at 19:35:24.566821 Asia/Jakarta and the June POST at 20:05:35.214587,
  1,810.648 seconds later. The June submission was initially deferred, the
  triggerer woke the task, and the cached context chunk was reused. A broken
  object-store download retried safely; there was no observed job POST retry.
  June RAW passed temporal/spatial validation (744 hours, 1,505 grid points,
  five variables). The extract log alone does not establish DagRun/load success;
- the 19:31 Asia/Jakarta local-host snapshot reported 5.97 GiB free for
  artifacts/logs/metadata, 53.59 GiB temporary space, 5.70 GiB available RAM,
  a 23.05 MiB PostgreSQL database, and two visible triggerer-related
  processes. This is a local sample, not a forecast of a future deployment;

Still pending:

- the existing DAG uses the mode contract, exact progress identity, mapped
  boundary status, persisted load proof, and live-verified CDS pacing; its
  schedule remains disabled;
- PostgreSQL progress/pacing backup and restart recovery are documented;
  keep the measured local disk headroom visible during the campaign. Future
  deployment capacity checks belong to that deployment, not the Stage 8 code gate;
- the configurable Asia/Jakarta scheduler cadence is implemented but disabled
  by default; a read-only exact-identity 2016–2025 summary is available.
  Complete remaining campaign observability and pilot scheduled selection
  before enabling the bounded campaign;
- decide future operational ingestion after the historical campaign. No additional DAG is part of the current plan.

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

## Initial Historical ETL Design (Implemented Baseline)

This section records the implemented manual baseline before the quarterly
campaign agreed on 2026-09-25. Stage 8 supersedes the proposed full-range
rollout without changing completed stages 0–6. The first DAG is a manually
triggered ERA5-Land ETL. The user
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

Exit condition (completed baseline): a single manual DagRun can accept a reviewed
explicit range and expose the status of its monthly partitions. Stage 8 adds
automatic scheduling in bounded quarters; a full ten-year DagRun is no longer
the planned rollout.

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

These measurements describe the local development host. A future cloud
installation must be evaluated at deployment, not as a prerequisite to
implementing the schedule. Monitor local disk headroom during any local run.
Mapped tasks now log a non-blocking warning below 5 GiB available on their
local artifact or temporary filesystem; this does not measure remote
PostgreSQL storage. Keep the final date range explicit. The local Airflow
metadata database uses SQLite for development; review migration to an
external database before increasing concurrency for a full-range run.

## Stage 7 — Controlled Pilot (Completed)

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
- [x] Record the successful January–March 2024 three-month pilot and the
      subsequent April fresh and resumed runs.

Exit condition (met): monthly resume and a three-month run have been verified.
The former proposal to trigger one complete ten-year DagRun has been replaced
by Stage 8.

## Stage 8 — Scheduled Quarterly Historical Campaign (In Progress)

**Agreed scope (2026-09-25):** extend the existing
`cds_era5_land_etl` DAG with automatic and explicit manual modes.
The campaign covers 2016-01-01 through 2025-12-31: 40 calendar
quarters and 120 monthly partitions. Airflow lists **one DAG**.
Each automatic DagRun selects at most one quarter and maps its three
monthly `extract_or_resume -> transform_or_resume -> load` chains.
The 40 quarters are campaign progress, not 40 DAG definitions;
manual recovery also uses the same DAG. A manual annual range is an
optional operator input, not a separate DAG per year.

Scheduling and selection:

- [x] Define an unambiguous mode contract in `orchestration/mode.py` before
      DAG integration. A scheduled run will select the next eligible incomplete
      quarter; a manual run requires explicit inclusive dates. Reject incomplete
      date pairs and ambiguous mode inputs.

The mode contract uses Airflow's DagRun `run_type` as its sole mode authority.
No `mode` parameter is accepted. `scheduled` runs require empty `dag_run.conf`
and return no operator dates; a later selection step will use persisted progress
to choose the earliest eligible incomplete quarter. `manual` runs require
`dag_run.conf` to contain exactly `start_date` and `end_date` as canonical
`YYYY-MM-DD` strings, inclusive and in ascending order. The existing request
builder still validates the historical range and six-day availability lag.
Unsupported run types, extra keys, incomplete pairs, invalid dates, and
scheduled configuration fail explicitly. A single failed month or quarter is
the recommended manual repair scope. Manual dates are not silently replaced
by scheduler-selected dates. The contract is implemented and used by the DAG. Its schedule remains
`None` by default pending the scheduled rollout.
- [x] Keep the campaign boundaries fixed at 2016-01-01 and 2025-12-31,
      select quarters in calendar order from 2016 Q1 through 2025 Q4,
      and map exactly three whole calendar months per automatic run.
- [x] Configure Jakarta-local campaign ticks at 30 minutes, one hour, or
      two hours, independently of CDS pacing. Default to disabled and retain
      `catchup=False` and `max_active_runs=1`; test the enabled DAG policy.
- [x] Pilot manual repair of the incomplete April 2024 campaign month.
      The 2024 Q2 parent advanced from 2/3 to complete after April's
      recorded valid load; the report changed from 0/40 to 1/40 quarters.
      The April manual DagRun finished in `success` on 2026-09-27.
- [ ] Pilot scheduled selection before enabling the schedule in deployment.
      Skipped ticks must not skip quarters; a run longer than two hours must
      not overlap the next run.
- [x] Keep manual triggers available through the same DAG; document that
      manual and automatic runs share `max_active_runs=1` and can queue
      behind one another.
- [ ] After all 40 quarters are verified complete, make scheduled ticks
      safe no-ops and document pausing the DAG. Do not infer new periods
      beyond 2025 from the wall clock.

Durable progress and recovery:

- [x] Refactor durable operational progress into the existing PostgreSQL climate
      database using two cadence-neutral tables:
      `climate.pipeline_progress` and
      `climate.pipeline_progress_detail`. Do not create a separate database.
      Quarter/two-month/month grouping remains scheduler policy rather than
      persistence identity. SQLite metadata remains local development state.
- [x] `climate.pipeline_progress` represents one logical requested range/batch.
      Store explicit `start_date` and `end_date`, pipeline/source/dataset identity,
      configuration identity, expected/completed/failed partition counts,
      execution status, validation status, timestamps, and the latest bounded
      diagnostic error. Do not encode quarter number as the durable identity.
- [x] `climate.pipeline_progress_detail` represents one processed partition and
      its artifact/database outcome. Store the parent progress ID, request key,
      exact period, RAW NetCDF path/checksum/file size, transformed artifact
      path/checksum/file size when available, temporal and spatial validation
      metrics, row counts, grid counts, extract/transform/load status,
      validation status/message, processing timestamps, and bounded diagnostic
      metadata.
- [x] Keep stable query/recovery fields as ordinary columns. Allow an optional
      `metrics JSONB` field only for evolving operational measurements such as
      task peak RSS, retry counts, durations, or disk-space observations; do
      not put request identity, status, or validation truth solely in JSONB.
- [x] Define parent completion from verified detail state: a progress row may
      become complete only when every expected detail partition is complete
      and valid. Partial manual repair must update only the affected detail and
      may complete the parent only after all expected partitions verify.
- [x] Document storage setup, backup, restart, and reconciliation behavior
      before enabling automatic mode. The runbook covers coordinated PostgreSQL
      and artifact snapshots, pacing state, stale claims, and manual repair.
- [x] Key progress to the explicit requested range plus a deterministic configuration identity derived from source, dataset, variables, bounding box, and expected partition request identities so changed configuration cannot reuse unrelated completion state.
- [x] For the current quarterly campaign, mark the parent progress row complete only after all three monthly detail rows have successful loads and verified persisted partitions. Persist completion atomically; task success alone must not advance the campaign.
- [x] Reconcile valid manual repairs with the same monthly completion
      evidence. A manual range may complete an affected quarter once all
      its months are verified, but partial manual success must never mark
      an incomplete quarter complete.
- [x] Select the earliest incomplete quarter on every scheduled run so
      restarts, missed ticks, and retries cannot silently skip a period.
      Guard concurrent progress updates even when `max_active_runs=1`.
- [ ] Bound repeated automatic failures for the same quarter; log/alert
      with year, quarter, month, and reason, then require diagnosis and
      manual repair instead of repeatedly submitting bad requests.
      Resume from validated checkpoints without overwriting RAW files.

Scheduler cadence and CDS submission interval are separate controls:

```text
scheduler cadence
        =
seberapa sering Airflow mencoba menjalankan pipeline

CDS submission interval
        =
jarak minimum antar request aktual ke CDS
```

Scheduler cadence is runtime orchestration policy and must be configurable
without changing progress-table identity or scientific contracts. The initial
campaign may use 30 minutes, one hour, two hours, or another reviewed value.
It controls how often Airflow attempts to start/select work. With
`max_active_runs=1`, a new run must not overlap an active run.

CDS submission interval is an independent provider-pacing control. It governs
minimum time between actual CDS retrieval submissions, including retries or
multiple monthly partitions within one logical progress range. The Airflow
schedule and the one-slot `cds_api` pool do not by themselves guarantee this
minimum elapsed time between provider submissions.

Neither value is part of `climate.pipeline_progress` or
`climate.pipeline_progress_detail` durable identity. They may be recorded as
runtime/observability metadata when useful, but changing cadence or pacing must
not invalidate otherwise identical request/progress identity.

CDS request pacing:

- [x] Keep the one-slot `cds_api` pool. Document that it limits
      concurrent extraction but does not enforce time between requests.
- [x] Add a configurable minimum interval before each CDS submission POST,
      shared by automatic and manual modes, including internal library retries,
      additional chunks, and Airflow task retries. The 30-minute default is
      an operational starting point, not a claimed Copernicus quota.
- [x] Control retriable CDS job POSTs inside pinned `cdsapi==0.7.7`.
      Its `robust` wrapper can retry submissions after timeouts or retriable
      HTTP responses. A dedicated requests session reserves a durable slot
      before every POST, including internal retries. GET polling and downloads
      do not reserve slots. Offline tests include the real pinned client's
      retry after a simulated submission timeout, with the second POST denied
      before transport.
- [x] Persist/coordinate the last submission timestamp across runs and
      workers; avoid holding a worker or pool slot idle for a long wait
      when Airflow can defer or reschedule it. Preserve a one-slot limit
      at submission, and make recovery after process termination safe.
- [ ] Review CDS queue time, provider errors, retries, and successful
      submissions; make both the inter-request interval and scheduler cadence adjustable independently without editing scientific contracts.
      The June pilot observed an object-store GET retry, not a job POST retry;
      a deterministic test uses the pinned cdsapi retry path to verify POST pacing.

Observability, validation, and rollout:

- [x] Provide a read-only exact-identity campaign report with yearly completion
      counts, partial monthly boundary states, failed quarters, and the month
      of the last recorded valid load. Keep its output out of XCom and exclude
      unrelated manual ranges/configurations.
- [ ] Log the selected year/quarter, three months, mode, attempt, CDS
      pacing decision, completed/remaining quarters, and failure reason.
      Use the read-only 2016–2025 summary without creating annual DAGs or
      sending large payloads through XCom.
- [ ] Update Airflow policy docs and README examples that currently say
      `schedule=None` or recommend triggering all 120 months in one
      DagRun; document manual quarter/month/year examples and status checks.
- [ ] Add focused offline tests for mode selection, 2016–2025 quarter
      boundaries, stable progress and configuration identity, missed ticks,
      incomplete/failing loads, manual repair, terminal no-op, spacing of
      successive CDS submissions, and no overlapping active runs.
- [ ] Run `python -m pytest tests/airflow_orchestration -q` and affected
      core/integration tests. Pilot scheduled and manual recovery runs
      before enabling the entire campaign.
- [x] Provide a read-only host capacity snapshot and a repeatable manual
      post-change CDS pilot procedure without exposing secrets.
- [x] Review the local June 2024 extraction pilot for actual POST spacing,
      deferral, cached context reuse, download retry, and RAW validation.
      No provider job POST retry occurred; DagRun/load completion was not supplied.
- [x] Record the local development-host disk, memory, PostgreSQL database
      size, and visible triggerer process snapshot. Keep local disk headroom
      under observation; revisit capacity when deploying to a different host.
      Do not extrapolate April wall time as a guaranteed CDS throughput.
- [ ] Track all 40 quarter statuses, failed months, last successful load,
      request pacing, and elapsed time during the campaign. Confirm
      2016–2025 completeness against expected monthly partitions and
      database records before declaring the campaign finished.

Exit condition: one DAG can schedule and recover the bounded 40-quarter
campaign; every completed quarter has three verified monthly loads,
its progress survives restarts, and additional ticks cannot submit
data after 2025.

## Stage 9 — Future Ongoing Ingestion (Deferred)

The quarterly 2016–2025 campaign has a fixed end date. Ingesting
newer periods after 2025 requires its own decision about data
availability, timing, and operations. This stage does not authorize
a second DAG or expand the historical campaign.

- [ ] Resolve the relationship between a future ingestion interval and
      the six-completed-local-day availability lag.
- [ ] Decide whether to extend the existing DAG's mode contract or use
      a different design after reviewing campaign results.
- [ ] Define freshness, last-success observability, bounded alerts,
      and retry behavior for periods outside the fixed campaign.
- [ ] Test delayed availability, retry, resume, and idempotency.

Exit condition: an explicitly approved ongoing-ingestion design
respects the scientific and historical campaign contracts.

## Explicit Non-Goals

- No separate Airflow validation tasks.
- No DataFrame, xarray Dataset, NetCDF bytes, or Parquet bytes in XCom.
- No scientific transformation or SQL persistence in DAG/task-wrapper files.
- No Airflow imports in `pipeline/`.
- No scheduler catchup as the ten-year bootstrap mechanism.
- No unpaced full-range 120-month DagRun for the 2016–2025 campaign.
- No 40 quarterly DAG definitions or ten yearly manual DAG definitions.
- No implicit rolling ten-year range.
- No distributed workers with isolated local artifact storage.
- No restoration of inactive ARCO paths.
