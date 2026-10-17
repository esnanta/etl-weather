# ERA5-Land Orchestration with Apache Airflow

Apache Airflow coordinates the existing Copernicus CDS ERA5-Land ETL pipeline. Scientific transformation, validation, artifact handling, and canonical PostgreSQL persistence remain owned by `pipeline/`; Airflow controls when work runs, how monthly partitions are scheduled, how failures are retried, and how campaign progress is tracked.

Run commands in this document from the repository root unless stated otherwise.

For Python, PostgreSQL, CDS credentials, dependencies, and the root `.env` configuration, first follow [`/README.md`](../README.md).

## Airflow Setup After Project Installation

After the project environment is installed and `.env` is configured, create the shared Airflow JWT secret:

```bash
./generate-airflow-jwt-secret.sh
```

The script creates or atomically replaces the ignored root file:

```text
airflow-jwt-key.txt
```

Airflow components use this file as one shared JWT secret. The secret is intentionally stored outside `.env`; `activate-airflow.sh` exposes it to Airflow through `AIRFLOW__API_AUTH__JWT_SECRET_CMD` without printing the secret.

Generate the file during initial Airflow setup. Normal activation should reuse the existing file. Running the generator again intentionally rotates the secret, so stop and restart every Airflow component after rotation.

Activate the project Airflow environment:

```bash
source activate-airflow.sh -q
```

The activation script loads the project `.env`, activates `myvenv`, validates the JWT file, and configures the repository-local Airflow runtime.

Initialize or upgrade Airflow metadata and create the one-slot CDS submission pool:

```bash
airflow db migrate
airflow pools set cds_api 1 "Copernicus CDS API requests"
```

The current local runtime uses repository-local Airflow metadata and the project DAG directory.

Start the local Airflow services with:

```bash
source activate-airflow.sh -q
airflow standalone
```

Standalone mode starts the scheduler, API server, DAG processor, and triggerer. Keep it running while operating the DAG.

## Running the Orchestration

The DAG ID is:

```text
cds_era5_land_etl
```

The basic operating flow is:

```text
activate Airflow
      ↓
start Airflow
      ↓
inspect cds_era5_land_etl
      ↓
run scheduled campaign or manual range
      ↓
inspect DagRun and mapped task states
      ↓
inspect campaign progress
```

The project supports two operating modes through the same DAG:

- **scheduled mode** selects campaign work automatically from durable PostgreSQL progress;
- **manual mode** processes an explicit inclusive date range supplied by the operator.

The DAG keeps `max_active_runs=1`, so only one DagRun is active at a time.

## Project Structure

Only Airflow-related paths are shown here:

```text
etl-weather/
├── config.py                        # single shared lightweight project config
├── activate-airflow.sh
├── generate-airflow-jwt-secret.sh
├── airflow-jwt-key.txt              # generated locally; ignored by Git
├── orchestration/
│   ├── airflow.cfg
│   ├── campaign/
│   ├── dags/
│   │   └── cds_era5_land_etl.py
│   ├── monitoring/
│   ├── operators/
│   ├── progress/
│   ├── repositories/
│   ├── scheduling/
│   └── tasks/
└── scripts/
    ├── destroy_all.py
    ├── inspect_campaign_capacity.py
    ├── report_campaign_progress.py
    ├── reset_all.py
    └── reset_airflow_runtime.py
```

The ETL implementation itself remains under `pipeline/` and is documented from the project root.

## Scheduled and Manual Mode Contract

Airflow derives the mode from the DagRun type. Operators do not pass a separate `mode` parameter.

### Scheduled example

With scheduling enabled, a scheduled DagRun selects the earliest eligible incomplete campaign quarter from PostgreSQL progress.

For example:

```text
campaign: 2016-2025

2016 Q1 incomplete
2016 Q2 incomplete

next scheduled selection → 2016 Q1
mapped months            → January, February, March
```

The selected months use the same extraction, transformation, and load boundaries as manual work.

### Manual example

A manual DagRun must supply `start_date` and `end_date`:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-02-01","end_date":"2024-02-29"}'
```

Manual execution is useful for explicit historical work and for repairing a failed campaign month. A whole-month repair that matches the campaign configuration can reconcile that month back into campaign progress.

## Orchestration Timing Policy

Timing values are source-controlled in root `config.py`:

```text
CAMPAIGN_SCHEDULE_INTERVAL_MINUTES = 10
QUARTER_COOLDOWN_MINUTES           = 10
MONTHLY_SLOT_SPACING_MINUTES       = 15
SUBMISSION_INTERVAL_MINUTES        = 5
```

Meaning:

- every 10 minutes Airflow checks whether scheduled campaign work is eligible;
- after one quarter becomes `complete + valid`, the next quarter waits 10 minutes;
- the three months inside one quarter use earliest-start offsets `+0/+15/+30`;
- actual CDS submission POST requests remain at least 5 minutes apart.

Example:

```text
21:00  Q1 selected
       Jan +0m, Feb +15m, Mar +30m

21:47  Q1 complete + valid
21:57  quarter cooldown ends

22:00  next scheduler check
       Q2 may be selected
```

`ERA5_CAMPAIGN_SCHEDULE_ENABLED` remains in `.env` only as the scheduled-mode
on/off switch. Waiting tasks defer without occupying a worker.

## Campaign Progress

Campaign progress is stored durably in PostgreSQL rather than inferred only from Airflow task history. This allows campaign state to survive Airflow process restarts and individual DagRuns.

The relevant operational tables are:

```text
climate_data.pipeline_progress
climate_data.pipeline_progress_detail
climate_data.submission_pacing
```

A quarter is complete only when all expected monthly partitions have valid persisted evidence.

Example:

```text
2016 Q1

January   complete
February  failed
March     complete

quarter status → incomplete
```

After a successful whole-month manual repair of February and reconciliation against PostgreSQL evidence:

```text
January   complete
February  complete
March     complete

quarter status → complete
```

Airflow task success alone is not enough to declare a campaign quarter complete. Reconciliation checks the persisted climate partition and its recorded artifact identity.

Inspect campaign progress with:

```bash
source activate-airflow.sh -q
python scripts/report_campaign_progress.py
```

## Monthly Start Slots and Retry Behavior

For a scheduled quarter, the three monthly TaskGroups receive earliest-start lower bounds separated by 15 minutes:

```text
month 1 → selection time
month 2 → selection time + 15 minutes
month 3 → selection time + 30 minutes
```

These are earliest-start limits, not promises that tasks will begin or finish at those times. Worker availability, CDS pacing, provider queueing, and retries may delay actual execution.

Manual runs do not receive these scheduled start offsets.

Each month gets an initial extraction attempt. A failure in one month does not prevent other monthly first attempts from progressing. Successful months may continue to transformation and load. Failed months wait until the initial extraction pass has reached its terminal states before the failed resolver retries according to the DAG retry policy.

A terminal failed monthly detail blocks automatic advancement to later campaign quarters until the affected month is repaired and reconciled.

## Task Flow

The active DAG uses one depth-first mapped TaskGroup per month:

```text
manual dates or selected scheduled quarter
                 ↓
       build_monthly_requests
                 ↓
       monthly_etl[YYYY-MM]
                 ↓
      attempt_raw_checkpoint
                 ↓
      resolve_raw_checkpoint
                 ↓
 ensure_transformed_checkpoint
                 ↓
                load
```

A month may transform and load as soon as its RAW checkpoint is ready, even
while sibling months are still deferred or waiting. Mapped task instances use
the month as the Airflow display label, for example `2016-01`. The display label is only for observability; it does not change request identity or artifact paths.

Airflow exchanges small metadata between tasks. NetCDF, Parquet, xarray, and Pandas payloads remain in shared storage rather than XCom.

## Triggering and Checking Status

Trigger one explicit manual range:

```bash
source activate-airflow.sh -q

airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-01-01","end_date":"2024-01-31"}'
```

`airflow dags trigger` creates a new DagRun and returns before that run necessarily finishes. Do not trigger the same request repeatedly just to see whether the earlier run completed.

Check existing runs with:

```bash
airflow dags list-runs cds_era5_land_etl
```

Use the returned `run_id` to identify the exact run in the CLI or Airflow UI.

For a failed campaign month, prefer the smallest complete calendar-month repair:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-02-01","end_date":"2024-02-29"}'
```

## Operational Examples

### Normal scheduled quarter

A scheduled run selects the earliest eligible incomplete quarter. Its three monthly TaskGroups use +0, +15, and +30 minute earliest-start lower bounds. Existing valid artifacts are resumed; missing artifacts are retrieved and processed depth-first per month.

Observed clean-pilot timings for one monthly partition were approximately:

```text
RAW checkpoint task   : 14-56 minutes
transform + validate  : 1.7-2.1 seconds
load + reconcile      : 1.8-2.1 seconds
```

The RAW task duration includes earliest-start deferral, five-minute CDS pacing,
provider queue/download time, validation, and checkpoint creation, so it should
not be interpreted as network transfer time alone.

### One month fails

If February fails while January and March succeed, the successful months can complete their downstream work. The failed detail remains durable and blocks automatic progression beyond that quarter.

### Manual repair

Trigger the complete failed month manually. The same checkpoint-backed pipeline validates or resumes existing artifacts, loads the partition, and reconciles campaign progress. The quarter becomes complete only after all three monthly partitions have valid evidence.

### Restart after interruption

If Airflow is stopped during a campaign, do not clear progress claims or delete artifacts immediately. First inspect the previous DagRun, campaign report, and shared artifacts. Then use an explicit whole-month manual repair for any affected month once the original run is confirmed inactive.

## Backup and Restart Recovery

Campaign truth is distributed across PostgreSQL and shared artifacts. Airflow's local SQLite metadata alone is not sufficient to reconstruct campaign state.

For a recoverable maintenance snapshot, preserve:

- the PostgreSQL application database, including climate observations, campaign progress, and submission pacing;
- the shared `data/` artifacts, including any reusable retrieval chunks;
- Airflow local metadata when run history is needed;
- the stable `airflow-jwt-key.txt` used by the running Airflow components.

Before taking a coordinated snapshot, pause new scheduling and wait for active or deferred work to finish. A paused DAG prevents new scheduled runs but does not stop tasks that are already running.

After restart, inspect campaign progress and active DagRuns before enabling scheduling again. Do not manually delete progress rows, reset pacing state, or clear an owner claim while the original DagRun may still be active.

## Observability

### Campaign report

Use:

```bash
python scripts/report_campaign_progress.py
```

This reports the configured campaign rather than counting unrelated manual ranges.

### Capacity snapshot

Use:

```bash
python scripts/inspect_campaign_capacity.py
```

The script reports local disk, memory, CPU, triggerer-process information, deferred-task count when available, and PostgreSQL relation sizes without printing credentials.

### Task memory

Relevant runtime boundaries log process peak RSS. Treat this as a task-process high-water mark, not total host memory and not memory allocated exclusively by the current boundary.

### Disk warnings

Workers check free space around artifact and temporary-file locations. A low-space warning is an operational warning, not a guarantee that the remaining space is sufficient. PostgreSQL storage on another host must be monitored separately.

## Safe Retry, Resume, and DAG Deployment

The orchestration layer relies on the pipeline's existing safety properties:

- valid RAW NetCDF artifacts are resumed and validated;
- valid transformed Parquet artifacts are resumed and validated;
- PostgreSQL persistence remains transactional and idempotent;
- clearing or retrying a task does not authorize manual artifact replacement;
- Airflow `run_id`, retries, and wall-clock time do not change request identity.

Do not deploy a changed DAG graph into an active DagRun. A run that spans two DAG versions can contain task instances and mapped metadata from different definitions.

Use this sequence for a DAG change during a campaign:

```text
disable/pause new scheduling
        ↓
inspect active and deferred work
        ↓
finish or diagnose the existing DagRun
        ↓
deploy the new DAG version
        ↓
start a clean post-change pilot
        ↓
verify PostgreSQL progress and climate evidence
        ↓
resume scheduled operation
```

Preserve Airflow metadata, PostgreSQL progress, and checkpoint files while doing this.

## Tests

Run orchestration regression tests with scheduling disabled:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

Run the PostgreSQL integration coverage for persistence, progress, pacing, and database bootstrap with:

```bash
python -m pytest \
  tests/integration/pipeline/test_postgresql_idempotency.py \
  tests/integration/orchestration/test_pipeline_progress_reconciliation.py \
  tests/integration/orchestration/test_submission_pacing.py \
  tests/integration/orchestration/test_orchestration_database_bootstrap.py \
  -q
```

The integration suite must use the isolated `TEST_POSTGRES_*` database configured in `env-example`. Airflow storage bootstrap initializes only the five climate/operational tables via root `database/`; geospatial table initialization is an independent explicit CLI operation (`python -m database.geospatial --execute`), never part of DAG execution. Airflow metadata remains SQLite.

Detailed test contracts remain in [`tests/README.md`](../tests/README.md).

## Maintenance, Reset, and Clean-Slate Destroy

The project intentionally separates conservative recovery from intentional destruction.

Use `reset_all.py` for guarded maintenance only after Airflow is fully inactive:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python scripts/reset_all.py

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python scripts/reset_all.py --execute
```

The command is inspect-only unless `--execute` is supplied. It refuses active or resumable Airflow state, including queued/running/deferred tasks, queued/running DagRuns, scheduler/API/triggerer/DAG-processor processes, and active workers. It does not kill those processes for the operator.

Use `destroy_all.py` only when the local development project must intentionally return to a clean state:

```bash
source activate-airflow.sh -q

python scripts/destroy_all.py
python scripts/destroy_all.py --execute
```

Destroy is available only with `APP_ENV=dev`. Inspect-only mode lists the exact PostgreSQL, data, Airflow log/runtime, and recognized process targets. Execution requires typing exactly:

```text
DESTROY
```

Unlike reset, destroy intentionally stops all recognized Airflow services/workers using SIGTERM followed by SIGKILL only for recognized processes that remain alive. If Airflow cannot be stopped, destructive database/data actions do not begin.

Before Airflow is stopped, destroy checks PostgreSQL connections and repeats the check just before DROP. It refuses to drop PostgreSQL while unrelated sessions remain connected to the target database. It reports the blocking PID, user, application, client address, and state rather than silently terminating arbitrary clients.

The destroy order is:

```text
check PostgreSQL sessions before stopping Airflow
        ↓
stop recognized Airflow runtime
        ↓
verify Airflow process count = 0
        ↓
recheck PostgreSQL session safety
        ↓
drop application PostgreSQL database (no recreation)
        ↓
clear project data/
        ↓
clear Airflow logs
        ↓
clear generated Airflow SQLite/auth/PID runtime state
        ↓
verify application database is absent and runtime/data are cleared
```

After a successful destroy, create missing development/test databases explicitly with `python -m database.setup --execute`; use `python -m database.bootstrap --execute` only for an already existing application database. Both maintenance paths preserve project configuration and source. In particular, destroy does not delete:

```text
.env
env-example
orchestration/airflow.cfg
airflow-jwt-key.txt
source code
Git metadata
Python virtual environment
```

Generated Airflow runtime state such as `orchestration/airflow.db`, its SQLite sidecar files, generated auth files, PID files, and `orchestration/logs/` are not canonical climate data. Do not remove them manually while Airflow is running.

The root `config.py` is the single lightweight project configuration module. There is no `orchestration/config.py`; orchestration imports shared constants and pure resolvers from the root module.
