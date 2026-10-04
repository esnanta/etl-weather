# ERA5-Land Orchestration with Apache Airflow

Apache Airflow orchestrates the existing Copernicus CDS ERA5-Land ETL
pipeline. Scientific transformation, validation, deterministic artifacts,
provenance, and PostgreSQL persistence remain in `pipeline/`; Airflow only
coordinates execution and monitoring.

> **Note:** Run every command from the repository root.

## Prerequisites

- Python 3.12
- PostgreSQL for canonical daily climate observations
- CDS API credentials
- A single host, or a shared `data/` filesystem visible to every task worker

The local configuration uses Apache Airflow 3.3.2, SQLite for Airflow metadata,
and `LocalExecutor`. Before increasing concurrency for a complete historical
ETL run, move the Airflow metadata database to a supported external database
and validate the deployment again.

## Setup and Installation

Before starting Airflow, set the application environment in the project `.env`:

```env
APP_ENV=dev
```

`APP_ENV` is mandatory and accepts only `dev` or `prod`. The activation
script and DAG parser log the selected mode. Missing, blank, or invalid values
abort startup and instruct the operator to set `APP_ENV=dev` or
`APP_ENV=prod`; Airflow does not assume a mode.


1. **Create and activate the project environment:**

   ```bash
   python3 -m venv myvenv
   source myvenv/bin/activate
   ```

   Reuse the existing `myvenv` instead of recreating it when it has already
   been configured.

2. **Install project and Apache Airflow dependencies:**

   The project uses one dependency manifest: `requirements.txt`. Airflow's
   official constraints apply only to the initial Airflow installation. The
   complete manifest is then installed without those constraints, while its
   explicit `apache-airflow==3.3.2` pin prevents an accidental Airflow upgrade
   or downgrade.

   ```bash
   AIRFLOW_VERSION=3.3.2
   PYTHON_VERSION=3.12
   CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

   pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
   pip install -r requirements.txt
   pip check
   airflow version
   ```

3. **Load credentials and activate the Airflow configuration:**

   Store local values in the untracked `.env` file. Never put credentials in
   DAG parameters, source files, XCom, or logs. Generate and save the shared
   Airflow JWT secret with the repository script:

   ```bash
   ./generate-airflow-jwt-secret.sh
   ```

   The script generates `openssl rand -hex 32`, then creates or atomically
   replaces the ignored root file `airflow-jwt-key.txt` without printing the
   secret. The file permission is set to `600`. Running the script again
   intentionally rotates the secret, so stop and restart every Airflow
   component after doing so. Do not run it during normal activation; the
   scheduler, task runner, and API server must use one stable value.

   ```bash
   source activate-airflow.sh
   ```

   The project expects `CDSAPI_URL`, `CDSAPI_KEY`, the pipeline
   `POSTGRES_*` variables, and the Airflow campaign settings in `.env`.
   For scheduled operation, configure `ERA5_CAMPAIGN_SCHEDULE_ENABLED`,
   `ERA5_CAMPAIGN_SCHEDULE_CADENCE`, and
   `CDS_MIN_SUBMISSION_INTERVAL_SECONDS` there rather than passing them on
   each terminal command.
   When the root `.env` file exists, `activate-airflow.sh` automatically loads
   and exports its values, preserving the shell's previous `allexport` state.
   The script then activates `myvenv`, validates the presence and permission
   of `airflow-jwt-key.txt`, and sets
   `AIRFLOW__API_AUTH__JWT_SECRET_CMD` so Airflow reads the shared secret from
   that file. It also configures `AIRFLOW_HOME`, `AIRFLOW_CONFIG`, the absolute
   SQLite metadata URL, the DAG folder, and `PYTHONPATH` for this repository.
   Therefore, the separate `set -a`, `source .env`, and `set +a` commands are
   no longer required.

4. **Initialize Airflow:**

   ```bash
   airflow db migrate
   airflow pools set cds_api 1 "Copernicus CDS API requests"
   ```

   The one-slot `cds_api` pool prevents mapped historical tasks from submitting
   concurrent CDS downloads.

5. **Verify the local configuration:**

   `orchestration/airflow.cfg` uses the repository DAG directory and disables
   example DAGs:

   ```ini
   dags_folder = orchestration/dags
   load_examples = False
   ```

## Running the Orchestration

### Campaign schedule configuration

The scheduled campaign is configured before Airflow starts. The recommended
local configuration lives in the untracked root `.env`:

```env
CAMPAIGN_KEY=era5_land_2016_2025
CAMPAIGN_START_YEAR=2016
CAMPAIGN_END_YEAR=2017

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false
ERA5_CAMPAIGN_SCHEDULE_CADENCE=2h
CDS_MIN_SUBMISSION_INTERVAL_SECONDS=1800
```

Keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` while validating configuration.
When scheduled rollout is deliberately enabled, change only that value to
`true` and restart standalone so the DAG processor reparses the DAG.

The campaign range itself is not supplied through a scheduled DagRun.
`orchestration/config.py` requires `CAMPAIGN_KEY`,
`CAMPAIGN_START_YEAR`, and `CAMPAIGN_END_YEAR` from the exported
environment loaded from `.env`. The current controlled rollout example is
2016 through 2017. Each scheduled DagRun selects the earliest eligible
incomplete configured quarter from PostgreSQL durable progress and maps that
quarter's three calendar months. `CAMPAIGN_KEY` is included in the campaign
configuration identity used by selection, manual campaign reconciliation, and
reporting.

`ERA5_CAMPAIGN_SCHEDULE_CADENCE=2h` means an Airflow scheduler tick every two
Jakarta-local hours. It does not mean three months are downloaded every two
hours: `max_active_runs=1`, durable quarter claims, the one-slot `cds_api`
pool, monthly earliest-start offsets, and the separate CDS pacing clock still
limit actual work.

For local development, use Airflow standalone mode as the canonical runtime.
It starts the scheduler, API server, DAG processor, and triggerer together so
the project does not require separate service terminals.

```bash
source activate-airflow.sh -q
airflow standalone
```

Open `http://localhost:8080`. Use the credentials displayed by standalone or
written by the configured Airflow authentication manager. Keep the standalone
process running while using the UI or triggering work, and stop it with
`Ctrl+C` when finished.

After a fresh Airflow metadata reset, `airflow dags list` may initially report
`No data found` until standalone starts its DAG processor and the project DAG
has been parsed into the new metadata database. This is expected immediately
after recreation when there are no import errors.

## Project Structure

```text
etl-weather/
├── config.py                       # shared lightweight application settings
├── request_factory.py              # typed CDS request construction
├── activate-airflow.sh
├── airflow-jwt-key.txt             # generated; ignored by Git; mode 600
├── generate-airflow-jwt-secret.sh
└── orchestration/
    ├── config.py                   # campaign, cadence, pacing, storage policy
    ├── database.py                 # engine lifecycle and storage bootstrap
    ├── requests.py                 # validated monthly request specifications
    ├── intervals.py                # Airflow interval -> local inclusive dates
    ├── monitoring.py               # disk and process-memory observations
    ├── progress_tracking.py        # shared progress/artifact recording
    ├── campaign_repair.py          # validated manual repair reconciliation
    ├── repositories/               # operational SQL and persisted evidence
    ├── dags/
    │   └── cds_era5_land_etl.py
    ├── docs/
    │   ├── IMPLEMENTATION_PLAN.md
    │   ├── REFACTOR_PLAN.md
    │   └── POSTGRES_RECREATION.md
    ├── logs/                       # generated; ignored by Git
    ├── tasks/
    │   ├── build_monthly_requests.py
    │   ├── cds_extract_operator.py
    │   ├── extract.py
    │   ├── transform.py
    │   ├── load.py
    │   ├── era5_land.py            # compatibility re-export facade
    │   └── progress.py             # compatibility re-export facade
    ├── airflow.cfg
    ├── airflow.db                  # generated; ignored by Git
    └── README.md
```

## ERA5-Land ETL DAG

DAG ID:

```text
cds_era5_land_etl
```

The currently implemented baseline is manually triggered with explicit
inclusive `start_date` and `end_date` parameters. Stage 8 extends this same
DAG with a bounded scheduled quarterly campaign whose first and last years are
configured by `CAMPAIGN_START_YEAR` and `CAMPAIGN_END_YEAR`, while
preserving explicit manual recovery through the same DAG.

The total number of campaign quarters and monthly partitions is derived from
those configured bounds. Each automatic DagRun selects at most one incomplete
quarter and maps exactly its three monthly partitions. It does not infer a
rolling range from the current date. The DAG retains
`max_active_runs=1`; mapped task concurrency remains bounded, and the
one-slot `cds_api` pool allows only one extraction submission at a time.

The campaign cadence is configured but disabled by default; the DAG remains
manual-only during Stage 8 verification.

### Scheduled and Manual Mode Contract

`orchestration/mode.py` defines the contract used by the existing DAG.
Airflow's DagRun `run_type` determines the mode; operators do not pass a
`mode` parameter. A scheduled DagRun must have empty `dag_run.conf`. It will
select the earliest eligible incomplete configured campaign quarter from
durable progress when scheduled operation is enabled. It does not take dates from Airflow's
scheduled data interval.

A manual DagRun must provide exactly two inclusive dates in `dag_run.conf`:

```json
{"start_date":"2024-01-01","end_date":"2024-01-31"}
```

Both must be canonical `YYYY-MM-DD` dates with `end_date >= start_date`.
Incomplete pairs, extra keys (including `mode`), invalid dates, configured
scheduled runs, and unsupported run types fail explicitly. Existing request
validation still checks the selected historical range and availability lag.
For recovery, prefer one failed month or quarter. Manual ranges may also be
chosen explicitly for other historical work; they never cause the scheduler
to skip incomplete campaign quarters without verified progress evidence.

The DAG now reads `dag_run.run_type` and `dag_run.conf` through this contract.
It remains manual-only by default because
`ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` resolves to `schedule=None`. Scheduled
operation still requires the planned pilots and rollout review.

### Scheduler Cadence vs CDS Submission Interval

These are deliberately separate operational controls:

```text
scheduler cadence
        =
seberapa sering Airflow mencoba menjalankan pipeline

CDS submission interval
        =
jarak minimum antar request aktual ke CDS
```

Scheduler cadence controls how often Airflow attempts to select/start work.
`orchestration/schedule.py` reads `ERA5_CAMPAIGN_SCHEDULE_ENABLED` and
`ERA5_CAMPAIGN_SCHEDULE_CADENCE` from the DAG parser's environment.
The explicit enable value is `true`; the default is `false` and resolves to
`schedule=None`. The default cadence after enabling is `2h`:

| Cadence value | Jakarta-local cron tick |
| --- | --- |
| `30m` | `*/30 * * * *` |
| `1h` | `0 * * * *` |
| `2h` | `0 */2 * * *` |

The DAG's timezone-aware start date is in `Asia/Jakarta`, so these ticks
follow the Jakarta clock. Set the same environment on every DAG parser and
restart/reparse the DAG after changing it. The toggle is prepared for the
scheduled pilot; do not enable it solely to inspect the configuration.
Changing cadence does not require a database migration or change request
identity. `max_active_runs=1` remains the overlap guard; a tick does not
select a fixed calendar quarter or change the 30-minute CDS submission gate.

CDS submission interval controls provider pacing between actual retrieval
submissions. Set `CDS_MIN_SUBMISSION_INTERVAL_SECONDS` in the runtime environment;
the default is `1800` seconds (30 minutes), an operational starting point rather
than a provider quota. A dedicated `climate_data.cds_submission_pacing` row uses the
PostgreSQL clock and a row lock to coordinate manual and scheduled attempts
across workers. The CDS HTTP session reserves it immediately before each
submission POST, including internal library retries, extra local-day context
chunks, and Airflow retries. Status polling and downloads do not reserve slots. The one-slot
`cds_api` pool continues to limit concurrent extraction.

When a slot is not ready, the extraction operator defers to the triggerer and
rechecks the database clock on wakeup. Completed retrieval chunks are saved
atomically under `data/.cds_retrieval_chunks/<request_key>/` and reused after
deferral. After the validated final RAW checkpoint is written, its chunk cache
is removed. If a cached chunk is damaged or its identity is wrong, extraction
fails for inspection rather than silently downloading it again. The task does
not count a pacing wait as a provider failure. Keep the shared `data/` mount
available to every worker and the triggerer service running.

One monthly partition may need two POSTs: a previous-month context chunk and
the target-month chunk. For example, June 2016 first retrieves May 31 for the
UTC+7 daily boundary. If that context POST is reserved at 10:30:28 with a
30-minute operational interval, the June POST cannot be reserved before
11:00:28. The operator may therefore wake at 10:30, download context, and
defer again for the target. The cached context is reused on the next wakeup.
`next_allowed_at` is a minimum submission time, not a task start or finish
promise; another worker's reservation can move the next opportunity later.
Inspect the `chunk` field and the `Resumed CDS retrieval chunk` event before
classifying a repeat deferral as a failed retry.


Neither cadence nor provider pacing belongs to the durable identity of
`climate_data.pipeline_progress` or `climate_data.pipeline_progress_detail`.

### Generic Progress Persistence Foundation

Stage 8 has cadence-neutral PostgreSQL progress integrated into the existing
DAG. Scheduling remains disabled by default while controlled rollout remains
pending. Scheduler state is not stored using quarter-specific table names. All
five application tables now share the `climate_data` schema:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
climate_data.pipeline_progress
climate_data.pipeline_progress_detail
climate_data.cds_submission_pacing
```

`pipeline_progress` represents one logical requested range or batch using
explicit `start_date` and `end_date`. The current scheduler may choose a
three-month quarter, but the persistence model must also support a future
two-month, one-month, annual, or manually selected range without a schema
rename.

`pipeline_progress_detail` represents each processed partition and records
the request/artifact/validation outcome needed for recovery and monitoring.
For the current ERA5-Land pipeline this includes the monthly request key, RAW
NetCDF artifact identity, transformed artifact identity, temporal/spatial
validation metrics, grid and row counts, extract/transform/load state, and
validation state.

Stable identity, status, validation, period, checksum, and count fields remain
ordinary columns. An optional JSONB metrics field may hold evolving operational
measurements, but it must not replace core progress truth.

The existing quarter helper remains scheduler policy for the bounded campaign
configured by environment; quarter number is not part of the durable
persistence identity. Parent completion is derived from persisted detail evidence: every
expected partition must have complete extract/transform/load state and valid
validation state. The current DAG records each mapped boundary against the selected progress ID,
while its schedule remains disabled.

The campaign helper can seed all configured quarter batches idempotently. Parent
reconciliation requires each completed monthly detail to match a canonical
`climate_data.daily_climate` row by request key, source, dataset, and exact
period, with matching transformed path/checksum and observation count. Missing
or changed persisted evidence clears a previously complete parent on the next
reconciliation. Historical rows already in `daily_climate` are not
automatically marked valid: checkpoint and progress-detail validation must
still supply evidence. The selector seeds the fixed campaign identities, reconciles quarters in calendar
order, and returns the first eligible incomplete quarter with exactly three
monthly request specifications. It returns no selection once all configured quarters are verified complete, or when the next quarter has not cleared the six-day availability
boundary. It selects only the exact initialized campaign batch IDs, so unrelated
manual ranges and configuration identities cannot displace a campaign quarter.
The DAG invokes this selector for scheduled run provenance. Scheduling is
disabled by default because `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` resolves
to `schedule=None`; enabling it selects the configured cadence without
changing request or progress identity. Manual triggers create or reuse their
exact range identity. Each mapped boundary records status and
artifact identity; loading becomes valid only after matching PostgreSQL
metadata and a positive observation count are verified.

Scheduled selection now writes a durable owner claim on the earliest incomplete
quarter with an atomic PostgreSQL update. A competing selector cannot advance
to another quarter while that claim remains. Completion releases the claim.
A quarter with a persisted failed detail is not submitted to CDS again on
subsequent scheduled ticks; the task logs the quarter, failed month, and
diagnostic reason and maps no extraction tasks. This bounds automatic
resubmission to the Airflow retries within the original DagRun. A validated
whole-month manual repair clears the failed detail and releases the claim once
no failed detail remains, allowing the next scheduled selection to resume.

A process termination can leave a claim without a failed detail. It remains
blocked instead of silently duplicating CDS work. Inspect the original DagRun
and its artifacts, then use an explicit whole-month manual recovery to
validate and release the claim. Do not clear a claim while its DagRun is active.
The progress-table setup adds claim columns to an existing development schema
with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. The schedule is still
disabled pending scheduled-selection and recovery pilots and final rollout checks.

## Offline Scheduled Selection Pilot

Run the deterministic pilot without a CDS credential or a campaign database:

```bash
python -m pytest tests/airflow_orchestration/test_scheduled_selection_pilot.py -q
```

The test uses the real scheduled mode, earliest-incomplete selector, and
monthly mapping boundary with isolated in-memory progress state. It verifies
2016 Q1 maps January–March, a competing tick stays on the claimed Q1,
completion moves to Q2, a failed month blocks further selection, a simulated
manual repair allows Q3, and all configured complete quarters produce no work. No
extraction task executes, and the test fails if it tries to call CDS. This
checks orchestration decisions; it does not demonstrate a PostgreSQL-backed
claim or a live scheduled DagRun. Keep the deployed schedule disabled until
the separate live pilot is reviewed.

## Controlled Scheduled Pilot on the Airflow Host

This pilot is an actual scheduled DagRun. It can submit the earliest incomplete
quarter to CDS (normally 2016 Q1 if no earlier campaign quarter is complete).
Run it only on the Airflow host with access to the normal PostgreSQL database,
CDS credentials, shared artifacts, and a running triggerer. Do not use
`airflow dags trigger` for this check: that CLI command creates a **manual**
DagRun and requires explicit dates.

1. Record the baseline and check free disk space before starting:

   ```bash
   source activate-airflow.sh
   python scripts/report_campaign_progress.py
   python scripts/inspect_campaign_capacity.py
   airflow dags list-runs cds_era5_land_etl
   ```

   Inspect the capacity snapshot. The prior development-host snapshot had
   only about 6 GiB free on the artifact/log filesystem, and the task warning
   threshold is 5 GiB. Free adequate space before a three-month extraction;
   the snapshot alone cannot prove that the pilot will fit. Confirm there is
   no active manual or scheduled DagRun.

2. Keep the DAG paused while changing its schedule. Stop the Airflow DAG
   processor and scheduler, then start them again with these variables in
   **both** service environments (after `source activate-airflow.sh`):

   ```bash
   export ERA5_CAMPAIGN_SCHEDULE_ENABLED=true
   export ERA5_CAMPAIGN_SCHEDULE_CADENCE=2h
   ```

   Restart the API server as well if it parses DAG files separately. Leave the
   triggerer running. Confirm in the Airflow UI that the DAG's schedule is
   `0 */2 * * *` in Asia/Jakarta, `catchup=False`, and
   `max_active_runs=1`. Use `airflow dags unpause cds_era5_land_etl` once
   the configuration has propagated. Wait for a DagRun whose `run_type` is
   `scheduled` and whose conf is empty; the next tick may take up to two
   hours.

3. Wait for the first scheduled DagRun and record its **full** run ID.
   Verify its `run_type` is `scheduled`, its conf is empty, and
   `build_monthly_requests` selects the earliest unfinished quarter with
   exactly three months. Do not start a manual DagRun in parallel. If
   selection reports a block or no work, pause the DAG and inspect the
   report and claim before proceeding.

4. Monitor the first run through its three mapped extraction,
   transformation, and load instances, including any paced CDS deferrals.
   Check the final DagRun state and CDS reservation timestamps. The
   two-hour cadence can create another tick while this run is still active;
   `max_active_runs=1` should keep it from executing concurrently. Inspect
   `airflow dags list-runs cds_era5_land_etl` around that tick. Pause the DAG
   after the first run completes and before another queued run starts. If
   another run has already started, pause and inspect its state; do not claim
   that this was a one-run pilot. Pausing a DAG can stop scheduling pending
   tasks, so do not assume that pausing an active run lets it finish.

   After the pause, collect:

   ```bash
   python scripts/report_campaign_progress.py
   python scripts/inspect_campaign_capacity.py
   airflow dags list-runs cds_era5_land_etl
   ```

   A successful 2016 Q1 pilot should show all three months valid and 2016 Q1
   complete. Compare the report with the baseline; the already completed
   2024 Q2 must remain complete. If the run fails or is interrupted, leave
   the DAG paused, inspect the failed month and claim, and use the documented
   whole-month manual repair before another scheduled tick.

5. Keep the DAG paused after the pilot. Restore
   `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` in the DAG processor and scheduler
   environments when restarting services. Verify the parsed schedule returns
   to `None`. Review the recorded run, resource use, and progress before
   deciding on continuous campaign operation.

The offline selector test and `max_active_runs=1` policy check do not prove
live scheduler behavior. The first scheduled run verifies the scheduled mode and three-month load.
If it extends past another tick, compare run states to verify no active
overlap; an earlier completion does not exercise that case.

## Campaign Progress Summary

After the progress tables exist, run this read-only command from the repository
root with the normal database environment:

```bash
source activate-airflow.sh
python scripts/report_campaign_progress.py
```

It lists completed quarters per year (for example, `2016: 3/4 quarters
complete`), totals for the fixed 40-quarter campaign, partial quarters with
their completed monthly counts and each month's recorded extract, transform,
load, and validation state, failed quarter reasons, and the month and timestamp
of the most recent recorded valid load. A result of `0/40 quarters
complete` can coexist with a valid monthly load when the other months of its
quarter are still incomplete. The command matches each current
quarter's exact source, dataset, dates, and configuration identity; unrelated
manual ranges and old configurations are excluded. A missing quarter appears
as incomplete. This is a snapshot of persisted progress, not a new
reconciliation or a provider request. Scheduled selection still reconciles
its earliest unfinished quarter against persisted observation evidence.

The selection task also logs the selected year and quarter, the three month
labels, the run mode, progress ID, and a reason when automatic selection is
blocked. CDS pacing decisions remain in the mapped extraction log. The
summary stays in the terminal/logs and is never placed in XCom.

### Complete a Partial Quarter by Manual Recovery

If the report shows one incomplete whole month in a partial quarter, first
check that the corresponding DagRun is no longer active. Trigger **only that
exact month** through the existing DAG, then rerun the report. For example,
if `2024-04` is pending in `2024 Q2`:

```bash
airflow dags trigger cds_era5_land_etl \\
  --conf '{"start_date":"2024-04-01","end_date":"2024-04-30"}'
airflow dags list-runs cds_era5_land_etl
python scripts/report_campaign_progress.py
```

The manual run validates and resumes existing RAW and transformed checkpoints
when present, verifies the persisted load, and reconciles its exact campaign
month. Confirm that the manual DagRun succeeds and the partial quarter reaches
`3/3` months. With the current `0/40` baseline, a successful April repair
should make the report show `2024: 1/4 quarters complete` and
`Campaign 2016-2025: 1/40 quarters complete`. A
recorded monthly state is a diagnostic; parent completion still requires
matching persisted observation evidence.

The April 2024 recovery performed on 2026-09-27 advanced 2024 Q2 from
`2/3` monthly partitions to complete and changed the report from `0/40`
to `1/40` quarters. The last recorded valid load was April at 10:36:10
Asia/Jakarta. The April manual DagRun subsequently reached `success` at
10:36:11, approximately 12 seconds after its start. Scheduled selection remains disabled
pending its own pilot.

## Campaign Host Capacity and Post-Change CDS Pilot

Run this read-only snapshot on the actual Airflow host, from the repository
root, after activating the deployment environment:

```bash
source activate-airflow.sh
python scripts/inspect_campaign_capacity.py > campaign-capacity.json
```

The JSON contains host name, paths, disk bytes available for `data/`, temporary
downloads, Airflow logs and metadata, available RAM and swap, PostgreSQL
database and relevant table sizes, and local triggerer process RSS. It does not
print credentials. Run it while the services and a representative extraction
are active as well as at idle; process RSS is a point-in-time measurement.
The triggerer process count does not establish a healthy heartbeat. A remote
PostgreSQL volume is **not** measured by this command. Its filesystem check
belongs to a future deployment review, if PostgreSQL is hosted separately. If `postgresql.status` says
`not measured`, fix the environment or database access before assessing
capacity. Missing `data/` or Airflow logs paths are also incomplete
measurements, not zero-byte usage. Do not use the development workspace's
numbers as measurements of the campaign host.

Before scheduling, repeat a manual CDS pilot against a month without a RAW
checkpoint. For example, use June 2024 only if its RAW has not already been
created on this host; otherwise choose another reviewed missing month:

```bash
source activate-airflow.sh
airflow dags trigger cds_era5_land_etl \\
  --conf '{"start_date":"2024-06-01","end_date":"2024-06-30"}'
airflow dags list-runs cds_era5_land_etl
```

In the Airflow UI, open that run's mapped `resolve_raw_checkpoint` log, including
the log before a deferral and the log after resumption. Record these lines in
chronological order: `CDS retrieval plan`, `CDS submission reserved`,
`CDS submission deferred`, `Resumed CDS retrieval chunk`, provider timeout
or HTTP retry messages, `RAW Temporal Validation PASSED`, and
`RAW Spatial Validation PASSED`. Also record the run state and the RAW
checkpoint status. Compare the `submitted_at` timestamps of **every**
reservation: each successive POST must be at least
`CDS_MIN_SUBMISSION_INTERVAL_SECONDS` apart (default 1,800 seconds).
A deferred request must not appear as a reserved POST before its
`next_allowed_at`. A provider job POST retry may not occur naturally in this pilot;
if none occurs, record that fact rather than claiming live retry coverage.
The offline test exercises the pinned cdsapi client's internal POST retry after
an injected timeout and proves that the second POST is deferred before it
reaches transport. No live provider failure is needed for this contract.
Do not delete an existing RAW checkpoint or shorten the interval merely to
force this pilot.

### Local Pilot Evidence (2026-09-26)

The June 2024 manual extraction pilot reserved its May 31 context POST at
19:35:24.566821 Asia/Jakarta, deferred June while the slot was unavailable,
then resumed the cached context chunk and reserved June at 20:05:35.214587.
The interval was 1,810.648 seconds, exceeding the configured 1,800-second
minimum. The June object-store download had an `IncompleteRead` and retried
after 120 seconds; this was a file download retry, **not a CDS job POST
retry**. June RAW validated 744 hourly steps, 1,505 grid points, and the five
requested variables. The supplied extract log does not prove that transform,
load, or the DagRun completed.

The local `cassiopeia-node` snapshot at 19:31 Asia/Jakarta reported 5.97 GiB
free for artifacts, logs, and SQLite metadata on the same filesystem,
53.59 GiB free in `/tmp`, 5.70 GiB available RAM, 2.67 GiB free swap, and
a 23.05 MiB PostgreSQL database. Two triggerer-related processes were visible
at approximately 169.5 and 147.3 MiB RSS; process count alone does not
establish separate triggerer instances or heartbeat health. This snapshot
does not measure peak overlapping task memory or a future deployment host.
Watch local disk headroom while running a local campaign and review capacity
when deploying to another host.

## Progress Backup and Restart Recovery

The local Airflow metadata database is SQLite, while the campaign truth is in
PostgreSQL. Keep the PostgreSQL database, the shared `data/` artifacts, and
Airflow metadata together when preparing a recoverable snapshot. The PostgreSQL
backup must include both `climate_data.pipeline_progress` /
`climate_data.pipeline_progress_detail` and `climate_data.cds_submission_pacing`,
as well as `climate_data.daily_climate` /
`climate_data.daily_climate_detail`. A progress-only restore cannot prove
completed loads and a missing pacing clock can permit an early CDS submission.

For a coordinated maintenance backup:

1. Pause the DAG and wait for every active task and deferred extraction to
   finish. Stop the scheduler, triggerer, and API server before copying local runtime state.
   Do not treat a paused DAG as proof that running tasks have stopped.
2. Back up the complete PostgreSQL database with `pg_dump -Fc` using a
   PostgreSQL account authorized to read both schemas. Set `PGHOST`,
   `PGPORT`, `PGUSER`, and `PGDATABASE` from the deployment configuration;
   use a protected `PGPASSFILE` or another approved password source instead
   of putting a password in the command line:

   ```bash
   pg_dump -Fc --file=/secure-backups/climate-and-progress.dump
   ```

3. Copy `data/` (including RAW, transformed, and any unfinished
   `.cds_retrieval_chunks/`), the local Airflow metadata state, and the
   protected JWT secret to a protected backup location. Keep the artifact paths
   and worker mount layout consistent for restore. Record the snapshot time
   and verify that the dump and copied files can be read.

Restore into an **isolated empty database and isolated artifact directory**
first, using `pg_restore --dbname=<empty-test-database>
--single-transaction /secure-backups/climate-and-progress.dump`; restore the
artifact copy and check representative RAW and transformed checksums. This is
a restore drill, not a command to overwrite the live database. A live restore
requires a maintenance window, a reviewed database cutover, and a consistent
artifact snapshot. The Airflow SQLite metadata file alone does not restore
campaign truth.

After a restart, keep the schedule disabled until PostgreSQL, the shared
artifact paths, and the triggerer are reachable. Inspect the latest DagRun
and any quarter with a non-null `selection_owner`. A terminated run can leave
an owner claim and a `running` detail; this intentionally prevents a new
automatic submission. Do not delete progress rows, reset the pacing row, or
clear a claim by hand while the original DagRun might still be active.
After confirming it has ended, trigger an explicit whole-month manual repair
for the affected month. The normal boundaries validate or resume checkpoints,
verify the persisted load, reconcile the quarter, and release its claim when
no failed detail remains. A `failed` detail also blocks automatic selection
until repaired. A successful Airflow task state alone does not mark a quarter
complete; reconciliation verifies all three monthly details against persisted
observations. Once repair is complete, review the quarter status and resume
scheduled processing only after the operational rollout checks pass.

PostgreSQL's [`pg_dump`](https://www.postgresql.org/docs/16/app-pgdump.html)
and [`pg_restore`](https://www.postgresql.org/docs/16/app-pgrestore.html)
documentation describes the archive and restore commands.

## Task Flow

```text
manual dates or scheduled quarter
               |
               v
    build_monthly_requests
               |
               v
 attempt_raw_checkpoint [monthly]
               |
               v
    prepare_retry_pass
               |
               v
 resolve_raw_checkpoint [monthly]
               |
               v
ensure_transformed_checkpoint [monthly]
               |
               v
        load [monthly]
```

### Task Principles

- **Airflow as orchestrator:** Airflow triggers, retries, and monitors the
  existing pipeline boundaries.
- **Thin DAGs:** DAG files define parameters, dependencies, timeouts, retries,
  pools, and mapping without containing scientific logic.
- **Checkpoint-backed tasks:** Each task resumes and validates deterministic
  artifacts rather than silently overwriting them.
- **Artifact-based communication:** NetCDF, Parquet, xarray, and Pandas objects
  never enter XCom. Tasks exchange only small JSON-serializable metadata.
- **Pipeline-owned validation:** RAW and transformed validation remain inside
  the existing runtime boundaries rather than becoming separate Airflow tasks.

### Mapped task labels

Monthly mapped task instances use Airflow named mapping with a `YYYY-MM`
display label derived from the request start date. The DAG ID and task IDs stay
stable while the UI can distinguish the active month, for example:

```text
attempt_raw_checkpoint [2016-01]
attempt_raw_checkpoint [2016-02]
attempt_raw_checkpoint [2016-03]

resolve_raw_checkpoint [2016-01]
ensure_transformed_checkpoint [2016-01]
load [2016-01]
```

The display label is observability metadata only. Request identity, artifact
paths, PostgreSQL progress identity, and pipeline boundary names do not depend
on it.

### Task Breakdown

| Task                      | Input                         | Output                            |
| ---                       | ---                           | ---                               |
| `build_monthly_requests`  | Explicit inclusive dates      | Monthly request specifications    |
| `attempt_raw_checkpoint`   | Monthly request specification | RAW metadata or failure control   |
| `prepare_retry_pass`      | Three first-pass results      | Monthly retry/pass-through inputs |
| `resolve_raw_checkpoint`      | Monthly retry/pass-through    | RAW artifact metadata             |
| `ensure_transformed_checkpoint`     | RAW artifact metadata         | Transformed artifact metadata     |
| `load`                    | Transformed artifact metadata | PostgreSQL load status            |

## Triggering the ETL

Keep Airflow running, then open another terminal and load the same environment:

```bash
source activate-airflow.sh
```

Run a one-month smoke test before any longer period:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-01-01","end_date":"2024-01-31"}'
```

After the one-month run and its identical rerun succeed, test three consecutive
months. Do not submit all 120 campaign months in one DagRun.

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-01-01","end_date":"2024-03-31"}'
```

For example, to process the following new month explicitly:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-04-01","end_date":"2024-04-30"}'
```

### Manual Campaign Recovery

A failed campaign month can be rerun through the same DAG using its complete
calendar-month dates. For example, to repair February 2024:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-02-01","end_date":"2024-02-29"}'
```

After the checkpoint-backed load verifies PostgreSQL evidence, the manual range
and the matching February detail of 2024 Q1 are reconciled. Q1 becomes complete
only when January, February, and March all have valid persisted evidence. A
partial-month range, an out-of-campaign period, or a changed request
configuration does not modify the campaign quarter. Existing historical
database rows alone are not silently promoted; rerun the missing whole months
to validate their checkpoints and record their progress.

### Check DagRun Status

`airflow dags trigger` creates a **new** DagRun and returns immediately. The
`queued` state printed by this command is its initial state, not the final
result. Every repeated trigger creates another DagRun, even when the date range
and its checkpoints are identical. Check the existing runs rather than
triggering the same period again to see whether it finished:

```bash
source activate-airflow.sh
airflow dags list-runs cds_era5_land_etl
```

Match the `run_id` returned by the trigger to the `run_id` in this list, then
read its current `state` (`queued`, `running`, `success`, or `failed`).
Alternatively, open **Dags → cds_era5_land_etl → Runs** in the Airflow UI and
select that exact run to inspect its mapped task statuses and logs. A
`success` on one run does not change the initial `queued` state printed by
a later trigger. The current DAG has `max_active_runs=1`, so another run can
wait while one is active.

### Verified Three-Month Rollout

The January-March 2024 rollout was verified on 2026-09-24. January had already
completed before this run, so its valid RAW NetCDF and transformed Parquet
checkpoints were resumed and validated. Its unchanged PostgreSQL partition had
also previously been confirmed idempotent: the request key, artifact path,
checksum, and 16,430-row count matched, so the database write was skipped.

The three-month DagRun therefore downloaded only the two missing monthly RAW
partitions, February and March. The one-slot `cds_api` pool downloaded them
serially. All three mapped extraction, transformation, and load task instances
completed successfully in approximately 20 minutes 26 seconds.

| Measurement | Before | After | Added by missing months |
| --- | ---: | ---: | ---: |
| RAW directory (`du -sh`) | 5.9 MB | 16 MB | approximately 10.1 MB |
| Transformed directory (`du -sh`) | 532 KB | 1.5 MB | approximately 1.0 MB |
| RAW file payload | January already present | 3 monthly files | 10,221,652 bytes |
| Parquet file payload | January already present | 3 monthly files | 921,253 bytes |

PostgreSQL contained the expected local-day observations after the run:

| Partition     | Local days    | Observation rows  |
| ---           | ---:          | ---:              |
| January 2024  | 31            | 16,430            |
| February 2024 | 29            | 15,370            |
| March 2024    | 31            | 16,430            |
| **Total**     | **91**        | **48,230**        |

This case demonstrates that a wider explicit date range can combine an
idempotently resumed partition with newly downloaded partitions. Timings and
storage are operational samples, not fixed CDS service guarantees.

For Stage 8, manual recovery remains explicit but should normally target the
smallest useful scope, such as one failed month or one affected quarter:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2016-01-01","end_date":"2016-03-31"}'
```

A manual annual range remains an operator option when deliberately needed:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2016-01-01","end_date":"2016-12-31"}'
```

The scheduled 2016-2025 campaign must not be started as one unpaced
120-month DagRun. After Stage 8 is enabled, scheduled runs select the earliest
eligible incomplete quarter from durable campaign progress. Manual repairs and
scheduled runs share the same DAG and `max_active_runs=1`.

## Peak Memory in Task Logs

Each mapped `resolve_raw_checkpoint`, `ensure_transformed_checkpoint`, and `load` task
logs a `[METRIC] task memory` line after its runtime boundary, including when
that boundary raises an exception. For example:

```text
[METRIC] task memory | boundary=ensure_transformed_checkpoint | pid=12345 | process_peak_rss_mib=842.31 | peak_before_mib=215.42
```

`process_peak_rss_mib` is the operating system's peak resident memory
(`ru_maxrss`) for the task **process**, reported in MiB on Linux.
`peak_before_mib` is the same high-water mark immediately before the boundary.
These are process high-water marks: if a worker process has previously reached a
higher peak, the final value can reflect that earlier work. Do not subtract the
two fields to estimate memory allocated by a task. The metric excludes memory
in separate child processes and reports a process peak rather than total host
or concurrent-worker memory. The process ID helps correlate measurements.

The January–March 2024 rollout preceded this instrumentation and has no
recorded memory measurements. An April 2024 fresh run on 2026-09-25 supplied
these task-process peak RSS measurements:

| April boundary | Peak RSS (MiB) | Outcome |
| --- | ---: | --- |
| `ensure_raw_checkpoint` | 517.59 | RAW created and validated |
| `ensure_transformed_checkpoint` | 572.22 | Parquet created; 15,900 daily observations |
| `load` | 307.30 | 15,900 observations processed |

The April run succeeded in about 33 minutes 36 seconds. Seven CDS download
read timeouts were recovered, so this wall-clock duration is not a stable
throughput estimate. A subsequent run resumed April RAW with a peak RSS of
457.25 MiB. These are per-process high-water marks, not host-wide peak RAM;
assess concurrent tasks along with Airflow, PostgreSQL, and system overhead
before increasing concurrency. Memory metrics stay in task logs and do not
enter XCom.

## Disk Space Warnings

Before each mapped `ensure_raw_checkpoint`, `ensure_transformed_checkpoint`, and `load`
boundary, the worker checks available space on the filesystem holding the
repository `data/` directory and on its temporary-file filesystem. If both
locations share one filesystem, it checks that filesystem once. When less than
5 GiB is available, task logs emit a warning such as:

```text
[DISK] low free space | boundary=ensure_raw_checkpoint | location=artifacts | path=/path/to/data | available_gib=3.60 | threshold_gib=5
```

If the worker cannot determine free space, it logs
`[DISK] free space check unavailable` (or
`[DISK] temporary directory unavailable`). These warnings do not fail the
task or prevent a later write from running out of space. The check is local
to the worker: a remote PostgreSQL server or its separate storage volume must
be monitored independently. The threshold is an operational early warning,
not a capacity guarantee for a particular date range.

## Safe Retry and Resume

- A valid existing RAW NetCDF checkpoint is resumed and validated.
- A valid existing transformed Parquet checkpoint is resumed and validated.
- PostgreSQL loading remains transactional and idempotent.
- Clearing a failed task does not authorize artifact overwrite.
- `run_id`, task attempts, and wall-clock time do not affect request or
  artifact identity.

If a task log stops at `Pre Execute` while standalone startup warns that
`api_auth/jwt_secret` is empty, stop all Airflow components, configure one
stable secret with `./generate-airflow-jwt-secret.sh`, and restart Airflow.
Separate random secrets prevent Airflow components from authenticating task
execution; they do not indicate a CDS or pipeline-data failure.

Before enabling the full quarterly campaign, inspect artifact counts,
provenance, row counts, disk usage, memory, task duration, database behavior,
and durable campaign progress on the target runtime. Diagnose a failed mapped
partition before manual repair or allowing that quarter to advance.

## Tests

Run `source activate-airflow.sh` before Airflow-specific tests. It activates
`myvenv`, loads `.env`, and configures the repository-local Airflow runtime.

Offline unit and orchestration regression:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit tests/airflow_orchestration -q
```

This suite covers DAG parsing and graph policy, scheduled/manual selection,
first-pass barrier and pass-through, +0/+1/+2-hour scheduled offsets, pacing
deferral behavior, monitoring paths, and the Airflow-independent pipeline
boundary.

PostgreSQL integration behavior on the isolated `TEST_POSTGRES_*` database:

```bash
python -m pytest \
  tests/integration/test_postgresql_idempotency.py \
  tests/integration/test_pipeline_progress_reconciliation.py \
  tests/integration/test_cds_submission_pacing.py \
  tests/integration/test_orchestration_database_bootstrap.py \
  -q
```

The bootstrap integration test drops only the `climate_data` schema inside
the explicitly isolated test database, then verifies fresh and repeated
creation of all five application tables.

## Cleanup and Local State

Stop standalone mode or each individual service with `Ctrl+C`. Confirm that no
Airflow process is still using the metadata database before performing local
maintenance.

`orchestration/logs/`, `orchestration/airflow.db`, PID files, and generated
authentication files are local runtime state and are ignored by Git. Database
reset is a deliberate recovery operation, not part of normal shutdown, retry,
or historical ETL execution. Do not remove `orchestration/airflow.db` while Airflow
services are running.


### Scheduled monthly slots and retry barrier

For a scheduled quarter, the three mapped months receive earliest extraction
starts at selection time, initially separated by one hour. All maps belong to
one DagRun and retain its single logical date. Slots are lower bounds; worker
availability, CDS queueing, context chunks, and the global PostgreSQL pacing
clock can delay actual starts and completion. The CDS clock has a separate
30-minute operational default and applies to every actual submission in both
scheduled and manual modes. Neither spacing value is a Copernicus requirement.
Manual recovery receives no scheduled start offset.

The `attempt_raw_checkpoint` map gives each month one initial attempt to
ensure a valid RAW checkpoint (including worker-free pacing deferrals). It may
resume an existing checkpoint or download and validate a new one. A real
failure is converted to bounded control metadata so the other months can finish
their initial pass. `prepare_retry_pass` waits for all initial maps; then
`resolve_raw_checkpoint` passes through successful months and retries failed
months with one Airflow retry, reusing validated checkpoints. Together with the first
pass, this preserves the original maximum of three extraction attempts. Terminal failures
remain failed in durable progress and block the next quarter until verified
manual repair. Transform and load follow the resolved extraction results.

Before resuming the campaign, check the Airflow UI or CLI for **active and
queued DagRuns** and their mapped task states. Pausing the DAG prevents new
scheduled runs but does not stop an already running task. Run the offline
Airflow suite, inspect the 2016 Q1 campaign report and the three database
partitions, and keep the DAG paused until that evidence and a post-change pilot
are reviewed. The observed first run took 2h 45m 52s, above the configured
2-hour tick; `max_active_runs=1` prevents overlap but can leave ticks queued
or delayed. Review more than one runtime and the queue before adjusting cadence.

### Deploying DAG changes during a campaign

Do not pull or restart with a changed DAG graph while a DagRun is active.
Airflow may show one DagRun with task instances from two DAG versions, remove
old mapped instances, and schedule new mapped instances after reparsing. Such
a mixed-version run cannot verify a new earliest-start policy: its completed
`build_monthly_requests` XCom may have been produced by the previous version
without `earliest_start_at`. Keep automatic scheduling disabled, inspect every
active and deferred task, and finish or diagnose the current run before a new
pilot. Preserve Airflow metadata, PostgreSQL progress, and checkpoint files.
A clean post-change scheduled pilot must start and complete under the new DAG
version and be reconciled against persisted database evidence.
