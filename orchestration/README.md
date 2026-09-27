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

   The project expects `CDSAPI_URL`, `CDSAPI_KEY`, and the pipeline
   `POSTGRES_*` variables in `.env`.
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

For local development, run Airflow in standalone mode. This starts the
required Airflow services in one process.

```bash
source activate-airflow.sh
airflow standalone
```

Open `http://localhost:8080`. Use the credentials displayed by the standalone
command or written by the configured Airflow authentication manager. Stop the
services with `Ctrl+C` in the terminal running Airflow.

### Alternative: Running Services Individually

For more granular control, open four terminals. Run the environment setup in
each terminal before starting its service:

```bash
source activate-airflow.sh
```

Then start one service per terminal:

1. **Terminal 1 — Scheduler**

   ```bash
   airflow scheduler
   ```

2. **Terminal 2 — API server and web UI**

   ```bash
   airflow api-server
   ```

3. **Terminal 3 — DAG processor**

   ```bash
   airflow dag-processor
   ```

4. **Terminal 4 — Triggerer**

   ```bash
   airflow triggerer
   ```

   The triggerer wakes extraction tasks after a CDS pacing deferral.

The Airflow 3 CLI uses `api-server`; the older `airflow webserver` command does
not apply to the pinned version. Stop each service with `Ctrl+C` in its own
terminal.

## Project Structure

```text
etl-weather/
├── activate-airflow.sh
├── airflow-jwt-key.txt             # generated; ignored by Git; mode 600
├── generate-airflow-jwt-secret.sh
└── orchestration/
    ├── dags/
    │   └── cds_era5_land_etl.py
    ├── docs/
    │   └── IMPLEMENTATION_PLAN.md
    ├── logs/                    # generated; ignored by Git
    ├── tasks/
    │   └── era5_land.py
    ├── airflow.cfg
    ├── airflow.db               # generated; ignored by Git
    └── README.md
```

## ERA5-Land ETL DAG

DAG ID:

```text
cds_era5_land_etl
```

The currently implemented baseline is manually triggered with explicit
inclusive `start_date` and `end_date` parameters. Stage 8 extends this same
DAG with a bounded scheduled quarterly campaign for 2016-01-01 through
2025-12-31 while preserving explicit manual recovery through the same DAG.

The scheduled campaign consists of 40 calendar quarters and 120 monthly
partitions in total, but each automatic DagRun selects at most one incomplete
quarter and maps exactly its three monthly partitions. It does not infer a
rolling ten-year range from the current date. The DAG retains
`max_active_runs=1`; mapped task concurrency remains bounded, and the
one-slot `cds_api` pool allows only one extraction submission at a time.

The campaign cadence is configured but disabled by default; the DAG remains
manual-only during Stage 8 verification.

### Scheduled and Manual Mode Contract

`orchestration/mode.py` defines the contract used by the existing DAG.
Airflow's DagRun `run_type` determines the mode; operators do not pass a
`mode` parameter. A scheduled DagRun must have empty `dag_run.conf`. It will
select the earliest eligible incomplete 2016–2025 quarter from durable
progress when scheduled operation is enabled. It does not take dates from Airflow's
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
than a provider quota. A dedicated `climate.cds_submission_pacing` row uses the
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

Neither cadence nor provider pacing belongs to the durable identity of
`climate.pipeline_progress` or `climate.pipeline_progress_detail`.

### Generic Progress Persistence Foundation

Stage 8 has cadence-neutral PostgreSQL progress integrated into the existing
DAG. The schedule is still disabled while recovery and provider pacing remain
under development. Scheduler state is not stored using quarter-specific table names. The
implemented model remains in the existing climate database:

```text
climate.pipeline_progress
climate.pipeline_progress_detail
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

The existing quarter helper remains scheduler policy for the bounded
2016-2025 campaign; quarter number is not part of the durable persistence
identity. Parent completion is derived from persisted detail evidence: every
expected partition must have complete extract/transform/load state and valid
validation state. The current DAG records each mapped boundary against the selected progress ID,
while its schedule remains disabled.

The campaign helper can now seed all 40 quarter batches idempotently. Parent
reconciliation requires each completed monthly detail to match a canonical
`climate_data.daily_climate` row by request key, source, dataset, and exact
period, with matching transformed path/checksum and observation count. Missing
or changed persisted evidence clears a previously complete parent on the next
reconciliation. Historical rows already in `daily_climate` are not
automatically marked valid: checkpoint and progress-detail validation must
still supply evidence. The selector seeds the fixed campaign identities, reconciles quarters in calendar
order, and returns the first eligible incomplete quarter with exactly three
monthly request specifications. It returns no selection once all 40 are verified
complete, or when the next quarter has not cleared the six-day availability
boundary. It selects only the exact initialized campaign batch IDs, so unrelated
manual ranges and configuration identities cannot displace a campaign quarter.
The DAG invokes this selector for scheduled run provenance, although
`schedule=None` prevents scheduled DagRuns today. Manual triggers create or
reuse their exact range identity. Each mapped boundary records status and
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
manual repair allows Q3, and all 40 complete quarters produce no work. No
extraction task executes, and the test fails if it tries to call CDS. This
checks orchestration decisions; it does not demonstrate a PostgreSQL-backed
claim or a live scheduled DagRun. Keep the deployed schedule disabled until
the separate live pilot is reviewed.

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

In the Airflow UI, open that run's mapped `extract_or_resume` log, including
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
backup must include both `climate.pipeline_progress` /
`climate.pipeline_progress_detail` and `climate.cds_submission_pacing`,
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
 extract_or_resume [monthly]
               |
               v
transform_or_resume [monthly]
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

### Task Breakdown

| Task                      | Input                         | Output                            |
| ---                       | ---                           | ---                               |
| `build_monthly_requests`  | Explicit inclusive dates      | Monthly request specifications    |
| `extract_or_resume`       | Monthly request specification | RAW artifact metadata             |
| `transform_or_resume`     | RAW artifact metadata         | Transformed artifact metadata     |
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

Each mapped `extract_or_resume`, `transform_or_resume`, and `load` task
logs a `[METRIC] task memory` line after its runtime boundary, including when
that boundary raises an exception. For example:

```text
[METRIC] task memory | boundary=transform_or_resume | pid=12345 | process_peak_rss_mib=842.31 | peak_before_mib=215.42
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
| `extract_or_resume` | 517.59 | RAW created and validated |
| `transform_or_resume` | 572.22 | Parquet created; 15,900 daily observations |
| `load` | 307.30 | 15,900 observations processed |

The April run succeeded in about 33 minutes 36 seconds. Seven CDS download
read timeouts were recovered, so this wall-clock duration is not a stable
throughput estimate. A subsequent run resumed April RAW with a peak RSS of
457.25 MiB. These are per-process high-water marks, not host-wide peak RAM;
assess concurrent tasks along with Airflow, PostgreSQL, and system overhead
before increasing concurrency. Memory metrics stay in task logs and do not
enter XCom.

## Disk Space Warnings

Before each mapped `extract_or_resume`, `transform_or_resume`, and `load`
boundary, the worker checks available space on the filesystem holding the
repository `data/` directory and on its temporary-file filesystem. If both
locations share one filesystem, it checks that filesystem once. When less than
5 GiB is available, task logs emit a warning such as:

```text
[DISK] low free space | boundary=extract_or_resume | location=artifacts | path=/path/to/data | available_gib=3.60 | threshold_gib=5
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

Core and Airflow-neutral orchestration helpers:

```bash
python -m pytest tests/unit tests/airflow_orchestration/test_tasks.py -q
```

Airflow DAG parsing and policy:

```bash
python -m pytest tests/airflow_orchestration -q
```

PostgreSQL integration behavior:

```bash
python -m pytest tests/integration/test_postgresql_idempotency.py -q
```

## Cleanup and Local State

Stop standalone mode or each individual service with `Ctrl+C`. Confirm that no
Airflow process is still using the metadata database before performing local
maintenance.

`orchestration/logs/`, `orchestration/airflow.db`, PID files, and generated
authentication files are local runtime state and are ignored by Git. Database
reset is a deliberate recovery operation, not part of normal shutdown, retry,
or historical ETL execution. Do not remove `orchestration/airflow.db` while Airflow
services are running.
