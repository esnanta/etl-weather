# ERA5-Land Orchestration with Apache Airflow

Apache Airflow coordinates the existing ERA5-Land ETL pipeline. Scientific transformation, validation, artifact handling, and PostgreSQL persistence remain owned by `pipeline/`.

For project installation, environment variables, database setup, and the compact CLI index, see [`/README.md`](../README.md).

## Scope

This directory owns:

- Airflow runtime configuration;
- scheduled and manual DAG execution;
- campaign selection;
- CDS submission pacing;
- mapped monthly start slots;
- retry and deferral behavior;
- campaign progress and reconciliation;
- operational recovery and observability.

The separate experimental monthly climate aggregation CLI is not part of the Airflow DAG.

## Airflow Setup

Generate the shared local JWT secret once:

```bash
./generate-airflow-jwt-secret.sh
```

Activate the project environment:

```bash
source activate-airflow.sh -q
```

Initialize or upgrade Airflow metadata and configure the one-slot CDS pool:

```bash
airflow db migrate
airflow pools set cds_api 1 "Copernicus CDS API requests"
```

Start the local Airflow stack:

```bash
airflow standalone
```

The project keeps Airflow metadata separate from the application PostgreSQL database.

## DAG

Active DAG ID:

```text
cds-era5-land
```

Source:

```text
orchestration/dags/cds_era5_land_etl.py
```

The DAG uses `max_active_runs=1`.

## Scheduled and Manual Modes

The DAG derives the mode from the DagRun type.

### Scheduled

Scheduled mode selects the earliest eligible incomplete campaign quarter from durable PostgreSQL progress.

A quarter maps to three monthly TaskGroups:

```text
quarter
  ├── month 1
  ├── month 2
  └── month 3
```

A later quarter is not eligible until the current quarter has complete and valid evidence.

### Manual

Manual mode processes an explicit inclusive range:

```bash
airflow dags trigger cds-era5-land \
  --conf '{"start_date":"2024-02-01","end_date":"2024-02-29"}'
```

Use complete calendar months for campaign repair so successful work can reconcile back into campaign progress.

## Timing and Pacing

Timing policy is source-controlled in root `config.py`.

Current roles:

- campaign schedule controls how often scheduled work is considered;
- quarter cooldown delays selection of the next completed-quarter successor;
- monthly slot spacing staggers the three mapped months;
- CDS submission pacing enforces a minimum interval between submission requests.

Scheduled monthly earliest-start offsets are:

```text
month 1 → +0 minutes
month 2 → +15 minutes
month 3 → +30 minutes
```

These are lower bounds, not guaranteed start times.

Waiting tasks defer rather than occupying a worker slot.

## Retry and Failure Behavior

Each mapped month gets an initial extraction attempt.

A failure in one month does not prevent sibling months from completing their first attempts and downstream work.

Failed months are retried only after the initial extraction pass reaches terminal states. A terminal failed month blocks automatic advancement to later campaign quarters until repaired and reconciled.

## Task Flow

```text
manual dates or scheduled quarter
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

A month may transform and load as soon as its RAW checkpoint is ready.

Airflow exchanges metadata only. NetCDF, Parquet, xarray, and Pandas payloads remain in shared storage.

## Campaign Progress

Campaign progress is persisted in PostgreSQL and does not rely only on Airflow task history.

Current operational tables are derived from `database/config.py` and use the configured schema/prefixes.

A quarter is complete only when all expected monthly partitions have valid persisted evidence.

Inspect campaign progress with:

```bash
python -m scripts.report_campaign_progress
```

Inspect host/campaign capacity with:

```bash
python -m scripts.inspect_campaign_capacity
```

## Trigger and Status

Trigger a manual month:

```bash
airflow dags trigger cds-era5-land \
  --conf '{"start_date":"2024-01-01","end_date":"2024-01-31"}'
```

List existing runs:

```bash
airflow dags list-runs cds-era5-land
```

Use the returned `run_id` when inspecting or recovering a specific DagRun.

## Recovery Rules

When a run is interrupted:

1. confirm the original DagRun is inactive;
2. inspect campaign progress and existing artifacts;
3. avoid deleting valid checkpoints or progress evidence;
4. repair the smallest complete affected calendar month;
5. verify reconciliation before scheduled work continues.

Do not deploy a changed DAG graph into an active DagRun.

For DAG changes:

```text
pause new scheduling
      ↓
finish or diagnose active work
      ↓
deploy DAG change
      ↓
run a clean pilot
      ↓
verify progress and persisted evidence
      ↓
resume scheduling
```

## Backup and Restart

Campaign truth is distributed across:

- PostgreSQL climate/progress/pacing data;
- shared `data/` artifacts;
- Airflow metadata when run history is needed;
- `airflow-jwt-key.txt`.

A paused DAG prevents new scheduled runs but does not stop already running or deferred tasks.

## Reset

Use reset when Airflow has already been fully stopped and the local development state should be rebuilt.

Preview:

```bash
python -m scripts.reset_all
```

Execute:

```bash
python -m scripts.reset_all --execute
```

Reset is restricted to `APP_ENV=dev`. It recreates the configured development PostgreSQL database and clears generated data, Airflow logs, and Airflow runtime state. It refuses to proceed while Airflow remains active.

## Destroy

Use destroy for an intentional clean slate when recognized Airflow processes may still be running.

Preview:

```bash
python -m scripts.destroy_all
```

Execute:

```bash
python -m scripts.destroy_all --execute
```

Destroy is restricted to `APP_ENV=dev`, requires typing exactly `DESTROY`, stops recognized Airflow processes, recreates the configured application database, clears generated data/log/runtime state, and verifies the final clean state.

It preserves project source, `.env`, `orchestration/airflow.cfg`, `airflow-jwt-key.txt`, Git metadata, and the Python virtual environment.

## Testing

For the canonical test commands and isolation rules, see [`tests/README.md`](../tests/README.md).
