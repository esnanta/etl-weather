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

For more granular control, open three terminals. Run the environment setup in
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

The DAG has no schedule and does not infer a ten-year period from the current
date. Every run requires explicit inclusive `start_date` and `end_date`
parameters. The pipeline validates the range and splits it into deterministic
monthly partitions.

Ten complete calendar years produce 120 mapped partitions. The DAG initially
limits itself to two active task instances, while the `cds_api` pool allows
only one extraction at a time.

## Task Flow

```text
explicit start_date and end_date
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

| Task | Input | Output |
| --- | --- | --- |
| `build_monthly_requests` | Explicit inclusive dates | Monthly request specifications |
| `extract_or_resume` | Monthly request specification | RAW artifact metadata |
| `transform_or_resume` | RAW artifact metadata | Transformed artifact metadata |
| `load` | Transformed artifact metadata | PostgreSQL load status |

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
months before selecting the complete ten-year range.

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2024-01-01","end_date":"2024-03-31"}'
```

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

| Partition | Local days | Observation rows |
| --- | ---: | ---: |
| January 2024 | 31 | 16,430 |
| February 2024 | 29 | 15,370 |
| March 2024 | 31 | 16,430 |
| **Total** | **91** | **48,230** |

This case demonstrates that a wider explicit date range can combine an
idempotently resumed partition with newly downloaded partitions. Timings and
storage are operational samples, not fixed CDS service guarantees.

Example of an explicit ten-year range:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2016-01-01","end_date":"2025-12-31"}'
```

An alternative range can be supplied without changing source code:

```bash
airflow dags trigger cds_era5_land_etl \
  --conf '{"start_date":"2014-01-01","end_date":"2024-12-31"}'
```

The second example spans eleven complete calendar years and therefore creates
132 monthly partitions. Airflow always processes the exact supplied range.

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

Before a complete historical run, inspect artifact counts, provenance, row
counts, disk usage, memory, task duration, and database behavior. Diagnose a
failed mapped partition before rerunning it.

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
