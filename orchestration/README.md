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
backfill, move the Airflow metadata database to a supported external database
and validate the deployment again.

## Setup and Installation

1. **Create and activate the project environment:**

   ```bash
   ./scripts/setup_environment.sh
   source myvenv/bin/activate
   ```

   The setup script creates `myvenv` when needed and installs the core pipeline
   dependencies from `requirements.txt`.

2. **Install Apache Airflow:**

   Airflow dependencies remain separately pinned in
   `requirements-airflow.txt`, but are installed into the same `myvenv`
   environment so DAG tasks can execute the pipeline.

   ```bash
   AIRFLOW_VERSION=3.3.2
   PYTHON_VERSION=3.12
   CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

   pip install -r requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
   pip check
   airflow version
   ```

3. **Load credentials and activate the Airflow configuration:**

   Store local values in the untracked `.env` file. Never put credentials in
   DAG parameters, source files, XCom, or logs.

   ```bash
   set -a
   source .env
   set +a

   source orchestration/activate.sh
   ```

   The project expects `CDSAPI_URL`, `CDSAPI_KEY`, and the pipeline
   `POSTGRES_*` variables. The activation script configures `AIRFLOW_HOME`,
   `AIRFLOW_CONFIG`, the DAG directory, and `PYTHONPATH`.

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
source myvenv/bin/activate

set -a
source .env
set +a

source orchestration/activate.sh
airflow standalone
```

Open `http://localhost:8080`. Use the credentials displayed by the standalone
command or written by the configured Airflow authentication manager. Stop the
services with `Ctrl+C` in the terminal running Airflow.

### Alternative: Running Services Individually

For more granular control, open three terminals. Run the environment setup in
each terminal before starting its service:

```bash
source myvenv/bin/activate

set -a
source .env
set +a

source orchestration/activate.sh
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
orchestration/
├── dags/
│   └── cds_era5_land_historical_backfill.py
├── docs/
│   └── IMPLEMENTATION_PLAN.md
├── logs/                    # generated; ignored by Git
├── tasks/
│   └── era5_land.py
├── activate.sh
├── airflow.cfg
├── airflow.db               # generated; ignored by Git
└── README.md
```

## Historical Backfill DAG

DAG ID:

```text
cds_era5_land_historical_backfill
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
    plan_monthly_requests
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
| `plan_monthly_requests` | Explicit inclusive dates | Monthly request specifications |
| `extract_or_resume` | Monthly request specification | RAW artifact metadata |
| `transform_or_resume` | RAW artifact metadata | Transformed artifact metadata |
| `load` | Transformed artifact metadata | PostgreSQL load status |

## Triggering a Backfill

Keep Airflow running, then open another terminal and load the same environment:

```bash
source myvenv/bin/activate

set -a
source .env
set +a

source orchestration/activate.sh
```

Run a one-month smoke test before any longer period:

```bash
airflow dags trigger cds_era5_land_historical_backfill \
  --conf '{"start_date":"2024-01-01","end_date":"2024-01-31"}'
```

After the one-month run and its identical rerun succeed, test three consecutive
months before selecting the complete ten-year range.

Example of an explicit ten-year range:

```bash
airflow dags trigger cds_era5_land_historical_backfill \
  --conf '{"start_date":"2016-01-01","end_date":"2025-12-31"}'
```

An alternative range can be supplied without changing source code:

```bash
airflow dags trigger cds_era5_land_historical_backfill \
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

Before a complete historical run, inspect artifact counts, provenance, row
counts, disk usage, memory, task duration, and database behavior. Diagnose a
failed mapped partition before rerunning it.

## Tests

Activate `myvenv`, load `.env`, and activate the Airflow configuration before
running Airflow-specific tests.

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
or backfill execution. Do not remove `orchestration/airflow.db` while Airflow
services are running.
