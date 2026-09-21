# ERA5-Land Orchestration with Apache Airflow

Apache Airflow orchestrates the existing CDS ERA5-Land pipeline. Scientific
transformation, validation, deterministic artifacts, provenance, and
PostgreSQL persistence remain in `pipeline/`.

Run all commands from the repository root unless a command says otherwise.

## Prerequisites

- Python 3.12
- PostgreSQL for canonical climate observations
- CDS API credentials
- A single host or shared `data/` filesystem visible to every task worker

The initial local configuration uses Airflow 3.3.2, SQLite metadata, and the
`LocalExecutor`. SQLite is suitable for local setup and controlled development.
Before increasing task concurrency for a full historical run, use a supported
external Airflow metadata database and revalidate the deployment.

## Installation

Create a dedicated local environment:

```bash
python3.12 -m venv .venv-airflow
source .venv-airflow/bin/activate
```

Install Airflow with its matching official Python 3.12 constraints, then
install the pipeline dependencies and verify the resolved environment:

```bash
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
pip check
```

The separate requirement files keep the core pipeline installable without
Airflow. Both sets are installed into the same local environment for DAG task
execution.

## Environment Activation

Activate the Python environment and repository-specific Airflow settings:

```bash
source .venv-airflow/bin/activate
source orchestration/activate.sh
```

Provide `CDSAPI_URL`, `CDSAPI_KEY`, and the pipeline `POSTGRES_*` values through
the environment or the deployment's approved secrets mechanism. Do not put
secret values in DAG parameters, source files, XCom, or logs.

For local development, values may be exported from the untracked `.env` file
before starting Airflow:

```bash
set -a
source .env
set +a
```

## Initialize Airflow

Initialize or migrate the local metadata database and create the CDS pool:

```bash
airflow db migrate
airflow pools set cds_api 1 "Copernicus CDS API requests"
```

The `cds_api` pool starts with one slot so historical mapping does not create
concurrent CDS downloads.

## Start Airflow

For local development:

```bash
airflow standalone
```

Open `http://localhost:8080`. Use the credentials displayed by the standalone
command or written by the configured Airflow authentication manager. Stop the
local services with `Ctrl+C`.

## Directory Structure

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

The DAG has no schedule. A user must trigger it with an explicit inclusive
period. For example:

```bash
airflow dags trigger cds_era5_land_historical_backfill \
  --conf '{"start_date":"2016-01-01","end_date":"2025-12-31"}'
```

An alternative period can be supplied without changing source code:

```bash
airflow dags trigger cds_era5_land_historical_backfill \
  --conf '{"start_date":"2014-01-01","end_date":"2024-12-31"}'
```

`2014-01-01` through `2024-12-31` spans eleven complete calendar years and
therefore produces 132 monthly partitions. Airflow processes the exact range
provided by the user and does not reinterpret it as a rolling ten-year period.

The range is split into deterministic monthly requests. Ten complete calendar
years produce 120 mapped partitions. The DAG limits itself to two active task
instances initially, while the `cds_api` pool permits only one extraction.

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

Validation is not a separate Airflow task. RAW validation remains inside
`extract_or_resume`, and transformed validation remains inside
`transform_or_resume` and checkpoint rehydration.

Tasks exchange only small request and artifact metadata through XCom. NetCDF,
Parquet, xarray, and Pandas objects remain in deterministic artifacts or task
process memory.

## Safe Retry and Resume

- A valid existing RAW NetCDF checkpoint is resumed and validated.
- A valid existing transformed Parquet checkpoint is resumed and validated.
- PostgreSQL loading remains idempotent.
- Clearing a failed task does not authorize artifact overwrite.
- `run_id` and task attempts do not affect request or artifact identity.

Before running the complete historical range, run and rerun one month, then
test three consecutive months. Check storage, row counts, provenance, duration,
and database behavior before triggering all 120 partitions.

## Tests

Core and Airflow-neutral orchestration helpers:

```bash
python -m pytest tests/unit tests/airflow_orchestration/test_tasks.py -q
```

Airflow DAG parsing and policy:

```bash
source orchestration/activate.sh
python -m pytest tests/airflow_orchestration -q
```

## Local State

`orchestration/logs/`, `orchestration/airflow.db`, PID files, and generated
authentication files are local runtime state and are ignored by Git.

Do not delete the metadata database while Airflow processes are running. A
reset is a deliberate local-development operation and is not part of the
normal retry or backfill procedure.
