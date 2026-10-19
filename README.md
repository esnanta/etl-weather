# ETL Weather

ETL Weather transforms Copernicus CDS ERA5-Land hourly data into UTC+7 daily climate observations. The project preserves reproducible NetCDF/Parquet artifacts, stores canonical observations in PostgreSQL, and supports Airflow orchestration, geospatial region mapping, operational SQL audits, and automated testing.

## Active Dataset

```text
source             : Copernicus Climate Data Store (CDS)
dataset            : reanalysis-era5-land
input              : hourly NetCDF
transformed output : UTC+7 daily Parquet
database output    : PostgreSQL daily climate observations
```

The active CDS variables are:

```text
tp, t2m, swvl1, ssrd, d2m
```

Monthly climate aggregation exists as a separate experimental CLI and is not part of the active daily ETL workflow.

## Prerequisites

- Python 3.12
- PostgreSQL 16
- PostGIS 3 for geospatial tables and region mapping
- CDS API credentials

On Ubuntu with PostgreSQL 16, install the matching PostGIS server package when needed:

```bash
sudo apt install postgresql-16-postgis-3 postgresql-16-postgis-3-scripts
```

## Environment Setup

Create and activate the local virtual environment:

```bash
python3 -m venv myvenv
source myvenv/bin/activate
```

Install Airflow with its matching constraints, then install the project requirements:

```bash
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
pip check
```

Copy `env-example` to `.env` and configure the required values:

```env
APP_ENV=dev

CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=YOUR_UID:YOUR_API_KEY

PIPELINE_START_DATE=2024-01-01
PIPELINE_END_DATE=2024-01-31

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=weather
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

TEST_POSTGRES_HOST=localhost
TEST_POSTGRES_PORT=5432
TEST_POSTGRES_DATABASE=weather_test
TEST_POSTGRES_USER=your_test_username
TEST_POSTGRES_PASSWORD=your_test_password

CAMPAIGN_KEY=era5_land_2016_2025
CAMPAIGN_START_YEAR=2016
CAMPAIGN_END_YEAR=2025

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false
```
Never commit your CDS API KEY or PostgreSQL credentials.

Activate the project environment before running project commands:

```bash
source activate-airflow.sh -q
```

## Variables and Daily Outputs

| Variable | Meaning | Daily operation | Output unit |
| --- | --- | --- | --- |
| `tp` | Total precipitation | Accumulation | mm/day |
| `ssrd` | Surface solar radiation downwards | Accumulation | J/m²/day |
| `t2m` | 2 metre temperature | Mean | °C |
| `d2m` | 2 metre dewpoint temperature | Mean | °C |
| `swvl1` | Volumetric soil water layer 1 | Mean | m³/m³ |

## Local-Day Time Semantics

ERA5-Land timestamps use UTC, while this project produces daily observations for UTC+7. A local calendar day therefore crosses two UTC dates.

For an accumulated variable:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

Example for local day 2024-05-01:

```text
2024-05-01 00:00 UTC+7 = 2024-04-30 17:00 UTC
2024-05-02 00:00 UTC+7 = 2024-05-01 17:00 UTC
```

Daily means for `t2m`, `d2m`, and `swvl1` use the complete hourly samples belonging to the same local day. Missing observations remain missing rather than being converted to zero.

## Architecture Overview

```text
Copernicus CDS
      │
      ▼
   pipeline/
 acquire hourly data
 validate source
 transform UTC → UTC+7 daily
 preserve NetCDF / Parquet
 load canonical observations
      │
      ├──────────────► PostgreSQL
      │                    │
      │                    ├── geospatial/
      │                    │   boundaries + grid mapping
      │                    │
      │                    └── query/
      │                        read-only operational audit
      │
      └──────────────► orchestration/
                       Apache Airflow coordinates
                       the existing pipeline
```

Airflow coordinates pipeline execution but does not redefine the scientific transformation or persistence rules.

## Project Structure

```text
etl-weather/
├── config.py
├── database/                 # PostgreSQL configuration, DDL, bootstrap, setup
├── pipeline/                 # extraction, validation, transformation, storage, loading
│   └── README.md
├── orchestration/            # Apache Airflow orchestration
│   └── README.md
├── geospatial/               # administrative boundaries and grid-to-region mapping
│   ├── README.md
│   └── sources/id/districts/
├── query/                    # read-only PostgreSQL operational audit SQL
│   └── README.md
├── scripts/                  # standalone and maintenance entry points
├── tests/                    # unit and integration tests
│   └── README.md
├── docs/
├── env-example
├── requirements.txt
└── README.md
```

### Domain Documentation

| Domain | Purpose | Documentation |
| --- | --- | --- |
| Pipeline | ERA5-Land extraction, validation, UTC+7 transformation, artifacts, loading | [pipeline/README.md](./pipeline/README.md) |
| Database | PostgreSQL schema and initialization | summarized in this README |
| Orchestration | Airflow scheduling, pacing, retries, progress, recovery | [orchestration/README.md](./orchestration/README.md) |
| Geospatial | PostGIS, district import, grid-region mapping, regional views | [geospatial/README.md](./geospatial/README.md) |
| Query | read-only operational SQL audit | [query/README.md](./query/README.md) |
| Tests | unit/integration structure, isolation, testing policy | [tests/README.md](./tests/README.md) |

## CLI

### 1. Setup Database

Use the combined database commands rather than initializing individual table groups separately.

Prepare the configured development and test databases:

```bash
python -m database.setup --execute
```

If the PostgreSQL database already exists and only the complete application schema needs to be initialized:

```bash
python -m database.bootstrap --execute
```

The combined bootstrap provisions 7 climate/operational tables, 3 geospatial tables, and 2 regional views. PostGIS is required for the spatial objects.

### 2. Run Pipeline Manually

```bash
python -m scripts.run_pipeline
```

The manual pipeline does not take start/end dates as CLI arguments. Its request period comes from the environment configuration:

```env
PIPELINE_START_DATE=2024-01-01
PIPELINE_END_DATE=2024-01-31
```

The configured inclusive period may cover part of one month or span multiple months. The runtime splits it at calendar-month boundaries before processing. Existing valid RAW and transformed checkpoints are validated and reused when possible.

### 3. Import Geospatial Data

Administrative boundary source data is stored under:

```text
geospatial/sources/id/districts/
```

Import the district data, then map existing climate grid coordinates to administrative regions:

```bash
python -m geospatial.importers.districts --execute
python -m geospatial.mapping --execute
```

The importer does not download ERA5-Land data. Grid mapping uses coordinates already persisted by the climate pipeline. See [geospatial/README.md](./geospatial/README.md) for PostGIS, import, mapping, and regional-view details.

### 4. Testing

Run all unit tests:

```bash
python -m pytest tests/unit -q
```

Run all integration tests:

```bash
python -m pytest tests/integration -q
```

Integration tests must use the isolated `TEST_POSTGRES_*` database configuration, never the development `weather` database. See [tests/README.md](./tests/README.md) for focused suites and testing policy.


### 5. Reset Development State

Use **reset** when you want to rebuild the local development state but Airflow is already fully stopped.

Preview the reset targets first:

```bash
python -m scripts.reset_all
```

Execute after reviewing the targets:

```bash
python -m scripts.reset_all --execute
```

Reset recreates the configured development PostgreSQL database and clears generated NetCDF/Parquet data, Airflow logs, and Airflow runtime state. It is restricted to `APP_ENV=dev` and refuses to continue if the Airflow runtime is still active.

Use this when the environment is under your control and you have already stopped Airflow cleanly.

### 6. Destroy Development State

Use **destroy** when you intentionally want a clean-slate development environment and recognized Airflow processes may still be running.

Preview all targets and detected Airflow processes:

```bash
python -m scripts.destroy_all
```

Execute the clean-slate operation:

```bash
python -m scripts.destroy_all --execute
```

Destroy is also restricted to `APP_ENV=dev`, requires typing the exact confirmation `DESTROY`, stops recognized Airflow processes, recreates the configured PostgreSQL database, clears generated data/log/runtime state, and verifies the resulting clean state.

Use destroy only when you intentionally want to discard the current local development runtime and generated data. Project source code, `.env`, `orchestration/airflow.cfg`, and the Airflow JWT key are preserved.


## Data Layout

Generated artifacts are stored by dataset, bounding box, layer, and year:

```text
data/
└── cds_reanalysis-era5-land/
    └── bbox_n6p1_w94p9_s1p9_e98p3/
        ├── raw/
        │   └── YYYY/
        │       └── YYYY_MMDD_MMDD_<variables>.nc
        └── transformed/
            └── YYYY/
                └── YYYY_MMDD_MMDD_<variables>.parquet
```

RAW hourly data is stored as NetCDF. UTC+7 daily transformed data is stored as Parquet. Deterministic paths allow valid artifacts to be resumed safely.

## Persistence and Idempotency

PostgreSQL stores the canonical daily observations. Re-running the same logical request validates and reuses existing artifacts and avoids duplicate canonical observations.

The project treats idempotency per request partition rather than as a global deduplication rule across unrelated requests.

## Orchestration

Apache Airflow provides scheduled and manual orchestration around the existing ETL pipeline. The active DAG ID is:

```text
cds-era5-land
```

Airflow-specific setup, scheduling, pacing, progress, retries, recovery, and observability are documented in [orchestration/README.md](./orchestration/README.md).

## Geospatial

The geospatial component imports administrative boundaries into PostGIS and maps persisted ERA5-Land grid coordinates to administrative regions. It operates independently from Airflow and does not alter the scientific climate observations.

See [geospatial/README.md](./geospatial/README.md).

## Operational SQL Audit

The `query/` directory contains seven read-only PostgreSQL checks for connection identity, core daily/operational tables, partition integrity, data quality, campaign progress, and load verification.

See [query/README.md](./query/README.md).

## Experimental Monthly Climate Aggregation

Monthly aggregation is separate from the daily ETL and remains experimental/unverified for production database writes.

Preview one month:

```bash
python -m scripts.run_monthly_climate --month 2024-05
```

Do not use its `--execute` mode against the live application database until its scientific contract and isolated PostgreSQL integration coverage are accepted.
