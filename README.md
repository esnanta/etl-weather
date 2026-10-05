# ETL Weather

ETL Weather converts Copernicus Climate Data Store (CDS) ERA5-Land hourly data into daily climate observations for the project's local UTC+7 day, preserves reproducible NetCDF and Parquet artifacts, and stores canonical daily observations in PostgreSQL.

The ETL pipeline can run directly from the command line or be coordinated by Apache Airflow. Airflow remains an orchestration layer; the scientific processing and persistence behavior stay in `pipeline/`.

## Prerequisites

- Python 3.12
- PostgreSQL for canonical daily climate observations
- CDS API credentials
- A single host, or a shared `data/` filesystem visible to every task worker

## Active Dataset

```text
source             : Copernicus Climate Data Store (CDS)
dataset            : reanalysis-era5-land
input              : hourly NetCDF
transformed output : UTC+7 daily Parquet
database output    : PostgreSQL daily climate observations
```

## How It Works

The program starts with hourly ERA5-Land climate data from Copernicus CDS. It checks that the requested source data is complete and suitable for processing, then converts the hourly observations into daily values that follow the project's UTC+7 local day rather than a UTC calendar day.

The original hourly data is retained as a reproducible RAW artifact, while the resulting daily observations are stored as Parquet. The validated daily observations are then persisted in PostgreSQL as the canonical dataset used by downstream analysis and modelling.

If the same logical request is run again, the program reuses and validates the existing artifacts where possible and avoids creating duplicate canonical observations. This allows interrupted or repeated processing to continue safely without treating every rerun as new climate data.

## Variables and Daily Outputs

| Variable | Meaning | Daily operation | Output unit |
| --- | --- | --- | --- |
| `tp` | Total precipitation | Accumulation | mm/day |
| `ssrd` | Surface solar radiation downwards | Accumulation | J/m²/day |
| `t2m` | 2 metre temperature | Mean | °C |
| `d2m` | 2 metre dewpoint temperature | Mean | °C |
| `swvl1` | Volumetric soil water layer 1 | Mean | m³/m³ |

## Local-Day Time Semantics

ERA5-Land input timestamps use UTC, while this project produces daily observations for `Asia/Jakarta` (UTC+7). A local calendar day therefore crosses two UTC dates.

Let:

- `D` be the local calendar date being produced; and
- `A(t)` be the accumulated ERA5-Land value at UTC time `t`.

For an accumulated variable, the implemented boundary calculation is:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

For example, local day **2024-05-01** runs from:

```text
2024-05-01 00:00 UTC+7 = 2024-04-30 17:00 UTC
2024-05-02 00:00 UTC+7 = 2024-05-01 17:00 UTC
```

For total precipitation, suppose the accumulated values after unit conversion are:

```text
A(2024-04-30 17:00 UTC) = 8 mm
A(2024-05-01 00:00 UTC) = 12 mm
A(2024-05-01 17:00 UTC) = 7 mm
```

Then the precipitation assigned to local day 2024-05-01 is:

```text
7 + 12 - 8 = 11 mm/day
```

The values above are only an arithmetic example of the implemented boundary calculation.

Daily means for `t2m`, `d2m`, and `swvl1` require the complete hourly samples belonging to the local day. Missing observations remain missing rather than being converted to zero. The transformed data retains both `valid_time` and `valid_time_local`.

## Pipeline Architecture

At a high level, the project follows this data flow:

```text
Copernicus CDS
      ↓
Acquire hourly climate data
      ↓
Validate source data
      ↓
Build UTC+7 daily observations
      ↓
Preserve reproducible artifacts
      ↓
Store canonical observations
      ↓
Analysis / modelling
```

The important boundary is conceptual: `pipeline/` owns climate-data acquisition, validation, transformation, artifact handling, and persistence. Airflow may coordinate those operations, but it does not redefine their scientific behavior.

## Project Structure

```text
etl-weather/
├── config.py                 # shared lightweight application configuration
├── docs/                     # design decisions and project documentation
├── orchestration/            # Airflow orchestration layer
│   └── README.md             # Airflow setup and operations
├── pipeline/                 # ETL and canonical data-access implementation
│   ├── database/
│   ├── extraction/
│   ├── loaders/
│   ├── repositories/
│   ├── storage/
│   ├── transformation/
│   └── validation/
├── scripts/                  # standalone and operational entry points
├── tests/                    # unit, integration, and orchestration tests
├── env-example               # example environment configuration
├── requirements.txt          # single project dependency manifest
└── README.md
```

The root README documents the project as a whole. Airflow-specific runtime files, authentication setup, scheduling behavior, and operational recovery are documented in [`orchestration/README.md`](./orchestration/README.md).

## Environment Setup

Create and activate the local Python environment:

```bash
python3 -m venv myvenv
source myvenv/bin/activate
```

The project uses one dependency manifest, `requirements.txt`. Install the pinned Airflow version first with its matching official constraints, then install the complete project manifest:

```bash
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
pip check
```

Copy `env-example` to `.env` and replace the placeholder values with the local configuration:

```env
APP_ENV=dev

CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=YOUR_UID:YOUR_API_KEY

CDS_START_DATE=2022-01-01
CDS_END_DATE=2022-01-03

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database_name
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

TEST_POSTGRES_HOST=localhost
TEST_POSTGRES_PORT=5432
TEST_POSTGRES_DATABASE=weather_test
TEST_POSTGRES_USER=your_test_username
TEST_POSTGRES_PASSWORD=your_test_password

CAMPAIGN_KEY=era5_land_2016_2025
CAMPAIGN_START_YEAR=2016
CAMPAIGN_END_YEAR=2016

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false
ERA5_CAMPAIGN_SCHEDULE_CADENCE=2h
SUBMISSION_INTERVAL_SECONDS=1800
```

`APP_ENV` selects the application environment. `CDS_START_DATE` and `CDS_END_DATE` provide the default inclusive period for standalone pipeline execution. `POSTGRES_*` configures the canonical application database, while `TEST_POSTGRES_*` identifies the isolated PostgreSQL database used by integration tests.

The campaign and scheduling variables configure the optional Airflow orchestration layer. Their scheduling, pacing, and recovery semantics are documented in [`orchestration/README.md`](./orchestration/README.md). Airflow authentication setup is also documented there rather than in the root README.

Never commit real CDS or PostgreSQL credentials.

## Running the Pipeline

Run the standalone ERA5-Land pipeline with:

```bash
python scripts/run_pipeline_cds.py
```

The active dataset, variables, and Aceh bounding box are defined by project configuration. The default standalone period comes from `CDS_START_DATE` and `CDS_END_DATE` in `.env`. Longer periods are processed as monthly request partitions while preserving the same ETL behavior.

For Airflow scheduling, manual recovery, and campaign operation, see [`orchestration/README.md`](./orchestration/README.md).

## Data Layout

Artifacts are stored by dataset, bounding box, layer, and year:

```text
data/
└── cds_reanalysis-era5-land/
    └── bbox_n6p1_w94p9_s1p9_e98p3/
        ├── raw/
        │   └── 2022/
        │       └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc
        └── transformed/
            └── 2022/
                └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

The dataset and bounding-box directories keep different requests separate. RAW hourly data is stored as NetCDF, transformed daily data is stored as Parquet, and deterministic filenames make the request period and variable set identifiable.

## Persistence and Modelling Access

PostgreSQL stores the canonical daily climate observations. Reprocessing the same logical request does not create another copy of the same partition.

For example, if January 2024 has already been processed and the identical January request is run again, the pipeline validates and reuses the same logical partition rather than appending duplicate daily grid observations.

Downstream modelling reads selected canonical observations into memory without changing the stored climate data or creating a second physical copy simply for feature access.

## Running Tests

Run the offline unit tests with:

```bash
python -m pytest tests/unit -q
```

Run the core isolated PostgreSQL idempotency integration test with:

```bash
python -m pytest tests/integration/test_postgresql_idempotency.py -q
```

Integration tests use the `TEST_POSTGRES_*` settings from `env-example`. Detailed testing contracts and additional test commands are documented in [`tests/README.md`](./tests/README.md).
