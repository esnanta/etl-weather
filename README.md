# ETL Weather

ETL pipeline for downloading Copernicus Climate Data Store (CDS) ERA5-Land hourly data, transforming it into UTC+7 daily climate observations, storing deterministic NetCDF and Parquet artifacts, and loading the results idempotently into PostgreSQL.

The pipeline runs independently from Apache Airflow. Airflow orchestration may be added later without moving scientific transformation logic out of `pipeline/`.

## Active Dataset

```text
source             : Copernicus Climate Data Store (CDS)
dataset            : reanalysis-era5-land
input              : hourly NetCDF
transformed output : UTC+7 daily Parquet
database output    : PostgreSQL daily climate observations
```

Project guidance:

- [AGENTS.md](./AGENTS.md)
- [docs/AIRFLOW_BEST_PRACTICE.md](./docs/AIRFLOW_BEST_PRACTICE.md)
- [docs/AIRFLOW_CODING_GUIDELINES.md](./docs/AIRFLOW_CODING_GUIDELINES.md)

Pipeline behavior and scientific contracts:

- [Readme-pipeline.md](./Readme-pipeline.md)

## Project Structure

```text
etl-weather/
├── dags/
├── docs/
├── pipeline/
├── scripts/
├── tests/
├── requirements.txt
└── README.md
```

## Environment Setup

Create and install the local Python environment with:

```bash
./scripts/setup_environment.sh
```

The script creates the `myvenv` virtual environment when needed and installs the project dependencies from `requirements.txt`.

Activate the environment in the current shell before running the pipeline or tests:

```bash
source myvenv/bin/activate
```

Copy `env-example` to `.env`, then set the CDS credentials:

```env
CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=YOUR_UID:YOUR_API_KEY
```

PostgreSQL and isolated integration-test settings are also documented in `env-example`.

## Running the Pipeline

Run the active CDS ERA5-Land pipeline with:

```bash
python scripts/run_pipeline_cds.py
```

The dataset, period, variables, and bounding box for this entrypoint are defined explicitly in `scripts/config.py`. Long periods are split into monthly requests by the pipeline runtime.

## Data Layout

Artifacts are stored by dataset, bounding box, layer, and year:

```text
data/
└── cds_reanalysis-era5-land/
    └── bbox_n5p2_w96p1_s3p4_e97p6/
        ├── raw/
        │   └── 2022/
        │       └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc
        └── transformed/
            └── 2022/
                └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

The bounding-box directory prevents artifacts from different spatial requests from colliding. RAW data uses NetCDF, transformed daily data uses Parquet, and filenames deterministically include the request period and variables. RAW artifacts are immutable checkpoints.

## Running Tests

Run the offline unit tests with:

```bash
python -m pytest tests/unit -q
```

Run the isolated PostgreSQL integration tests with:

```bash
python -m pytest tests/integration/test_postgresql_idempotency.py -v
```

The integration tests require the `TEST_POSTGRES_*` variables from `env-example`. Detailed testing contracts and expected behavior are documented in [tests/README_TESTING.md](./tests/README_TESTING.md).
