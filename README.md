# ETL Weather

ETL pipeline for downloading Copernicus Climate Data Store (CDS) ERA5-Land hourly data, transforming it into UTC+7 daily climate observations, storing deterministic NetCDF and Parquet artifacts, and loading the results idempotently into PostgreSQL.

The pipeline runs independently from Apache Airflow and can also be orchestrated through the explicit date-range ETL DAG in `orchestration/`. Airflow does not move scientific transformation logic out of `pipeline/`.

## Active Dataset

```text
source             : Copernicus Climate Data Store (CDS)
dataset            : reanalysis-era5-land
input              : hourly NetCDF
transformed output : UTC+7 daily Parquet
database output    : PostgreSQL daily climate observations
```

## How It Works

```text
CDS ERA5-Land hourly data
        ↓
RAW NetCDF checkpoint
        ↓
RAW validation
        ↓
UTC+7 daily transformation
        ↓
Transformed Parquet checkpoint
        ↓
Transformed validation
        ↓
Idempotent PostgreSQL load
        ↓
Read-only modelling access
```

The hourly RAW checkpoint remains the scientific source of truth. Existing checkpoints are resumed and validated rather than silently overwritten, and repeated database loads update the same logical partition instead of appending duplicates.

## Variables and Daily Outputs

| Variable | Meaning | Daily operation | Output unit |
| --- | --- | --- | --- |
| `tp` | Total precipitation | Accumulation | mm/day |
| `ssrd` | Surface solar radiation downwards | Accumulation | J/m²/day |
| `t2m` | 2 metre temperature | Mean | °C |
| `d2m` | 2 metre dewpoint temperature | Mean | °C |
| `swvl1` | Volumetric soil water layer 1 | Mean | m³/m³ |

## Local-Day Time Semantics

ERA5-Land input timestamps use UTC, while this project produces daily observations for `Asia/Jakarta` (UTC+7). The transformed data retains both `valid_time` and `valid_time_local`; neither is replaced with a generic `date` field.

For an accumulated variable and local day `D`, the implemented boundary calculation is:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

This uses the UTC boundaries corresponding to a UTC+7 local day. Daily means for `t2m`, `d2m`, and `swvl1` require the complete set of hourly samples for the local day. Missing observations remain missing rather than being converted to zero.

## Project Structure

```text
etl-weather/
├── docs/
├── orchestration/
│   ├── dags/
│   ├── docs/
│   ├── tasks/
│   ├── airflow.cfg
│   └── README.md
├── pipeline/
│   ├── contracts/
│   ├── database/
│   ├── extraction/
│   ├── loaders/
│   ├── providers/
│   ├── repositories/
│   ├── storage/
│   ├── transformation/
│   ├── validation/
│   ├── interfaces.py
│   ├── model.py
│   ├── request.py
│   └── runtime.py
├── scripts/
├── tests/
├── activate-airflow.sh
├── airflow-jwt-key.txt         # generated locally; ignored by Git
├── generate-airflow-jwt-secret.sh
├── requirements.txt
└── README.md
```

The `pipeline/` package is organized by responsibility: provider/request contracts, extraction, validation, scientific transformation, artifact storage, PostgreSQL persistence, and read-only repository access. `runtime.py` is the composition/orchestration layer and remains independent from Airflow.

## Pipeline Architecture

```text
                              +-------------------------+
                              | scripts/run_pipeline_*  |
                              | / Airflow task wrappers |
                              +------------+------------+
                                           |
                                           v
                              +-------------------------+
                              |      runtime.py         |
                              | orchestration/composition|
                              |                         |
                              | extract_or_resume()     |
                              | transform_or_resume()   |
                              | load()                  |
                              +----+---------+--------+-+
                                   |         |        |
                  -----------------+         |        +-----------------
                  |                          |                          |
                  v                          v                          v
        +-------------------+      +-------------------+      +-------------------+
        | WeatherExtractor  |      | DataTransformer   |      |    DataLoader     |
        |    interface      |      |    interface      |      |    interface      |
        +---------+---------+      +---------+---------+      +---------+---------+
                  |                          |                          |
                  v                          v                          v
        +-------------------+      +-------------------+      +-------------------+
        | extraction/cds.py |      | transformation/   |      | loaders/postgres.py|
        |                   |      | era5_land.py      |      |                   |
        | CDS retrieval     |      | UTC+7 daily ETL   |      | transactional      |
        | context handling  |      | scientific rules  |      | idempotent UPSERT  |
        +---------+---------+      +---------+---------+      +---------+---------+
                  |                          |                          |
                  |                          |                          v
                  |                          |                +----------------------+
                  |                          |                | PostgreSQL           |
                  |                          |                | climate_data.*       |
                  |                          |                +----------+-----------+
                  |                          |                           |
                  |                          |                           v
                  |                          |                +----------------------+
                  |                          |                | repositories/        |
                  |                          |                | daily_climate.py     |
                  |                          |                | read-only modelling  |
                  |                          |                +----------------------+
                  |                          |
                  +------------+-------------+
                               |
                               v
                    +-------------------------+
                    |       DataStorage       |
                    |        interface        |
                    +------------+------------+
                                 |
                                 v
                    +-------------------------+
                    |      storage/local.py   |
                    | deterministic artifact  |
                    | checkpoint / resume     |
                    +-----------+-------------+
                                |
                   +------------+-------------+
                   |                          |
                   v                          v
          +------------------+       +---------------------+
          | RAW NetCDF       |       | Transformed Parquet |
          | immutable source |       | daily checkpoint    |
          | of truth         |       | + provenance        |
          +------------------+       +---------------------+

Shared contracts and support modules
------------------------------------
contracts/era5_land.py  -> scientific constants and ERA5-Land contracts
providers/cds.py        -> CDS provider metadata and API variable mapping
request.py              -> validation, normalization, partitioning, request identity
validation/era5_land.py -> RAW and transformed contract validation
database/               -> PostgreSQL configuration and canonical schema bootstrap
model.py                -> pipeline data models and artifact metadata
interfaces.py           -> infrastructure boundaries used by runtime.py

Key boundary:
Airflow orchestrates these same checkpoint-backed runtime stage boundaries, but
scientific transformation, storage, validation, and persistence remain inside
pipeline/.
```

## Environment Setup

Create and activate the local Python environment:

```bash
python3 -m venv myvenv
source myvenv/bin/activate
```

The project uses one dependency manifest, `requirements.txt`. Install the
pinned Airflow version first with its matching official constraints, then
install the complete project manifest without applying the Airflow constraints
to unrelated project packages:

```bash
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
pip check
```

Activate the environment in the current shell before running the pipeline or tests:

```bash
source myvenv/bin/activate
```

Copy `env-example` to `.env`, set the application environment explicitly,
then set the CDS credentials:

```env
APP_ENV=dev
CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=YOUR_UID:YOUR_API_KEY
```

`APP_ENV` is mandatory and accepts only `dev` or `prod`. Application
entrypoints log the selected mode at startup. If it is missing, blank, or
invalid, startup aborts with an instruction to set `APP_ENV=dev` or
`APP_ENV=prod`; no mode is assumed.

Generate the shared Airflow JWT secret without printing it to the terminal:

```bash
./generate-airflow-jwt-secret.sh
```

The script creates or atomically replaces the ignored `airflow-jwt-key.txt`
file and restricts its permission to the current user. `activate-airflow.sh`
then configures Airflow to read this file through
`AIRFLOW__API_AUTH__JWT_SECRET_CMD`; the secret is not stored in `.env`.

PostgreSQL and isolated integration-test settings are also documented in `env-example`.

## Running the Pipeline

Run the active CDS ERA5-Land pipeline with:

```bash
python scripts/run_pipeline_cds.py
```

The dataset, period, variables, and bounding box for this entrypoint are defined explicitly in `scripts/config.py`. Long periods are split into monthly requests by the pipeline runtime.

## Running with Apache Airflow

The manually triggered ERA5-Land ETL DAG accepts an explicit inclusive
date range and maps it into deterministic monthly partitions. Installation,
configuration, task flow, and safe rollout instructions are documented in
[`orchestration/README.md`](./orchestration/README.md).

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

The bounding-box directory prevents artifacts from different spatial requests from colliding. RAW data uses NetCDF, transformed daily data uses Parquet, and filenames deterministically include the request period and variables. RAW artifacts are immutable checkpoints.

## Persistence and Modelling Access

PostgreSQL stores one metadata row for each logical daily-climate partition and detail rows for daily grid-cell observations. Metadata and observations use deterministic UPSERT identities, so rerunning the same request partition does not create duplicates. Before writing, the loader compares the `request_key`, transformed artifact path/checksum, and observation count; an unchanged partition skips metadata/detail writes and reports zero processed rows, while any mismatch falls back to the transactional UPSERT path. Idempotency is scoped to the `request_key` (provider, dataset, period, variable set, and bounding box), not globally to grid points: overlapping bounding boxes are separate partitions and may store the same time/latitude/longitude observation under different `daily_climate_id` values.

The modelling read path selects data explicitly by source, dataset, period, and bounding box. It returns an in-memory Pandas DataFrame without mutating the canonical climate tables or creating a second physical feature dataset.

## Running Tests

Run the offline unit tests with:

```bash
python -m pytest tests/unit -q
```

Run the isolated PostgreSQL integration tests with:

```bash
python -m pytest tests/integration/test_postgresql_idempotency.py -v
```

The integration tests require the `TEST_POSTGRES_*` variables from `env-example`. Detailed testing contracts and expected behavior are documented in [tests/README.md](./tests/README.md).
