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
├── dags/
├── docs/
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
├── requirements.txt
└── README.md
```

The `pipeline/` package is organized by responsibility: provider/request contracts, extraction, validation, scientific transformation, artifact storage, PostgreSQL persistence, and read-only repository access. `runtime.py` is the composition/orchestration layer and remains independent from Airflow.

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

The integration tests require the `TEST_POSTGRES_*` variables from `env-example`. Detailed testing contracts and expected behavior are documented in [tests/README_TESTING.md](./tests/README_TESTING.md).
