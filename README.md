# ETL Weather

ETL Weather transforms Copernicus CDS ERA5-Land hourly data into UTC+7 daily climate observations, then derives complete local-calendar monthly statistics from validated daily Parquet. It preserves reproducible NetCDF/Parquet artifacts and stores daily and monthly observations separately in PostgreSQL.

The ETL pipeline can run directly from the command line or be coordinated by Apache Airflow. Airflow remains an orchestration layer; the scientific processing and persistence behavior stay in `pipeline/`.

## Prerequisites

- Python 3.12
- PostgreSQL 16 for canonical climate observations; **PostGIS 3** extension is additionally required for administrative boundaries and spatial grid-to-region mapping (not required for the standalone climate ETL alone).
- CDS API credentials
- A single host, or a shared `data/` filesystem visible to every task worker

## PostgreSQL and PostGIS

PostGIS is an **extension installed into PostgreSQL**, not a separate database product. The existing climate ETL uses ordinary PostgreSQL tables and can run without the extension; the independent `geospatial/` component requires PostGIS for `geometry(MultiPolygonZ, 4326)`, spatial indexes, and point-in-polygon operations. The Airflow metadata database remains separate and is not involved in geospatial setup.

On Ubuntu with a PostgreSQL 16 server, install the matching server-side package if needed:

```bash
sudo apt install postgresql-16-postgis-3 postgresql-16-postgis-3-scripts
```

Check package availability from a connection to the intended PostgreSQL server:

```sql
SELECT name, default_version, installed_version
FROM pg_available_extensions
WHERE name = 'postgis';
```

An available version with `installed_version IS NULL` means PostGIS is installed on the **server** but is not enabled in that **database**. Enable it only in the chosen database, with appropriate privileges and after confirming the target:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

For this project, activate PostGIS on the existing application database `weather` after confirming the intended connection and taking an appropriate backup. Preserve existing climate observations and create new spatial tables under the `climate_data` schema. The extension must eventually be enabled in the application database used to store geographic geometries; merely installing the package does not enable it in every database. Spatial bootstrap can be run manually (this **changes the selected database** by enabling the extension if absent):

```bash
source activate-airflow.sh -q
python -m geospatial.database.bootstrap
```

This command connects through the existing `POSTGRES_*` configuration, verifies the connected database name, enables PostGIS only if missing, and does **not** create or reset the database, touch climate data, or start Airflow. It requires sufficient database privileges on first activation. Future GeoJSON importers must call the same `ensure_postgis_extension` function **automatically before loading features**, so users need not run the CLI first. The importer has not yet been implemented (P4.3/P4.4).

For read-only readiness checks and 3D geometry examples, see [`geospatial/sql/01_postgis_preflight.sql`](./geospatial/sql/01_postgis_preflight.sql). Source GeoJSON is immutable and `geospatial/` operates independently of Airflow. Administrative polygon columns are named `boundary_geometry` (PostGIS type `geometry(MultiPolygonZ,4326)`). For existing databases populated before this rename, run [`geospatial/sql/03_rename_boundary_geometry.sql`](./geospatial/sql/03_rename_boundary_geometry.sql) once on `weather` before using the updated importer.

## Active Dataset

```text
source             : Copernicus Climate Data Store (CDS)
dataset            : reanalysis-era5-land
input              : hourly NetCDF
transformed output : UTC+7 daily Parquet
database output    : PostgreSQL daily climate observations
```

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

STANDALONE_PIPELINE_START_DATE=2024-01-01
STANDALONE_PIPELINE_END_DATE=2024-01-31

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
```

`APP_ENV` selects the application environment. Orchestration timing intervals are source-controlled in root `config.py`; only the scheduled-mode enable switch remains environment-controlled. `STANDALONE_PIPELINE_START_DATE` and `STANDALONE_PIPELINE_END_DATE` provide the default inclusive period for standalone pipeline execution. `POSTGRES_*` configures the canonical application database, while `TEST_POSTGRES_*` identifies the isolated PostgreSQL database used by integration tests.

The root `config.py` is the single lightweight configuration module shared by pipeline, scripts, and orchestration. There is no separate `orchestration/config.py`. The campaign and scheduling variables configure the optional Airflow orchestration layer. Their scheduling, pacing, maintenance, reset, and clean-slate destroy semantics are documented in [`orchestration/README.md`](./orchestration/README.md). Airflow authentication setup is also documented there rather than in the root README.

Never commit real CDS or PostgreSQL credentials.



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

Daily means for `t2m`, `d2m`, and `swvl1` require the complete hourly samples belonging to the local day. Missing observations remain missing rather than being converted to zero. Each transformed row records the local day as a half-open period `[start, end)` using four explicit boundaries: `valid_time_start`, `valid_time_end`, `valid_time_local_start`, and `valid_time_local_end`. The UTC and UTC+7 boundary pairs denote the same instants.

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
├── geospatial/               # independent administrative boundaries and spatial grid mapping
│   ├── sources/id/districts/ # immutable Aceh GeoJSON source
│   ├── database/             # PostgreSQL/PostGIS bootstrap
│   └── sql/                  # read-only spatial capability checks
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
├── tests/                    # unit/integration tests grouped by pipeline, orchestration, and operations
├── env-example               # example environment configuration
├── requirements.txt          # single project dependency manifest
└── README.md
```

The root README documents the project as a whole. Airflow-specific runtime files, authentication setup, scheduling behavior, and operational recovery are documented in [`orchestration/README.md`](./orchestration/README.md).

## Running the Pipeline

Run the daily and monthly stages together:

```bash
source activate-airflow.sh -q
STANDALONE_PIPELINE_START_DATE=2024-01-01 \\
STANDALONE_PIPELINE_END_DATE=2024-01-31 \\
python -m scripts.run_pipeline
```

**Clean-slate development run (destructive):** If you need to discard all existing generated data and restart from zero, first inspect the targets:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m scripts.destroy_all
```

After confirming the displayed PostgreSQL database, data directory, and Airflow runtime/log targets are correct, run:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m scripts.destroy_all --execute
```

Type `DESTROY` when prompted. This command is restricted to `APP_ENV=dev`; it stops recognized Airflow processes, recreates the configured `POSTGRES_*` database, and clears generated `data/`, Airflow logs, and Airflow runtime state. **It is broader than a weather-data-only reset.** It preserves source files, `.env`, and tracked configuration. Review the targets carefully; deleted data cannot be recovered through the command.

The combined runner uses the configured CDS period, variables and bounding box. Choose a **complete calendar month** (for example January 1–31): incomplete months are rejected before downloading data. First run the command with a test PostgreSQL database and an intentionally limited period; the existing daily and monthly Parquet artifacts are validated and reused on later runs.

### Checkpoint stages

A **checkpoint** is a saved, validated artifact at a processing stage that can be reused after interruption or retry. It is not an additional download or a separate file format.

- **RAW checkpoint:** hourly ERA5-Land data stored as NetCDF in `raw/YYYY/`.
- **Daily transformed checkpoint:** UTC+7 daily observations stored as Parquet in `transformed/YYYY/daily/`.
- **Monthly transformed checkpoint:** complete UTC+7 monthly observations stored as Parquet in `transformed/YYYY/monthly/`.

The combined `pipeline/runtime.py` runs `pipeline/daily.py` and then `pipeline/monthly.py` for every complete calendar month. The combined terminal command is `python -m scripts.run_pipeline`. Use `scripts/run_monthly_climate.py` to process existing daily Parquet files separately.

### Monthly climate: processing and statistics

Monthly climate data is calculated from **daily observations in UTC+7**. The processing steps are:

| Step | What happens | Saved result |
|---|---|---|
| 1. Download | Retrieve hourly weather data from CDS. | RAW NetCDF: `raw/YYYY/*.nc` |
| 2. Daily processing | Convert hourly data into daily observations and validate the results. | Daily Parquet: `transformed/YYYY/daily/*.parquet` |
| 3. Monthly processing | Combine all daily observations for one complete calendar month, separately for each grid point. | Monthly Parquet: `transformed/YYYY/monthly/YYYY_MM_<sorted_variables>.parquet` |
| 4. Database loading | Save validated daily and monthly observations in their respective PostgreSQL tables. | PostgreSQL observations |

**What do the daily values mean?**

Each daily observation covers one local calendar day (UTC+7), separately for each grid point.

| Daily output column | Meaning | How it is calculated |
|---|---|---|
| `tp_daily_total_mm` | Total rainfall during the day (mm). | Add the hourly precipitation contributions for the local day. |
| `ssrd_daily_total_j_m2` | Total solar energy reaching the surface during the day (J/m²). | Add the hourly radiation contributions for the local day. |
| `t2m_daily_mean_c` | Average air temperature during the day (°C). | Average hourly temperatures for the local day. |
| `d2m_daily_mean_c` | Average dew-point temperature during the day (°C). | Average hourly dew-point temperatures for the local day. |
| `swvl1_daily_mean_m3_m3` | Average moisture in the top soil layer during the day (m³/m³). | Average hourly soil-moisture values for the local day. |

The daily results become the input for monthly calculations. Each daily Parquet row corresponds to one day and one grid point; it is not an average over Aceh.

**What do the monthly values mean?**

| Monthly output column | Meaning | How it is calculated |
|---|---|---|
| `tp_monthly_total_mm` | Total rainfall for the month (mm). | Add all daily rainfall totals. |
| `tp_monthly_daily_mean_mm` | Average rainfall per day (mm/day). | Monthly rainfall total ÷ number of days in the month. |
| `ssrd_monthly_total_j_m2` | Total surface solar radiation for the month (J/m²). | Add all daily radiation totals. |
| `ssrd_monthly_daily_mean_j_m2` | Average daily solar radiation (J/m²/day). | Monthly radiation total ÷ number of days in the month. |
| `t2m_monthly_mean_c` | Average air temperature (°C). | Average the daily mean temperatures. |
| `d2m_monthly_mean_c` | Average dew-point temperature (°C). | Average the daily mean dew-point temperatures. |
| `swvl1_monthly_mean_m3_m3` | Average soil moisture in the top layer (m³/m³). | Average the daily mean soil-moisture values. |


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

PostgreSQL stores canonical daily observations and separately stores UTC+7-derived monthly observations in `climate_data.monthly_climate` and `climate_data.monthly_climate_detail`. Idempotent loading does not create duplicate logical partitions.

For example, if January 2024 has already been processed and the identical January request is run again, the pipeline validates and reuses the same logical partition rather than appending duplicate daily grid observations.

Downstream modelling reads selected canonical observations into memory without changing the stored climate data or creating a second physical copy simply for feature access.

## Running Tests

Run the offline unit tests with:

```bash
python -m pytest tests/unit -q
```

Run the core isolated PostgreSQL idempotency integration test with:

```bash
python -m pytest tests/integration/pipeline/test_postgresql_idempotency.py -q
```

Integration tests use the `TEST_POSTGRES_*` settings from `env-example`. Detailed testing contracts and additional test commands are documented in [`tests/README.md`](./tests/README.md).