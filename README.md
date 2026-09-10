# ETL Weather

Weather ETL project for Airflow-based processing of ERA5 data from two sources:

- ARCO ERA5
- Copernicus Climate Data Store

Current working source: ARCO.

Project guidance:

- [AGENTS.md](./AGENTS.md)
- [docs/AIRFLOW_BEST_PRACTICE.md](./docs/AIRFLOW_BEST_PRACTICE.md)
- [docs/AIRFLOW_CODING_GUIDELINES.md](./docs/AIRFLOW_CODING_GUIDELINES.md)

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

The script creates a virtual environment, activates it, and installs the project dependencies from `requirements.txt`.

Configure credentials in `.env`:

- For CDS runs, set `CDSAPI_URL` and `CDSAPI_KEY`

## Running the Pipeline

- ARCO entrypoint: `python3 scripts/run_pipeline_arco.py`
- CDS entrypoint: `python3 scripts/run_pipeline_cds.py`

## Data Layout

- Raw data is stored under `/data/era5-single-levels/raw/arco/[year]/` or `/data/era5-single-levels/raw/cds/[year]/`
- Transformed data is stored under `/data/era5-single-levels/transformed/arco/[year]/` or `/data/era5-single-levels/transformed/cds/[year]/`
- Weekly file names include the date range and variables, for example `19900101_19900107_ssrd_tp.nc` and `19900101_19900107_ssrd_tp.parquet`

## Conventions

- Use English for code, comments, and identifiers.
- Keep ARCO and CDS outputs separate.
- Treat raw data as immutable.
- Put Airflow orchestration in `dags/`.
- Put ETL logic in `pipeline/`.
- Use `scripts/` only for thin entrypoints and wrappers.
