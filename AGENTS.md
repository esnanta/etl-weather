# AGENTS.md

This repository contains the ETL workflow for weather data in Airflow.
Use this file as the project operating guide.

Authoritative references:

- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`

If this file and the docs disagree, treat the docs as the more detailed source of truth for the relevant topic.

## Scope

- This project is for weather ETL only.
- The workflow reads from two sources: ARCO ERA5 and Copernicus Climate Data Store.
- ARCO is the current working source.
- CDS must be treated as a first-class source, not as an afterthought.
- ARCO and CDS outputs must remain separate forever, even when they cover the same time period.

## Data Storage

- Store project data under `data`.
- Do not hardcode storage paths outside `data`.
- Raw data must be stored as NetCDF files.
- Transformed data must be stored as Parquet files.
- Raw and transformed outputs use weekly file naming.
- File names must include the date range and variables, for example:
  - `19900101_19900107_ssrd_tp.nc`
  - `19900101_19900107_ssrd_tp.parquet`
- Raw storage layout:
  - `data/era5-single-levels/raw/arco/[year]/`
  - `data/era5-single-levels/raw/cds/[year]/`
- Transformed storage layout:
  - `data/era5-single-levels/transformed/arco/[year]/`
  - `data/era5-single-levels/transformed/cds/[year]/`
- Raw files are immutable.
- Transformed files must always be derived from raw files.
- Never overwrite a raw file.
- If a transformed file already exists, handle the rerun explicitly and deterministically.

## Working Rules

- Use English for comments, variable names, function names, and code identifiers.
- Use descriptive English for messages only when the situation is complex enough to need them.
- Keep messages minimal when the outcome is obvious.
- Document source-specific assumptions clearly when they affect extraction, validation, or transformation.
- Do not run large or expensive data queries before a sanity check.
- For quick sanity checks, always inspect:
  - variable existence in `ds.data_vars`
  - time range
  - row count after `to_dataframe()`
- Always verify that required variables exist before transformation.
- Keep source-specific logic explicit instead of hidden behind generic assumptions.

## Project-Specific Defaults

- Use the Aceh bounding box as the default regional reference unless a task says otherwise.
- ARCO default bbox:
  - `lat 6.0..1.0`
  - `lon 95.0..98.5`
- Copernicus CDS request area:
  - `[6.0, 94.5, 5.0, 96.0]`

## Repository Structure

Use these top-level folders for the codebase:

- `dags/`
- `pipeline/`
- `scripts/`
- `tests/`
- `docs/`

Keep responsibilities separated by folder:

- `dags/`
  - Airflow DAG definitions only
  - orchestration, dependencies, schedules, retries, sensors, and task wiring
  - no ETL transformation logic

- `pipeline/`
  - extraction, validation, transformation, loading, storage, and helpers
  - source adapters for ARCO and CDS
  - source-specific parsing and schema normalization
  - no Airflow-specific orchestration logic

- `scripts/`
  - thin entrypoints for local execution and automation
  - command-line wrappers for running pipeline workflows
  - default local WeatherRequest configuration in `scripts/config.py`
  - environment setup helpers when needed
  - no ETL transformation logic

- `tests/`
  - test code for the project
  - keep test scope aligned with the code it verifies
  - detailed test policy will be defined separately

- `docs/`
  - project documentation, including best practice and coding guideline references

## Airflow DAG Code

- Follow `docs/AIRFLOW_BEST_PRACTICE.md`.
- DAG files must stay thin and declarative.
- DAGs should describe what runs, in what order, and under which conditions.
- DAG code must not contain transformation logic.
- DAG code should only orchestrate pipeline modules and pass artifact references or parameters.
- Keep Airflow-specific concerns in DAG code:
  - scheduling
  - task dependencies
  - retries
  - sensors
  - concurrency controls
  - operational metadata

## Pipeline Modules

- Follow `docs/AIRFLOW_CODING_GUIDELINES.md`.
- Keep ETL logic inside pipeline modules.
- Make extraction, validation, transformation, and loading explicit.
- Keep source-specific adapters separate for ARCO and CDS.
- Keep transformed outputs source-specific and period-specific.
- Do not merge ARCO and CDS into one output file.
- Preserve raw data as the source-of-truth layer.
- Make units, timestamps, schemas, and assumptions explicit.
- Use clear boundaries for external I/O.
- Prefer small, focused functions that are easy to test and reason about.

## Test Code

- Keep test code separate from implementation code.
- Use tests to protect pipeline behavior and data contracts.
- Keep fixtures small and representative.
- Detailed test standards are intentionally deferred for now and will be defined in a separate pass.

## Forbidden Actions

- Do not hardcode paths outside `/data`.
- Do not overwrite raw files.
- Do not combine ARCO and CDS into a single result file.
- Do not put transformation logic directly in DAG code.
- Do not rely on implicit assumptions about variables, units, or time ranges.

## Bash Wrapper Rules

- Bash scripts may exist as thin wrappers around Python entrypoints or other approved commands.
- Bash scripts must not contain ETL transformation logic.
- Bash scripts must not become a second source of truth for pipeline behavior.
- Use bash wrappers only for:
  - setting environment variables
  - selecting runtime parameters
  - invoking a script or command
  - simple orchestration glue
- Keep wrapper scripts short, explicit, and easy to audit.

## Script Configuration Rules

- Keep default local execution parameters in `scripts/config.py`.
- Treat `build_default_request()` as the single source of truth for default local pipeline requests.
- Do not duplicate default request values in entrypoint scripts such as `scripts/run_pipeline.py`.
- When default source, dataset, date range, variables, or bbox changes, update `scripts/config.py` first and keep it aligned with project defaults in this guide.
- Keep script-level configuration typed with `pipeline.model` objects (`DataSource`, `BoundingBox`, `WeatherRequest`) instead of ad-hoc dictionaries.

## Practical Rule

When in doubt, prefer:

1. explicit source separation
2. explicit data lineage
3. explicit units and timestamps
4. explicit orchestration boundaries
5. explicit documentation of assumptions
