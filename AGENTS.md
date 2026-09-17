# AGENTS.md

This repository contains a weather ETL pipeline that is being built to run independently and later be orchestrated by Apache Airflow.

Use this file as the project operating guide.

Authoritative references:

- `Readme-pipeline.md`
- `docs/AIRFLOW_BEST_PRACTICE.md`
- `docs/AIRFLOW_CODING_GUIDELINES.md`
- `tests/AGENTS.md` for test-specific work

If this file and the detailed docs disagree, use the more specific document for that topic. Scientific data contracts documented in code/tests must not be silently changed.

---

## Current Project Scope

- This project is for weather ETL only.
- The repository supports two source families:
  - ARCO ERA5
  - Copernicus Climate Data Store (CDS)
- The **current active development path is CDS ERA5-Land hourly data**.
- ARCO support remains separate and must not be removed or silently harmonized into CDS behavior.
- ARCO and CDS artifacts must remain source-separated even for the same variables, region, and period.
- The current CDS ERA5-Land pipeline converts hourly source data into validated UTC+7 daily data.
- The next planned modelling stage is **P15 — Daily Feature Dataset**.

Do not prematurely implement P15/P16/P17 assumptions while working on infrastructure or reliability tasks.

---

## Airflow Boundary

The pipeline is **Airflow-ready, not Airflow-dependent**.

Core rule:

> Airflow orchestrates the ETL pipeline; Airflow does not own ETL business logic.

Keep in `pipeline/`:

- extraction;
- validation;
- transformation;
- scientific formulas;
- unit conversion;
- artifact storage;
- checkpoint/resume behavior;
- database loading and idempotency.

Keep in future `dags/`:

- schedules;
- Airflow data intervals;
- task dependencies;
- retries and retry delays;
- pools and concurrency;
- sensors;
- XCom wiring;
- Airflow Connections and secret integration;
- operational alerting.

DAG files must stay thin and declarative.

Future Airflow tasks should pass small control metadata or artifact references, not full DataFrames or NetCDF payloads.

---

## Current ERA5-Land Scientific Contract

### Source

```text
source  = cds
dataset = reanalysis-era5-land
input   = hourly
```

RAW hourly data is the scientific source-of-truth layer.

### Provider variable names

Provider codes are the primary pipeline variable names:

```text
tp
 t2m
 d2m
 swvl1
 ssrd
```

Human-readable descriptions:

```text
tp     = total precipitation
t2m    = 2 metre temperature
d2m    = 2 metre dewpoint temperature
swvl1  = volumetric soil water layer 1
ssrd   = surface solar radiation downwards
```

Long CDS API names are external API details only. Translate to/from them at the CDS boundary; do not spread them through RAW, transformed Parquet, validation contracts, filenames, or modelling code.

Do not introduce a second public naming vocabulary for the same variables.

### Time contract

Current daily timezone:

```text
Asia/Jakarta = UTC+7
```

Daily outputs retain both:

```text
valid_time        = UTC instant corresponding to local midnight
valid_time_local  = local daily boundary
```

For UTC+7:

```text
valid_time_local = valid_time + 7 hours
```

A local midnight therefore corresponds to `17:00 UTC` on the preceding UTC date.

### ERA5-Land aggregation rules

Accumulated variables:

```text
tp
ssrd
```

Daily-mean variables:

```text
t2m
d2m
swvl1
```

For accumulated ERA5-Land variables, do not sum stored hourly accumulation values directly.

Current UTC+7 local-day formula:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

where `A` is the ERA5-Land forecast-step accumulation.

The extractor may retrieve one preceding day as aggregation context. The request metadata still represents the target period.

### Output units

Current daily transformed units:

```text
tp     = mm/day
t2m    = degC
d2m    = degC
swvl1  = m3/m3
ssrd   = J/m2/day
```

Never silently change these units or reinterpret the values.

Missing values are not zero.

---

## Current Transformed Schema

For the active five-variable ERA5-Land request, transformed Parquet uses:

```text
valid_time
valid_time_local
latitude
longitude
tp
t2m
swvl1
ssrd
d2m
```

Source metadata such as `number`, `surface`, or `expver` must not become weather observation columns unless explicitly justified by a new contract.

---

## Data Storage Contract

Store all project artifacts under `data/`.

Do not hardcode operational data paths outside the project data root.

### Formats

```text
RAW         = NetCDF
TRANSFORMED = Parquet
```

Do not describe transformed output as CSV or NetCDF unless a new explicit export layer is introduced.

### Current layout

Pattern:

```text
data/<dataset>/<layer>/<source>/<year>/<period>_<variables>.<ext>
```

Example:

```text
data/reanalysis-era5-land/raw/cds/2022/2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc

data/reanalysis-era5-land/transformed/cds/2022/2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

Artifact filenames identify the logical request partition. They must not depend on wall-clock runtime, UUIDs, or Airflow run IDs.

Do not assume all artifacts are weekly. The current CDS runtime batches requests by calendar month, while filenames retain the actual request date range.

---

## P10 Checkpoint / Resume Rules

P10 is complete and is part of the required runtime contract.

### RAW

- RAW files are immutable.
- Never silently overwrite an existing RAW checkpoint.
- RAW writes must be atomic.
- If the deterministic RAW checkpoint exists, resume from it.
- A resumed RAW checkpoint must still pass validation.

### Transformed

- Transformed Parquet is deterministically derived from RAW.
- If the deterministic Parquet checkpoint exists, resume from it.
- A resumed Parquet checkpoint must still pass validation.

Current runtime stage boundaries:

```text
extract_or_resume()
transform_or_resume()
load()
```

Preserve these boundaries because they are intended to map cleanly to future Airflow tasks.

Do not make `pipeline.runtime` depend on Airflow imports.

---

## P11 Database Idempotency Rules

P11 is complete and is part of the required persistence contract.

### Dataset identity

A logical request partition is identified by deterministic `request_key` derived from:

```text
source
dataset
start_date
end_date
variable set
bbox
```

Variable ordering must not change the request identity.

For the same logical request:

```text
same request_key
→ same dataset_id
```

### Observation identity

Daily database detail rows are idempotent on:

```text
dataset_id + valid_time + latitude + longitude
```

Use UPSERT semantics. Do not return to blind append behavior.

### Current database weather columns

Provider variables map to descriptive daily columns:

```text
tp     -> tp_daily_total_mm
t2m    -> t2m_daily_mean_c
d2m    -> d2m_daily_mean_c
swvl1  -> swvl1_daily_mean_m3_m3
ssrd   -> ssrd_daily_total_j_m2
```

Use `rows_processed` in logs for UPSERT operations. Do not call the value `rows_inserted` when a retry may update existing rows.

Database migrations may support legacy columns, but legacy names must not become a second active schema contract.

---

## Validation Contract

Do not bypass validation because a file already exists.

Current important checks include:

### RAW temporal

- non-empty time coordinate;
- datetime `valid_time`;
- no duplicate timestamps;
- monotonic order;
- continuous 1-hour interval;
- no unexpected missing hourly steps.

### RAW spatial

- numeric latitude/longitude;
- no duplicate coordinates;
- monotonic axes;
- source-specific regular spacing;
- expected bbox containment/counts.

### Transformed temporal

- UTC/local offset is exactly +7 hours;
- `valid_time_local` represents local midnight;
- `valid_time` represents the matching UTC boundary;
- every requested local day is present;
- no duplicate `(valid_time_local, latitude, longitude)` rows.

### Transformed spatial

- coordinates remain within bbox;
- coordinates align to source grid resolution;
- valid land-grid membership is stable between local days.

Do not replace scientific validation with generic row-count assertions.

---

## Current Spatial Defaults

Full Aceh reference bbox:

```text
north_lat = 6.2
south_lat = 1.8
east_lon  = 98.6
west_lon  = 94.8
```

The current small CDS sanity-run configuration intentionally uses a land-dominant subset in `scripts/config.py`:

```text
north_lat = 5.2
south_lat = 3.4
east_lon  = 97.6
west_lon  = 96.1
```

Do not confuse the small development/sanity bbox with the full project study-region definition.

When defaults change, update `scripts/config.py` first and keep documentation aligned.

---

## Repository Structure

Use these top-level responsibilities:

- `pipeline/`
  - ETL implementation and scientific logic
- `scripts/`
  - thin local/automation entrypoints
- `tests/`
  - unit and integration tests
- `docs/`
  - project and scientific documentation
- future `dags/`
  - Airflow orchestration only

### `pipeline/`

Keep extraction, validation, transformation, storage, database, loading, and runtime composition here.

Source-specific behavior should remain explicit.

### `scripts/`

Keep local entrypoints thin.

Default request configuration belongs in `scripts/config.py`.

Do not duplicate default source/dataset/date/variable/bbox settings inside runner scripts.

### `tests/`

Follow `tests/AGENTS.md`.

Unit tests must be small, deterministic, offline by default, and independent from Airflow.

### `dags/`

When introduced, DAG files must contain orchestration only.

No data transformation formulas belong in DAG files.

---

## Current Verification Status

Completed and verified:

```text
[x] P4  RAW hourly retained
[x] P5  local day UTC+7
[x] P6  variable-specific aggregation
[x] P7  observation schema whitelist
[x] P8  CDS metadata excluded from observations
[x] P9  provider variable naming contract
[x] P10 checkpoint / resume
[x] P11 idempotent database load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[x] P20 hourly ERA5-Land as primary RAW input
```

Latest focused verification after P10/P11:

```text
31 passed
```

Runtime verification also confirmed:

- RAW and transformed checkpoints are created on first run;
- both are resumed on an identical second run;
- the same `dataset_id` is reused;
- duplicate database observation query returns zero rows.

Do not mark later roadmap items complete without runtime/test evidence.

---

## P15 / Modelling Guardrails

The next planned task is:

```text
P15 — Daily Feature Dataset
```

The five available meteorological variables are currently **candidate parameters**, not a locked clustering feature set.

Do not encode the assumption that all five variables must be used for clustering.

Later conceptual sequence:

```text
P15 daily modelling-ready feature dataset
→ P16 wet-day definition / identification
→ P17 feature contribution / feature-set evaluation
→ final feature set
→ scaling
→ clustering wet days
→ cluster-quality evaluation
→ interpretation
→ comparison with Keuneunong
```

Keep P15 as a modelling-data layer separate from RAW and transformed scientific truth.

---

## Working Rules

- Use English for comments, identifiers, function names, and code-level messages.
- Prefer explicit, small, testable functions.
- Keep transformations deterministic.
- Keep external I/O boundaries obvious.
- Preserve RAW data.
- Do not treat missing values as zero.
- Make units and timestamp semantics explicit.
- Do not use current wall-clock time to determine logical data partitions.
- Do not catch exceptions merely to keep the pipeline running; let deterministic failures surface so Airflow can fail the task later.
- Never log credentials or complete large datasets.
- Do not perform expensive network/database work at Airflow DAG import time.
- Do not introduce abstractions that solve no current problem.

---

## Forbidden Actions

- Do not overwrite RAW checkpoints.
- Do not combine ARCO and CDS outputs.
- Do not silently change provider variable names.
- Do not silently change units or UTC/local semantics.
- Do not bypass validation on resumed artifacts.
- Do not return database loading to append-only behavior.
- Do not make artifact identity depend on Airflow run ID or wall-clock execution time.
- Do not place scientific transformations inside future DAG code.
- Do not pass large datasets through future XCom.
- Do not hardcode credentials.
- Do not assume all five weather parameters are final modelling features.

---

## Practical Decision Order

When in doubt, prefer:

1. scientific correctness;
2. immutable RAW source truth;
3. deterministic logical partition identity;
4. explicit source separation;
5. explicit units and timestamps;
6. validation before downstream use;
7. safe retry/resume behavior;
8. idempotent persistence;
9. thin Airflow orchestration boundaries;
10. simple, testable implementation over unnecessary abstraction.
