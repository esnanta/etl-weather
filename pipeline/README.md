# Pipeline

The `pipeline/` package owns the core ERA5-Land ETL behavior. It validates and normalizes weather requests, retrieves hourly source data, validates source checkpoints, transforms the data into UTC+7 daily observations, stores deterministic NetCDF/Parquet artifacts, persists canonical observations to PostgreSQL, and exposes a controlled read-only handoff for modelling.

Airflow may call these boundaries, but Airflow does not own the scientific transformation, artifact identity, or database persistence rules.

## Active Pipeline Contract

```text
source       : Copernicus Climate Data Store (CDS)
dataset      : reanalysis-era5-land
input        : hourly ERA5-Land
RAW artifact : NetCDF
daily zone   : Asia/Jakarta (UTC+7)
output       : daily Parquet + PostgreSQL observations
```

Active provider variable codes:

```text
tp
t2m
d2m
swvl1
swvl2
swvl3
ssrd
u10
v10
```

Configured Aceh bounding box:

```text
north : 6.1
west  : 94.9
south : 1.9
east  : 98.3
```

ERA5-Land is consumed on the regular 0.1° × 0.1° CDS grid. The current bounding box therefore contains 43 latitude coordinates × 35 longitude coordinates = 1,505 grid points before land/missing-value effects are considered.

## Package Structure

```text
pipeline/
├── contracts/              # scientific constants and ERA5-Land contracts
├── extraction/             # provider extraction and retrieval composition
├── interfaces/             # ETL boundary protocols
├── loaders/                # PostgreSQL persistence and idempotency
├── models/                 # typed request/dataset/artifact models
├── observability/          # pipeline logging helpers
├── providers/              # provider catalogue and CDS clients
├── repositories/           # controlled read-only data access
├── requests/               # request validation, normalization, splitting, identity
├── storage/                # deterministic paths, checkpoints, provenance
├── transformation/         # ERA5-Land hourly → UTC+7 daily transformation
├── validation/             # RAW and transformed validation
└── runtime.py              # ETL composition for CLI and Airflow callers
```

### Responsibility boundaries

| Area | Responsibility |
| --- | --- |
| `contracts/` | Dataset constants, timezone rules, aggregation groups, output units |
| `requests/` | Validate and normalize requests, serialize request specs, split by month, derive deterministic identity |
| `providers/` | Describe supported provider/dataset combinations and interact with CDS |
| `extraction/` | Retrieve provider data required for one logical request partition |
| `validation/` | Reject invalid RAW or transformed datasets before downstream use |
| `transformation/` | Convert hourly ERA5-Land observations to canonical UTC+7 daily observations |
| `storage/` | Build deterministic paths, write/read immutable checkpoints, preserve provenance |
| `loaders/` | Persist canonical partitions and observations transactionally and idempotently |
| `repositories/` | Read canonical daily observations for downstream modelling without mutation |
| `runtime.py` | Compose the ETL stages; it does not contain Airflow scheduling logic |

Airflow scheduling, retries, pools, pacing, campaign progress, and task dependencies remain in [`orchestration/README.md`](../orchestration/README.md).

## Request Lifecycle and Identity

The active request factory is `pipeline.requests.factory.build_cds_request()`. It constructs a typed `WeatherRequest` using the configured CDS dataset, variables, date range, and Aceh bounding box.

Before execution, requests are validated and normalized:

- dates must use `YYYY-MM-DD`;
- start date must not be after end date;
- the dataset name and provider variable names are normalized;
- duplicate variables are rejected;
- variables must be supported by the selected provider/dataset;
- bounding-box coordinates must be valid and ordered.

A configured period may span more than one calendar month. The runtime splits such a request into calendar-month partitions before normal ETL execution. Each partition receives a deterministic `request_key` derived from provider, dataset, period, variable set, and bounding box.

This identity is used for safe artifact reuse and PostgreSQL partition idempotency. It is intentionally partition-scoped rather than a global grid-point deduplication rule.

## ETL Flow

The core execution sequence is:

```text
WeatherRequest
     │
     ▼
validate + normalize
     │
     ▼
ensure_raw_checkpoint()
     │
     ├── existing RAW → load + validate
     └── missing RAW  → extract + save + validate
     │
     ▼
NetCDF RAW checkpoint
     │
     ▼
ensure_transformed_checkpoint()
     │
     ├── existing Parquet → load + validate
     └── missing Parquet  → transform + validate + save
     │
     ▼
Parquet daily checkpoint
     │
     ▼
load()
     │
     ├── unchanged partition → skip database write
     └── changed/new partition → metadata UPSERT + observation UPSERT
     │
     ▼
PostgreSQL canonical daily climate data
```

The three stable runtime boundaries are:

```python
ensure_raw_checkpoint()
ensure_transformed_checkpoint()
load()
```

Airflow wrappers call these boundaries and exchange only small control metadata. Core pipeline modules do not import Airflow.

## RAW Extraction and Context

### Provider cost-limit grouping

The active nine-variable **logical monthly partition** is retrieved through
two smaller CDS submissions per calendar chunk: the original five provider
variables (`tp,t2m,d2m,swvl1,ssrd`) and four additions
(`swvl2,swvl3,u10,v10`). The final RAW remains one canonical
nine-variable NetCDF for the month, not two independent science products.
Subrequests have distinct deterministic cache identities; cached successful
subrequests are reused after a later CDS failure. Source coordinates and
hourly timestamps must align exactly before variable groups are merged.
The same pacing gate applies before **each** provider POST.


ERA5-Land input is hourly. UTC+7 local days cross UTC calendar boundaries, so extraction must include the context needed by the scientific transformation instead of downloading only the visible target dates.

The pipeline preserves provider timestamps as `valid_time` and later derives `valid_time_local`; it does not replace them with a generic `date` column.

RAW validation checks, among other things:

- xarray dataset type;
- required `valid_time`, `latitude`, and `longitude` coordinates;
- requested variables;
- datetime timestamp type;
- unique, monotonically increasing hourly timestamps;
- continuous one-hour temporal spacing;
- regular source-grid spacing;
- coordinates inside the requested bounding box;
- expected latitude/longitude counts for the configured resolution.

A resumed RAW checkpoint is validated again before it is trusted.

## UTC+7 Daily Transformation

The local timezone contract is:

```text
Asia/Jakarta = UTC+7
local midnight = 17:00 UTC on the previous UTC calendar date
```

The pipeline separates variables into two scientific groups.

### Accumulated variables

```text
tp
ssrd
```

For local day `D`:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

This reconstructs the local-day accumulation from ERA5-Land forecast accumulations at the required UTC boundaries.

### Daily-mean variables

```text
t2m
d2m
swvl1
swvl2
swvl3
u10
v10
```

Daily means are formed from hourly values assigned to the UTC+7 local day. A mean is retained only when the grid cell has all 24 expected hourly samples.

### Unit conversion

| Variable | Transformation | Canonical output unit |
| --- | --- | --- |
| `tp` | metres → millimetres | mm/day |
| `t2m` | kelvin → Celsius | degC |
| `d2m` | kelvin → Celsius | degC |
| `swvl1` | unchanged | m3/m3 |
| `swvl2` | unchanged | m3/m3 |
| `swvl3` | unchanged | m3/m3 |
| `ssrd` | local-day accumulation | J/m2/day |
| `u10` | unchanged | m/s |
| `v10` | unchanged | m/s |
| `wind_speed` | daily mean of 24 paired hourly magnitudes | m/s |
| `wind_direction` | daily mean-vector meteorological from-direction; null for calm resultant | degrees [0, 360) |

Wind speed uses `mean(sqrt(u10_hourly² + v10_hourly²))`, not `sqrt(mean(u10)² + mean(v10)²)`. Direction is derived from the daily mean `u10`/`v10` vector using the meteorological from-direction convention, with a tested near-zero threshold of `1e-10 m/s`. A full 24 valid hourly pairs is required. For the scientific derivation, see [docs/app_description.md](../docs/app_description.md).

Missing values remain missing. They are never converted to zero.

## Canonical Transformed Schema

For the active variable set, transformed Parquet observations use:

```text
valid_time
valid_time_local
latitude
longitude
tp
t2m
d2m
swvl1
swvl2
swvl3
ssrd
u10
v10
wind_speed
wind_direction
```

`valid_time_local` represents local midnight for the observation day. `valid_time` is the corresponding UTC instant, which is seven hours earlier.

Transformed validation requires:

- both time columns to satisfy the UTC+7 offset contract;
- local timestamps to be exact local midnight;
- UTC timestamps to represent the UTC instant of local midnight;
- no duplicate local-day/grid observations;
- coverage of every requested local day;
- coordinates inside the request bbox and aligned to the source grid;
- stable grid membership for every local day;
- numeric-or-missing weather values;
- scientifically hard physical bounds where defined.

Validation is part of normal execution and also applies when a checkpoint is resumed.

## Deterministic Artifacts

Artifacts are generated from the logical request, not from runtime-generated filenames.

Current layout:

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

The path includes:

- provider and dataset;
- deterministic bounding-box signature;
- data layer;
- start year;
- target period;
- deterministic variable signature.

RAW is treated as immutable source truth. Existing RAW or transformed artifacts are resumed through their checkpoint readers and validated rather than silently overwritten.

## Provenance

The pipeline tracks minimal artifact provenance without transform-version or Git-commit versioning.

RAW NetCDF metadata stores:

```text
downloaded_at
```

Transformed Parquet metadata stores:

```text
raw_artifact_path
raw_sha256
downloaded_at
transformed_at
```

A resumed transformed checkpoint must still correspond to the expected RAW artifact/checksum. Checksums are SHA-256 digests of the artifact files.

Provenance is partition metadata; it is not repeated on every weather-observation row.

## PostgreSQL Persistence and Idempotency

The canonical daily climate tables are:

```text
climate_data.t_daily_climate
climate_data.t_daily_climate_detail
```

Provider variables map to database columns as follows:

| Provider | PostgreSQL |
| --- | --- |
| `tp` | `tp_daily_total_mm` |
| `t2m` | `t2m_daily_mean_c` |
| `d2m` | `d2m_daily_mean_c` |
| `swvl1` | `swvl1_daily_mean_m3_m3` |
| `ssrd` | `ssrd_daily_total_j_m2` |
| `swvl2` | `swvl2_daily_mean_m3_m3` |
| `swvl3` | `swvl3_daily_mean_m3_m3` |
| `u10` | `u10_daily_mean_m_s` |
| `v10` | `v10_daily_mean_m_s` |
| `wind_speed` (derived) | `wind_speed_daily_mean_m_s` |
| `wind_direction` (derived) | `wind_direction_daily_deg` |

Partition metadata is keyed by deterministic `request_key`.

Observation UPSERT identity is:

```text
daily_climate_id + valid_time + latitude + longitude
```

Before writing, the loader checks whether the current partition is unchanged. A database write is skipped only when all of these match:

```text
request_key
transformed artifact path
transformed SHA-256
expected observation count
```

If any item differs, the normal transactional metadata and observation UPSERT path is used.

The canonical table is created with all eleven nullable observation columns; reinitialization inspects `information_schema.columns`, adds missing compatible columns through transactional `ALTER TABLE`, and fails explicitly for incompatible existing types or structural columns. Historical five-variable observations remain valid with `NULL` in the six new fields. Primary/foreign keys, unique observation identity, and cascade behavior are unchanged.

Pandas missing values are converted to SQL `NULL`, not zero.

Database creation/bootstrap details are project/database concerns; see the root [README](../README.md) and the `database/` package.

## Read-only Modelling Handoff

`pipeline/repositories/daily_climate_reader.py` is the controlled read-only boundary from canonical PostgreSQL observations to an in-memory Pandas DataFrame.

A `DailyClimateSelection` explicitly specifies:

```text
source
dataset_name
start_date
end_date
bbox
variable_signature (optional, for selecting an exact variable generation)
```

When historical five-variable and current nine-variable partitions coexist
for the same period and bounding box, provide the active signature
(`d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10`) in
`DailyClimateSelection.variable_signature` to read only the expanded
generation. Omitting the optional filter keeps the existing behavior:
ambiguous duplicate local-day/grid observations are rejected, not silently
deduplicated or merged.

Returned columns are:

```text
daily_climate_id
valid_time
valid_time_local
latitude
longitude
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
swvl2_daily_mean_m3_m3
swvl3_daily_mean_m3_m3
u10_daily_mean_m_s
v10_daily_mean_m_s
wind_speed_daily_mean_m_s
wind_direction_daily_deg
```

The repository validates the selection and returned data, preserves missing values and both timestamps, rejects duplicate local-day/grid observations, and returns deterministic ordering.

It intentionally does not:

- create or alter database objects;
- add wet-day labels;
- scale or impute values;
- calculate anomalies or rolling features;
- create clustering features;
- create a second physical modelling dataset.

## Entry Points

### Local daily ETL

For normal execution, use the full pipeline:

```bash
python -m scripts.run_pipeline
```

For a specific stage only, use `python -m scripts.run_extract`,
`python -m scripts.run_transform`, or `python -m scripts.run_load`.
Transform requires existing validated RAW data; load requires existing validated
Parquet and matching RAW provenance. Neither stage downloads missing inputs.
Stage reruns validate and reuse artifacts and retain the PostgreSQL
per-request-partition idempotency contract. See the root README for command usage.

The configured inclusive period comes from the project environment. The runtime splits multi-month periods into calendar-month request partitions and processes each partition through the same checkpoint boundaries.

### Offline analytics export

The Airflow-independent `pipeline/analytics/` modules discover **all
available monthly checkpoints** in the selected source layer (`raw/` for
hourly, `transformed/` for daily), validate their time/grid/variable
contracts, group them by source calendar year, stream target rows to Parquet,
and verify each output before publishing a per-resolution `manifest.json`.

`python -m scripts.run_analytics --resolution hourly --variables all`
provides a read-only preview; add `--execute` to publish/revalidate annual
partitions. Use `--resolution daily` for already-transformed UTC+7 values.
**The CLI has no date-range flags**; discovered source files determine the
available years and months. Missing calendar months are not downloaded,
imputed, or assumed present. A year is complete only when all 12 months
exist. Manifest entries for nonconsecutive source months list the actual
months, not just their first/last dates.

Output identity is `analytics/<selection>/<resolution>/YYYY_<resolution>.parquet`.
Both `all` (nine provider variables) and smaller subsets are supported,
provided every discovered source month contains the selected fields. Ambiguous
or incompatible monthly sources are rejected. RAW context hours are excluded;
daily values and missingness are preserved without recomputation.

Existing matching annual output is verified and reused. If a new month is
later added to an already-published partial annual file, its coverage no
longer matches and the exporter safely refuses replacement; `--force`
and automatic extension are deliberately unsupported. See the operator
examples in [README.md](../README.md) and the scientific/data contract
in [docs/app_description.md](../docs/app_description.md).

### Airflow

Airflow calls pipeline runtime boundaries rather than reproducing ETL logic in DAG files. For DAG IDs, schedules, pools, CDS pacing, retries, campaign progress, and recovery procedures, use [`orchestration/README.md`](../orchestration/README.md).

### Experimental monthly climate aggregation

The separate `scripts/run_monthly_climate.py` workflow is experimental and is not part of the active daily pipeline or daily Airflow DAG. Its scientific and persistence acceptance is tracked separately under `docs/todo/`.

## Testing

Core pipeline tests are pytest-based and remain independent from Airflow.

Typical unit test command:

```bash
python -m pytest tests/unit -q
```

Integration tests use the isolated `TEST_POSTGRES_*` database configuration. Testing structure, isolation rules, focused suites, and integration commands are documented in [`tests/README.md`](../tests/README.md).

Changes affecting request identity, checkpoint resume, temporal aggregation, validation, provenance, persistence, or modelling handoff should retain focused regression coverage.

## Documentation Boundaries

This file documents the core pipeline only.

Use these documents for adjacent concerns:

- project setup and cross-domain overview: [`README.md`](../README.md)
- Airflow orchestration: [`orchestration/README.md`](../orchestration/README.md)
- geospatial mapping: [`geospatial/README.md`](../geospatial/README.md)
- operational SQL audit: [`query/README.md`](../query/README.md)
- testing policy: [`tests/README.md`](../tests/README.md)
- pending work: [`docs/todo/`](../docs/todo/)

## Upstream ERA5-Land References

The pipeline's scientific rules are designed for the Copernicus CDS ERA5-Land hourly product and ECMWF ERA5-Land conventions:

- ERA5-Land hourly data from 1950 to present — CDS dataset DOI: `10.24381/cds.e2161bac`
- ERA5-Land data documentation — ECMWF Copernicus Knowledge Base

These upstream sources define the provider dataset semantics; this repository defines the additional application contract for deterministic UTC+7 daily ETL, checkpointing, persistence, and downstream access.
