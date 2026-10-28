# ETL Weather — Application Description

## Documentation Role: WHAT

This document defines **what** the application must accomplish: product scope, requirements, architectural decisions, selected technologies, and acceptance criteria. Airflow operations and coding techniques are owned by their respective companion documents.

**Status:** Living application and architecture baseline  
**Document owner:** Application requirements and architecture (**WHAT**)  
**Primary scope:** Copernicus CDS ERA5-Land hourly-to-daily ETL, canonical PostgreSQL persistence, geospatial mapping, and downstream climate-data access.

## 1. Purpose

ETL Weather must provide a reproducible climate-data foundation for analyzing weather and seasonal patterns in Aceh and for future interpretation alongside local knowledge.

The application must:

- acquire the selected Copernicus CDS ERA5-Land hourly variables for a defined spatial and temporal request;
- preserve the downloaded scientific source artifact;
- validate source structure and scientific assumptions before transformation;
- transform hourly UTC data into daily observations using UTC+7 local-day semantics;
- persist deterministic transformed artifacts and canonical PostgreSQL observations;
- support safe retry, resume, and repeat execution without unintended duplicates;
- provide geospatial mapping from climate grid coordinates to administrative regions without altering the canonical climate observations;
- expose controlled, read-only daily climate data for downstream analysis.

User-facing analytics, recommendations, cultural interpretation, or predictive products may be built on top of this data foundation, but they must not change the scientific meaning of the canonical ERA5-Land observations.

## 2. Authoritative scientific source

The active upstream dataset is **Copernicus CDS ERA5-Land hourly data from 1950 to present** (`reanalysis-era5-land`).

Repository source references:

- [ERA5-Land hourly data from 1950 to present](<ERA5-Land hourly data from 1950 to present/00 ERA5-Land hourly data from 1950 to present.html>)
- [ERA5-Land data documentation](<ERA5-Land hourly data from 1950 to present/01 ERA5-Land_ data documentation.html>)
- [ERA5-Land post-processed daily statistics](<ERA5-Land hourly data from 1950 to present/00 ERA5-Land post-processed daily statistics from 1950 to present.html>)

ERA5-Land is a land-surface reanalysis driven by ERA5 atmospheric forcing. The CDS distribution is gridded on a regular latitude-longitude grid at 0.1° × 0.1°, while the native ERA5-Land model resolution is approximately 9 km. The hourly CDS product is the scientific input to this application.

The application must not silently substitute the ERA5 single-level dataset, ERA5 post-processed products, ARCO, or another provider for the active ERA5-Land source.

## 3. Active variables

The required provider variables are:

| Provider code | CDS variable | Application use | Source unit |
| --- | --- | --- | --- |
| `tp` | Total precipitation | Daily accumulation | m |
| `t2m` | 2 metre temperature | Daily mean | K |
| `d2m` | 2 metre dewpoint temperature | Daily mean | K |
| `swvl1` | Volumetric soil water layer 1 | Daily mean | m³/m³ |
| `ssrd` | Surface solar radiation downwards | Daily accumulation | J/m² |

These provider codes are canonical application variable names and must remain explicit in requests, artifacts, tests, logs, and transformation contracts.

## 4. Spatial scope

The production Aceh request bounding box is:

```text
north = 6.1
west  = 94.9
south = 1.9
east  = 98.3
```

At the CDS 0.1° distribution grid, this extent contains:

```text
latitude points  = 43
longitude points = 35
grid points      = 1,505
```

The deterministic artifact identifier for this extent is:

```text
bbox_n6p1_w94p9_s1p9_e98p3
```

Each latitude/longitude pair represents a gridded model location. Administrative-region mapping is an application-side geospatial relation and must not rewrite or replace the original grid coordinates.

## 5. Temporal and daily-statistic requirements

ERA5-Land source timestamps are UTC. The application produces daily observations for:

```text
Asia/Jakarta = UTC+7
```

The canonical transformed data must retain both:

```text
valid_time
valid_time_local
```

Neither timestamp may be replaced with a generic `date` field.

A UTC+7 local calendar day crosses two UTC dates. Therefore the transformation must retrieve and validate enough source context to construct the complete local day.

### 5.1 Instantaneous / mean variables

Daily means apply to:

```text
t2m
d2m
swvl1
```

The daily result is the mean of the valid hourly observations belonging to the same UTC+7 local day at the same grid point. Grid coordinates must never be mixed during aggregation.

### 5.2 Accumulated variables

Accumulated variables are:

```text
tp
ssrd
```

ERA5-Land accumulation semantics differ from ERA5. ERA5-Land forecast accumulations start at 00 UTC; the CDS value at 00 UTC represents the accumulation over the previous 24 hours.

For local day `D` in UTC+7, the application contract is:

```text
Daily(D) = A(D 17 UTC) + A(D 00 UTC) - A(D-1 17 UTC)
```

This contract prevents double-counting while reconstructing the accumulation bounded by local midnight.

### 5.3 Missing values

Missing scientific observations are not zero. Missingness must be preserved through transformation and PostgreSQL persistence unless a future explicit product requirement defines an imputation method outside the canonical observations.

## 6. Output units

The canonical daily output units are:

```text
tp     = mm/day
t2m    = degC
d2m    = degC
swvl1  = m3/m3
ssrd   = J/m2/day
```

Required unit conversions include Kelvin to degrees Celsius for temperature variables and metres of water equivalent to millimetres for total precipitation.

## 7. ETL architecture

The application architecture is:

```text
Copernicus CDS ERA5-Land
          |
          v
     Hourly RAW data
        NetCDF
          |
          v
Validation + UTC+7 daily transformation
          |
          v
  Daily transformed data
        Parquet
          |
          v
Canonical PostgreSQL observations
          |
          +--> controlled daily-climate reads
          |
          +--> PostGIS grid-to-region mapping
          |
          +--> read-only operational audit
```

Apache Airflow coordinates execution of the existing ETL but does not own scientific transformation rules or canonical persistence semantics.

The reusable pipeline must remain executable without importing Airflow.

## 8. Artifact requirements

Canonical artifact formats are:

```text
RAW         = NetCDF
TRANSFORMED = Parquet
```

Artifacts must use deterministic paths based on dataset identity, bounding box, logical request partition, variables, and year.

Expected layout:

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

RAW NetCDF is immutable. Existing RAW or transformed checkpoints may be reused only after validation. The application must not silently overwrite an existing valid or conflicting scientific artifact.

## 9. Canonical transformed schema

The daily transformed Parquet schema must contain:

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

Source bookkeeping fields such as `number` and `expver` are not weather observation columns unless a later explicit requirement adds them.

## 10. Request identity and idempotency

A logical request partition must have a deterministic `request_key` derived from the provider, dataset, date range, variable set, and bounding box.

Idempotency is **per logical request partition**. It is not a global deduplication rule across unrelated bounding boxes or independently configured requests.

Repeating the same request must:

- validate and reuse matching checkpoints when possible;
- avoid duplicate canonical observations;
- preserve transaction safety;
- fall back to the normal UPSERT path when persisted metadata no longer matches the transformed artifact.

## 11. PostgreSQL requirements

PostgreSQL is the canonical store for daily climate observations.

Canonical climate tables are:

```text
climate_data.t_daily_climate
climate_data.t_daily_climate_detail
```

The canonical detail foreign key is:

```text
daily_climate_id
```

Observation identity for persistence is:

```text
daily_climate_id + valid_time + latitude + longitude
```

Daily-statistic columns are:

```text
tp     -> tp_daily_total_mm
t2m    -> t2m_daily_mean_c
d2m    -> d2m_daily_mean_c
swvl1  -> swvl1_daily_mean_m3_m3
ssrd   -> ssrd_daily_total_j_m2
```

Database loading must be transactional. A failure while persisting details must not leave partially committed metadata or observations.

An unchanged load may skip writes only when the existing `request_key`, transformed artifact path/checksum, and observation count match the current transformed partition.

## 12. Provenance requirements

The application must preserve enough provenance to trace canonical daily observations back to their generated artifacts without introducing unnecessary algorithm/Git version fields.

Required provenance:

- RAW NetCDF records `downloaded_at` as file metadata.
- Parquet metadata records `raw_artifact_path`, `raw_sha256`, `downloaded_at`, and `transformed_at`.
- PostgreSQL partition metadata records `downloaded_at`, `transformed_artifact_path`, `transformed_sha256`, and `transformed_at`.
- RAW path/checksum are artifact-level provenance and are not duplicated into each PostgreSQL observation row.
- A resumed transformed checkpoint must reject a changed RAW checksum.

## 13. Controlled daily-climate access

Downstream analysis must use the canonical PostgreSQL daily observations rather than creating duplicate physical modelling datasets by default.

The controlled read interface must select explicitly by:

```text
source
dataset_name
start_date
end_date
bbox
```

Returned daily climate data must preserve:

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
```

The read path must be read-only, deterministic, bounded to the requested local-day period, and reject ambiguous duplicate local-day/grid observations.

Wet-day labels, scaling, rolling features, anomalies, clustering features, and other modelling semantics belong to downstream analytical stages, not the canonical daily climate dataset.

## 14. Geospatial requirements

PostGIS is used for administrative-region mapping.

The geospatial subsystem must:

- store immutable source boundary files separately from generated database objects;
- validate source geometry before import;
- import supported administrative boundaries idempotently;
- map persisted ERA5-Land grid coordinates to administrative regions without modifying climate observations;
- remain executable independently from Airflow.

For Aceh district boundaries, the expected source location is:

```text
geospatial/sources/id/districts/Aceh.geojson
```

## 15. Airflow architecture boundary

Apache Airflow is the orchestration layer for scheduled and manual operation.

Airflow may own:

- schedules and data intervals;
- task dependencies and mapped execution;
- retry and deferral policy;
- pacing and concurrency;
- progress/recovery coordination;
- operational observability and Airflow-specific configuration.

Airflow must not redefine scientific daily-statistic formulas, artifact semantics, canonical schema, or database identity rules.

Operator-facing Airflow behavior belongs in `orchestration/README.md`; agent implementation instructions belong in `AGENTS.md`.

## 16. Selected technology baseline

| Layer | Selected technology |
| --- | --- |
| Language | Python 3.12 |
| Scientific source | Copernicus CDS ERA5-Land |
| RAW artifact | NetCDF |
| Transformed artifact | Parquet |
| Structured persistence | PostgreSQL 16 |
| Spatial extension | PostGIS 3 |
| Orchestration | Apache Airflow |
| DataFrame access | Pandas |
| Testing | pytest |
| CI | GitHub Actions |

Additional providers, distributed-processing frameworks, alternate databases, or duplicate modelling stores require an explicit architecture decision and demonstrated need.

## 17. Campaign scope

The defined historical campaign covers:

```text
2016-01-01 .. 2025-12-31
```

The campaign contains 40 calendar quarters. Quarterly grouping is a campaign/progress policy; scientific ETL partitions remain monthly request partitions with the context required to compute complete UTC+7 local days.

## 18. Experimental monthly aggregation

Monthly climate aggregation is separate from the canonical daily ETL and remains experimental until its scientific contract and isolated persistence tests are explicitly accepted.

It must not silently become part of the daily Airflow workflow or canonical daily-climate persistence contract.

## 19. Non-functional requirements

- **Scientific correctness:** source semantics, units, grid identity, and local-day behavior must be explicit and testable.
- **Reproducibility:** deterministic request identity and artifact paths must permit repeatable processing.
- **Recoverability:** extraction, transformation, and loading must support safe resume/retry behavior.
- **Data integrity:** canonical persistence must be transactional and idempotent per logical partition.
- **Traceability:** generated daily observations must be traceable to transformed and RAW artifacts through defined provenance.
- **Separation of concerns:** Airflow coordinates; reusable domain code performs scientific ETL and data access.
- **Operational safety:** destructive development utilities require explicit safeguards and must not affect production unintentionally.
- **Security:** credentials and secrets must remain outside source control.
- **Extensibility:** future analytics may build on canonical daily observations without mutating their scientific meaning.

## 20. Application acceptance criteria

The application satisfies its current WHAT contract when:

- a valid ERA5-Land request for the configured extent and five active variables can be deterministically constructed;
- hourly source data can be downloaded to immutable NetCDF checkpoints and validated;
- UTC+7 daily means and accumulations are calculated per grid point with correct cross-UTC-day handling;
- `tp`, `t2m`, `d2m`, `swvl1`, and `ssrd` are emitted in the defined daily units;
- canonical transformed Parquet contains the required schema and provenance;
- repeating an unchanged logical request does not create duplicate canonical PostgreSQL observations;
- mismatched artifacts or metadata are detected instead of silently accepted;
- PostgreSQL load failure is transactionally rolled back;
- controlled daily-climate reads are deterministic, read-only, period-bounded, and preserve missing values and both timestamps;
- geospatial mapping can associate persisted grid coordinates with supported administrative regions without changing the climate observations;
- the core pipeline and geospatial modules remain independently testable without Airflow;
- Airflow can coordinate the pipeline without duplicating ETL/scientific logic;
- routine unit and integration suites verify the relevant contracts.

## 21. Out of scope unless explicitly approved

The current application contract does not include:

- ARCO as an active runtime provider;
- silent substitution of ERA5 for ERA5-Land;
- global deduplication across unrelated request partitions;
- automatic imputation of missing scientific observations;
- embedding wet-day, clustering, recommendation, or cultural-interpretation labels into canonical daily climate observations;
- treating the experimental monthly aggregation path as production;
- moving scientific processing into Airflow DAG definitions;
- adding distributed infrastructure without measured need.

## Documentation ownership

- **This document (WHAT):** application requirements, scientific/data contracts, architecture decisions, selected technologies, and acceptance criteria.
- **[README.md](../README.md):** current repository overview, setup, commands, and operator-facing behavior.
- **[pipeline/README.md](../pipeline/README.md):** current pipeline-domain implementation and usage.
- **[orchestration/README.md](../orchestration/README.md):** Airflow operations, scheduling, pacing, retries, progress, and recovery.
- **[geospatial/README.md](../geospatial/README.md):** geospatial import/mapping behavior.
- **[tests/AGENTS.md](../tests/AGENTS.md):** test-specific agent instructions.
- **[AGENTS.md](../AGENTS.md):** repository-wide agent workflow and change discipline.

When documents touch the same topic, update the document that owns that topic rather than copying the same detailed rules into multiple places.
