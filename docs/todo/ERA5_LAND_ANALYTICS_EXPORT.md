# ERA5-Land Analytics Export — Implementation Checklist

**Document role:** Plan a standalone, read-only-source CLI that publishes two reproducible Parquet exports for a separate notebook-based analytics project. Product/scientific requirements remain owned by [app_description.md](../app_description.md); existing ETL behavior and operator commands are documented in [README.md](../../README.md) and [pipeline/README.md](../../pipeline/README.md). This checklist is not evidence that the feature has been implemented.

**Branch:** `dev`  
**Created:** 2026-11-12  
**Status:** Planned; implementation not started  
**Scope:** Export only. EDA, data quality statistics, feature engineering, clustering, and model evaluation belong to a separate project (Jupyter / Google Colab).

## 1. Agreed outcome and boundaries

`etl-weather` acquires, validates, transforms, stores, and exposes meteorological datasets for analysis. The new analytics export is **not** an alternate ETL pipeline, an EDA engine, or a database load path.

| Export | Input | Output | Scientific treatment |
| --- | --- | --- | --- |
| `hourly` | Immutable `raw/YYYY/*.nc` | `era5_land_hourly_<start>_<end>.parquet` | Preserve source values, units, UTC validity times, and grid coordinates; no scientific transformation |
| `daily` | Existing `transformed/YYYY/*.parquet` | `era5_land_daily_<start>_<end>.parquet` | Preserve the existing UTC+7 daily values and timestamps; no recomputation |

For both exports:
- Select only variables present in the corresponding input files; never fetch missing variables from CDS.
- Keep missing observations missing. Do not impute, interpolate, rescale, convert units, recompute accumulations, or remove rows based on weather values.
- Do not compute or publish NULL/NaN counts, outlier summaries, descriptive statistics, wet-day labels, or model features. These belong to notebook EDA.
- Perform **structural and fidelity validation** (including missing-value correspondence) without treating that as EDA.
- Retain original RAW and transformed checkpoints unchanged, including five-variable historical files. Do not change existing extraction, daily transformation, PostgreSQL loading, or Airflow tasks.
- Do not add a database dependency, a new CDS request path, or a new CLI framework.

## 2. Output layout and identity

Use the existing dataset and bounding-box directory. Separate variable selections to prevent filename collisions:

```text
data/
└── cds_reanalysis-era5-land/
    └── bbox_n6p1_w94p9_s1p9_e98p3/
        ├── raw/YYYY/*.nc
        ├── transformed/YYYY/*.parquet
        └── analytics/
            ├── all/
            │   ├── era5_land_hourly_2016_2025.parquet
            │   ├── era5_land_hourly_2016_2025.manifest.json
            │   ├── era5_land_daily_2016_2025.parquet
            │   └── era5_land_daily_2016_2025.manifest.json
            └── t2m_tp/
                ├── era5_land_hourly_2016_2025.parquet
                ├── era5_land_hourly_2016_2025.manifest.json
                ├── era5_land_daily_2016_2025.parquet
                └── era5_land_daily_2016_2025.manifest.json
```

- Directory signatures must be deterministic, case-normalized, sorted, and order-independent: `tp,t2m` and `t2m,tp` produce the same identity.
- `all` means the **nine active source/provider variables** (`tp,t2m,d2m,swvl1,swvl2,swvl3,ssrd,u10,v10`) from current configuration, not an assumption that every historic file has all nine.
- The daily source schema additionally contains derived `wind_speed` and `wind_direction`. For the initial CLI contract, `all` selects only the nine provider variables; preserve required `valid_time`, `valid_time_local`, `latitude`, and `longitude` keys. Do not silently include or calculate derived wind columns. Document an explicit future decision if these need selectable export.
- The naming period represents the **requested target dates**, not extra source-context timestamps stored to calculate adjacent local days. Hourly exports use UTC target-hour boundaries; daily exports use UTC+7 target calendar days.
- Never merge artifacts from different bounding boxes, provider datasets, incompatible schemas, or conflicting source identities.

## 3. Proposed interface (not implemented)

Follow the repository's existing thin `scripts/` entrypoint + `pipeline/` domain logic convention:

```bash
# Preview only; no export is written
python -m scripts.run_analytics --resolution hourly --variables t2m,tp

# Produce the hourly raw-preserving export
python -m scripts.run_analytics --resolution hourly --variables t2m,tp --execute

# Produce the daily already-transformed export
python -m scripts.run_analytics --resolution daily --variables all --execute
```

Proposed arguments:
- `--resolution {hourly,daily}` (required).
- `--variables all|comma,separated,codes` (default `all`).
- `--start-year`, `--end-year` (defaults resolved from `CAMPAIGN_START_YEAR` and `CAMPAIGN_END_YEAR`, **not** inferred from `CAMPAIGN_KEY`).
- `--output` (optional, explicit output destination; must maintain paired manifest and collision safeguards).
- `--execute` (default preview-only).
- `--force` (explicit replacement intent; valid only with `--execute`, after all preflight checks).

A supported selection must be complete **across the requested time range**, not merely present in one month. A missing selected variable or month must fail with a file/period-specific error. No implicit fallback to unrelated five-/nine-variable artifacts or to downloading from CDS.

## 4. Implementation phases

### P1 — Source and CLI contract audit

- [ ] Verify `dev`, clean/known working-tree status, and repository docs: `AGENTS.md`, `docs/app_description.md`, `pipeline/README.md`, `README.md`, `tests/AGENTS.md`, and `docs/python_se_principle.md`.
- [ ] Inspect existing RAW and transformed file readers, storage paths, validation, checksums, metadata, and dependencies. Reuse production helpers instead of duplicating them.
- [ ] Confirm real NetCDF coordinate/dimension names, masks/fill values, dtypes, and variable availability; confirm transformed daily schema (including both timestamps).
- [ ] Determine whether overlapping context hours exist among stored RAW partitions; specify unambiguous period ownership and source-selection priority before implementation.
- [ ] Inspect available memory/disk headroom and realistic artifact sizes; do not assume all hourly rows fit in RAM.
- [ ] Record source coverage and schema findings; do not start an export if coverage or identity is ambiguous.
- [ ] Establish CLI parameter validation, default campaign range, output naming, preview behavior, and exit codes.

**Gate:** Scientific/source layout and selection semantics are confirmed before writing.

### P2 — Reusable selection and validation

- [ ] Add narrow analytics-export functions under `pipeline/` and a thin `scripts/run_analytics.py` entrypoint; avoid a new `weather_cli` package.
- [ ] Discover input artifacts by dataset, bbox, year, date partition, and variable signature; reject other sources/areas.
- [ ] Validate the selected variables for every included partition; report missing/ambiguous files and variables clearly.
- [ ] Validate coordinate compatibility, timestamp semantics and continuity, and unique `valid_time + latitude + longitude` (hourly) or local-day/grid identity (daily).
- [ ] Exclude out-of-period source-context hours by timestamp, not by weather-value filtering.
- [ ] For overlapping identical boundary observations, select a deterministic canonical owner and retain overlap provenance. For differing values at the same identity, **fail** rather than silently deduplicating or overwriting.
- [ ] Define how masked arrays / `_FillValue` decode to Parquet missing values, and verify that valid numerical values and missing positions are preserved.
- [ ] Ensure preview does not write outputs or change source files.

**Gate:** Selected sources and rows are deterministic, with no scientific value changes.

### P3 — Parquet export for both resolutions

- [ ] Implement `hourly` NetCDF-to-tabular export retaining original numeric values, units (in metadata), UTC `valid_time`, and original `latitude/longitude`.
- [ ] Implement `daily` concatenation of existing transformed Parquet without rerunning UTC+7 aggregation or unit conversion; retain both `valid_time` and `valid_time_local`.
- [ ] Support `--variables` for both resolutions, keeping mandatory coordinate/time columns.
- [ ] Write incrementally (chunked/streamed) to one destination Parquet without materializing the full 2016–2025 hourly dataset in memory.
- [ ] Preserve stable column naming, ordering, and numeric precision where supported; explicitly detect incompatible dtypes/schema.
- [ ] Stage output to a temporary file and publish only after complete validation. Remove incomplete temporary files on failure without deleting an existing valid export.
- [ ] Make same-selection reruns idempotent: validate and reuse or fail clearly; different source checksums must not be mistaken for an unchanged export.
- [ ] Reject conflicting output by default. Allow guarded replacement only via `--execute --force`, with failure-safe file/manifest publishing.

**Gate:** Each resolution yields one readable, independently usable Parquet with values faithful to its respective source.

### P4 — Manifest and fidelity verification

- [ ] Publish a same-stem `.manifest.json` alongside each completed Parquet.
- [ ] Record provider/dataset, bbox, target start/end dates, actual coverage, resolution/timezone, variable list and source units, expected coordinates/timestamps, row count, source artifact paths and SHA-256 digests, output SHA-256, and generation time.
- [ ] State that hourly values are raw-preserving, whereas daily values were **already transformed upstream**. The export itself performs no scientific transformation.
- [ ] Track overlap/partition-selection decisions and any source decoding choices sufficient for reproducibility; do not store quality statistics.
- [ ] Verify input vs exported values and missing-value positions; test equality of the resulting column and time/grid identities, not only row totals or checksums.
- [ ] Confirm the manifest and Parquet describe exactly the same artifact, even after reruns or failed writes.

**Gate:** An exported artifact can be traced to its exact input files and validated independently.

### P5 — Offline tests, documentation, and release

- [ ] Add focused unit tests under `tests/unit/pipeline/` for selection, input validation, overlapping context, variable errors, missing values, timestamp boundaries, and numeric fidelity.
- [ ] Test both resolutions; `all`, subset, reordered and invalid variable lists; 5-variable historical inputs; mixed/missing months; incompatible grids/schema; duplicate/conflicting timestamps.
- [ ] Test preview read-only behavior, existing output protection, `--force` safeguards, checksum changes, write interruption, and rerun idempotency using temporary directories.
- [ ] Run focused tests first, then `python -m pytest tests/unit -q`; run additional affected suites where needed. No live CDS or PostgreSQL needed for unit tests.
- [ ] After the export contract is accepted, update the owning `docs/app_description.md` and operator-facing `README.md` / `pipeline/README.md` without duplicating full specifications across files.
- [ ] Ensure generated analytics files and temporary outputs are ignored by Git and excluded from reset/destroy operations unless **explicitly** approved; inspect existing maintenance behavior before changing it.
- [ ] Make small, focused, verified commits on `dev` only. Never touch other branches; push only as authorized. Document tests run, limitations, and commit hashes.

**Acceptance:** Two selectable export modes, paired manifests, raw-preserving hourly values, unchanged daily science, explicit missing-variable failures, provenance, safe retries, offline tests, and no impact on current ETL/PostgreSQL/Airflow operation.

## 5. Non-goals and constraints

- No EDA notebooks, NULL/NaN summaries, outlier detection, data cleansing/imputation, wet-day classification, feature engineering, clustering, or model evaluation in this repository.
- No new daily scientific formulas and no conversion of raw ERA5-Land accumulated `tp`/`ssrd` to hourly increments during export.
- No API or PostgreSQL storage of analytics exports in this phase.
- No scheduled export DAG unless separately requested.
- No silent mixing of historical 5-variable and 9-variable files to fabricate coverage or missing variables.
- No destructive refresh of RAW, transformed checkpoints, databases, or existing analytics outputs.

## 6. Follow-up decisions (do not guess during coding)

- Confirm whether daily `wind_speed` and `wind_direction` should eventually be selectable as **derived** fields; keep provider-only `all` for the initial implementation.
- Decide whether supporting partial-year date ranges is necessary; current proposal exposes year-based campaign bounds only.
- Determine the safest coordinated replacement strategy for Parquet + manifest on the available filesystem before enabling `--force`.
