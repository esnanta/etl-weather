# ERA5-Land Analytics Export — Implementation Checklist

**Document role:** Plan a standalone, read-only-source CLI that publishes two reproducible, year-partitioned Parquet datasets for a separate notebook-based analytics project. Product/scientific requirements remain owned by [app_description.md](../app_description.md); existing ETL behavior and operator commands are documented in [README.md](../../README.md) and [pipeline/README.md](../../pipeline/README.md). This checklist is not evidence that the feature has been implemented.

**Branch:** `dev`  
**Created:** 2026-11-12  
**Status:** Planned; implementation not started  
**Scope:** Export only. EDA, data quality statistics, feature engineering, clustering, and model evaluation belong to a separate project (Jupyter / Google Colab).

## 1. Agreed outcome and boundaries

`etl-weather` acquires, validates, transforms, stores, and exposes meteorological datasets for analysis. The new analytics export is **not** an alternate ETL pipeline, an EDA engine, or a database load path.

| Export | Input | Output | Scientific treatment |
| --- | --- | --- | --- |
| `hourly` | Immutable `raw/YYYY/*.nc` | `analytics/<selection>/hourly/YYYY.parquet` | Preserve source values, units, UTC validity times, and grid coordinates; no scientific transformation |
| `daily` | Existing `transformed/YYYY/*.parquet` | `analytics/<selection>/daily/YYYY.parquet` | Preserve the existing UTC+7 daily values and timestamps; no recomputation |

For both exports:
- Select only variables present in the corresponding input files; never fetch missing variables from CDS.
- Keep missing observations missing. Do not impute, interpolate, rescale, convert units, recompute accumulations, or remove rows based on weather values.
- Do not compute or publish NULL/NaN counts, outlier summaries, descriptive statistics, wet-day labels, or model features. These belong to notebook EDA.
- Perform **structural and fidelity validation** (including missing-value correspondence) without treating that as EDA.
- Retain original RAW and transformed checkpoints unchanged, including five-variable historical files. Do not change existing extraction, daily transformation, PostgreSQL loading, or Airflow tasks.
- Do not add a database dependency, a new CDS request path, or a new CLI framework.

## 2. Output layout and identity

Use the existing dataset and bounding-box directory. **Each resolution is one logical dataset, stored as one Parquet file per target calendar year**, plus a dataset-level manifest. Separate variable selections so files never collide:

```text
data/
└── cds_reanalysis-era5-land/
    └── bbox_n6p1_w94p9_s1p9_e98p3/
        ├── raw/YYYY/*.nc
        ├── transformed/YYYY/*.parquet
        └── analytics/
            ├── all/
            │   ├── hourly/
            │   │   ├── 2016.parquet
            │   │   ├── ...
            │   │   ├── 2025.parquet
            │   │   └── manifest.json
            │   └── daily/
            │       ├── 2016.parquet
            │       ├── ...
            │       ├── 2025.parquet
            │       └── manifest.json
            └── t2m_tp/
                ├── hourly/
                │   ├── 2016.parquet
                │   ├── ...
                │   ├── 2025.parquet
                │   └── manifest.json
                └── daily/
                    ├── 2016.parquet
                    ├── ...
                    ├── 2025.parquet
                    └── manifest.json
```

- Directory signatures must be deterministic, case-normalized, sorted, and order-independent: `tp,t2m` and `t2m,tp` produce the same identity.
- `all` means the **nine active source/provider variables** (`tp,t2m,d2m,swvl1,swvl2,swvl3,ssrd,u10,v10`) from current configuration, not an assumption that every historic file has all nine.
- The daily source schema additionally contains derived `wind_speed` and `wind_direction`. For the initial CLI contract, `all` selects only the nine provider variables; preserve required `valid_time`, `valid_time_local`, `latitude`, and `longitude` keys. Do not silently include or calculate derived wind columns. Document an explicit future decision if these need selectable export.
- Each `YYYY.parquet` contains only that year's **target** timestamps: UTC calendar year for hourly; UTC+7 local calendar year for daily. Do not leak adjacent source-context hours/days into a partition.
- The manifest in each `hourly/` or `daily/` directory identifies the requested year range and all constituent partitions; filenames alone do not encode the complete campaign.
- Each complete annual partition must be independently readable by Pandas, Polars, or DuckDB. A full dataset can be read with a glob over `*.parquet` without concatenating into one large file.
- Future year additions must not rewrite valid, unchanged earlier-year Parquet files.
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
- `--output` (optional, explicit **dataset directory** destination; preserve the year partitions, dataset manifest, and collision safeguards).
- `--execute` (default preview-only).
- `--force` (explicit replacement intent; valid only with `--execute`, after all preflight checks).

A supported selection must be complete **for every requested annual partition and constituent month**, not merely present in one month. A missing selected variable or month must fail with a file/period-specific error. No implicit fallback to unrelated five-/nine-variable artifacts or to downloading from CDS.

## 4. Implementation phases

### P1 — Source and CLI contract audit

- [ ] Verify `dev`, clean/known working-tree status, and repository docs: `AGENTS.md`, `docs/app_description.md`, `pipeline/README.md`, `README.md`, `tests/AGENTS.md`, and `docs/python_se_principle.md`.
- [x] Inspect existing RAW and transformed file readers, storage paths, validation, checksums, metadata, and dependencies. Reuse production helpers instead of duplicating them. (Repository code audit; actual files pending.)
- [ ] Confirm real NetCDF coordinate/dimension names, masks/fill values, dtypes, and variable availability; confirm transformed daily schema (including both timestamps).
- [ ] Determine whether overlapping context hours exist among stored RAW partitions; specify unambiguous period ownership and source-selection priority before implementation.
- [x] Host-wide memory/disk capacity audit waived by user for the initial pilot; retain streaming/chunked design without assuming all rows fit in RAM.
- [ ] Record coverage/schema for the selected pilot inputs only; full 2016–2025 inventory waived. Never label incomplete year coverage as complete.
- [ ] Establish CLI parameter validation, default campaign range, year-partition ownership, output naming, preview behavior, and exit codes.

**Gate:** Scientific/source layout and selection semantics are confirmed before writing.

#### P1 audit findings — 2026-11-12 (repository-only; no local RAW access)

**Evidence base:** Read-only inspection of the GitHub `dev` branch at `8fe2a782b26338f92546d3c07682671a09a5ad8f`. The working tree on the execution server, actual `data/` files, file sizes, disk/RAM capacity, and local environment settings were **not accessible** through this inspection. Therefore the full P1 gate remains **open**.

| Subject | Verified from source | Consequence for export |
| --- | --- | --- |
| Ownership | `AGENTS.md` assigns reusable scientific/storage logic to `pipeline/` and thin entrypoints to `scripts/`. | Keep export reusable and Airflow-independent. |
| Paths/provenance | `pipeline/storage/artifacts.py` provides `DATA_DIRECTORY`, `build_bbox_signature()`, `calculate_file_sha256()`, and `artifact_path_for_metadata()`. Existing checkpoints have deterministic month/variable paths. | Reuse these helpers; do not infer the active variable generation merely from the filename. |
| I/O | `pipeline/storage/local_checkpoints.py` uses `xr.open_dataset`, `pd.read_parquet`, PyArrow metadata, and a same-directory temporary file for atomic single-file writes. Its `load_raw_netcdf()`, `convert_netcdf_to_dataframe()`, and `load_transformed_parquet()` materialize data in memory. | Reuse metadata/checksum policies but **do not** reuse whole-file materializing readers as the long-range export path. Design chunked reads / Parquet row-group writes. |
| RAW schema | `pipeline/validation/era5_land_raw.py` requires `valid_time`, `latitude`, `longitude`, hourly time ordering/continuity, and every requested variable on precisely those three axes. | Validate every source partition; examine actual file dtypes and fill-value encoding locally before selecting Parquet types. |
| RAW context | `pipeline/extraction/cds_era5_land.py` retrieves one preceding day using `LOCAL_DAY_CONTEXT_DAYS = 1`; the retrieval can span the prior month/year. | Assign rows to the requested **UTC** year by `valid_time` and resolve boundary overlaps explicitly. No duplicate row should survive by default. |
| Daily schema | `pipeline/transformation/era5_land_daily.py` produces `valid_time` and `valid_time_local` plus grid coordinates, nine configured provider variables, and derived wind columns when `u10,v10` exist. `pipeline/validation/era5_land_daily.py` checks local midnight and per-day grid identity. | Assign rows to the **UTC+7 local** calendar year using `valid_time_local`; export existing values without recomputation. |
| Variable set | `config.CDS_VARIABLES` contains nine provider variables. `pipeline/requests/weather.py` already has deterministic sorted variable signatures. | Keep `all` limited to these nine provider variables initially; check actual presence in **every** source partition. |
| Campaign range | `config.py` resolves `CAMPAIGN_START_YEAR` / `CAMPAIGN_END_YEAR` lazily from the environment. The pilot runbook reports an example with `CAMPAIGN_KEY=era5_land_2016_2025` but `CAMPAIGN_END_YEAR=2016`. | Never equate campaign key with coverage; actual environment must be inspected; explicit CLI years take precedence. |
| Dependencies | `requirements.txt` pins `xarray==2026.7.0`, `netCDF4==1.7.4`, `pandas==3.0.3`, `pyarrow==25.0.0`, and `numpy==2.5.1`. | Use existing libraries; do not add Dask/DuckDB as runtime dependencies without evidence they are needed. |
| Test placement | `tests/AGENTS.md` requires fast offline pipeline tests under `tests/unit/pipeline/`, with temporary source artifacts and no live CDS/PostgreSQL. | Cover both resolutions and failure/replay paths with controlled NetCDF/Parquet fixtures. |
| Output size caveat | The spatial contract defines 43 × 35 = 1,505 RAW grid coordinates; the May 2024 pilot runbook records **530 complete land-grid points/day** in its transformed output. | Do **not** assert daily rows = 1,505 × days. Preserve exactly the transformed checkpoint rows; compare against actual sources. |
| Destructive maintenance | `scripts/maintenance/reset_data.py` calls `clear_directory_contents(data/)`; `.gitignore` already ignores `/data/`. | **Correction to P5:** analytics under `data/` are currently deleted by approved reset/destroy operations. This plan must not claim otherwise. Either explicitly accept that behavior or separately approve a maintenance change; no silent exception. |
| Existing stale draft | `docs/readme_eda.md` still mentions one 2016–2025 raw Parquet file and notebook processing in-repo. | Reconcile this older draft later with the agreed separate-notebook-project and year-partitioned dataset contract; avoid duplicate product specifications. |

**Preliminary source-selection policy (needs local evidence):**
1. Discover only the configured dataset/bbox and requested target months. Identify the actual request period from the stored artifact name plus validated metadata; reject multiple competing artifacts that cannot be reconciled deterministically.
2. For RAW, include hours with `year(valid_time) == YYYY`; for daily, include rows with `year(valid_time_local) == YYYY`. For hourly periods, a monthly file's extra context hours are not owned by that target month unless they are the only validated source for a requested hour.
3. Compare overlapping records before accepting any canonical owner. A conflicting value, incompatible grid, or missing requested variable is a hard error; identical duplication is handled deterministically with documented provenance.
4. Treat the requested year range as a **completeness requirement**, not proof of data existence. Do not create partial output and label it complete.

**Local-only checks still required to close P1:**
- [ ] On the intended host, confirm `git branch --show-current` is `dev` and inspect `git status --short`; do not change branches.
- [ ] Inventory actual `raw/` and `transformed/` files and their monthly variable signatures across selected years; confirm which years/months are complete.
- [ ] Read representative NetCDF headers and transformed Parquet schemas (including 5-variable vs 9-variable generations) for coordinate dtypes, fill-value/mask decoding, source units, temporal context, and duplicate boundaries.
- [ ] Measure file sizes, output disk headroom and temporary filesystem usage with the existing `python -m scripts.monitoring.inspect_campaign_capacity` inspection helper or shell read-only equivalents.
- [ ] Validate a concrete small-period source-selection example and estimate annual memory/disk needs from actual data before accepting P2.
- [ ] Clarify whether analytics output under `data/` being deleted by reset/destroy is acceptable; until resolved, leave destructive maintenance unchanged.

**P1 scope decision — 2026-11-12:** The user explicitly waived the full historical source inventory and host capacity inspection as prerequisites to implementation. Only one real nine-variable RAW file is currently reported locally: `raw/2024/2024_0501_0531_d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10.nc`. Use a second adjacent month (proposed June 2024) as an **optional, narrowly scoped integration pilot**, acquired separately through the existing extraction pipeline if needed; never make Analytics Export download missing inputs. For the daily pilot, the corresponding existing transformed Parquet is also required or must first be generated through the existing transform stage. No second month is claimed to exist yet.

The test gate for P2–P5 is therefore **two adjacent months** plus synthetic offline fixtures; it does **not** require a completed 2016–2025 campaign or a host-wide capacity report. Keep chunked/streamed processing, input completeness checks for whichever years/months are explicitly requested, missing-variable errors, checksums, source fidelity, and safe output writes. Do not weaken these correctness guarantees simply because the pilot is small. Do not treat the single available month as a complete 2024 export; a partial-year research pilot needs an explicitly scoped test mode or temporary destination that cannot be mistaken for the complete annual dataset. Export completeness and accurate manifest coverage remain mandatory.

**P1 status:** Repository-level module and contract audit completed. The broad data-inventory and capacity gates are **waived by decision**; representative source-schema confirmation and two-month pilot evidence remain pending but are not blockers to starting P2 on synthetic fixtures. No source code, test, RAW, transformed checkpoint, or database mutation was performed during this audit.

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
- [ ] Write one Parquet per target calendar year, incrementally (chunked/streamed) without materializing an entire year or campaign in memory. Partition hourly by UTC year and daily by local UTC+7 year.
- [ ] Preserve stable column naming, ordering, and numeric precision where supported; explicitly detect incompatible dtypes/schema.
- [ ] Stage each annual Parquet to a temporary file and publish only after complete validation. Remove incomplete temporary files on failure without deleting valid prior partitions. Keep manifest consistency when some yearly partitions succeed and others fail.
- [ ] Make same-selection reruns idempotent **per year**: validate and reuse unchanged partitions or fail clearly. A new year may be added without regenerating prior years; changed source checksums must not be mistaken for unchanged output.
- [ ] Reject conflicting annual output by default. Allow guarded replacement only via `--execute --force`, with failure-safe annual file/manifest publishing; do not replace unrelated years.

**Gate:** Each resolution yields one logical, year-partitioned dataset with independently readable annual Parquet files faithful to their respective sources.

### P4 — Manifest and fidelity verification

- [ ] Publish one dataset-level `manifest.json` in each `<selection>/<resolution>/` directory, with a partition inventory for every published `YYYY.parquet`.
- [ ] Record provider/dataset, bbox, requested target year range, resolution/timezone, variable list and source units, generation time, and **for each year**: actual coverage, expected coordinates/timestamps, row count, source artifact paths and SHA-256 digests, output SHA-256, and partition status.
- [ ] State that hourly values are raw-preserving, whereas daily values were **already transformed upstream**. The export itself performs no scientific transformation.
- [ ] Track overlap/partition-selection decisions and any source decoding choices sufficient for reproducibility; do not store quality statistics.
- [ ] Verify input vs exported values and missing-value positions; test equality of the resulting column and time/grid identities, not only row totals or checksums.
- [ ] Confirm the manifest partition inventory and filesystem contain the same committed annual artifacts, even after incremental reruns or interrupted writes. Never advertise a partial/incomplete campaign as complete.

**Gate:** Every annual partition can be traced to its exact input files, and the dataset manifest describes the validated published years.

### P5 — Offline tests, documentation, and release

- [ ] Add focused unit tests under `tests/unit/pipeline/` for selection, input validation, overlapping context, variable errors, missing values, timestamp boundaries, and numeric fidelity.
- [ ] Test both resolutions; `all`, subset, reordered and invalid variable lists; 5-variable historical inputs; mixed/missing months; incompatible grids/schema; duplicate/conflicting timestamps.
- [ ] Test preview read-only behavior, existing output protection, `--force` safeguards, checksum changes, write interruption, **annual boundary placement**, independently readable year files, incremental addition of a new year, and per-year rerun idempotency using temporary directories.
- [ ] Run focused tests first, then `python -m pytest tests/unit -q`; run additional affected suites where needed. No live CDS or PostgreSQL needed for unit tests.
- [ ] After the export contract is accepted, update the owning `docs/app_description.md` and operator-facing `README.md` / `pipeline/README.md` without duplicating full specifications across files.
- [ ] Verify generated analytics files and temporary outputs remain ignored by Git. **Existing reset/destroy currently clears all of `data/`, including future `analytics/` exports**; decide explicitly whether this is acceptable before changing maintenance behavior.
- [ ] Make small, focused, verified commits on `dev` only. Never touch other branches; push only as authorized. Document tests run, limitations, and commit hashes.

**Acceptance:** Two selectable, year-partitioned analytics datasets with one manifest each, raw-preserving hourly values, unchanged daily science, explicit missing-variable failures, per-year provenance and safe incremental reruns, offline tests, and no impact on current ETL/PostgreSQL/Airflow operation.

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
- Determine the safest coordinated replacement strategy for annual Parquet partitions + dataset manifest on the available filesystem before enabling `--force`.
