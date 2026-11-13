# ERA5-Land Analytics Export — Implementation Checklist

**Document role:** Plan a standalone, read-only-source CLI that publishes two reproducible, year-partitioned Parquet datasets for a separate notebook-based analytics project. Product/scientific requirements remain owned by [app_description.md](../app_description.md); existing ETL behavior and operator commands are documented in [README.md](../../README.md) and [pipeline/README.md](../../pipeline/README.md). This checklist is not evidence that the feature has been implemented.

**Branch:** `dev`  
**Created:** 2026-11-12  
**Status:** P1–P5 accepted for the previous date-driven pilot (37 analytics/497 unit tests). Source-driven CLI change committed on `dev`, pending new focused and full-unit regression verification; safe expansion of partial annual files remains unresolved.  
**Scope:** Export only. EDA, data quality statistics, feature engineering, clustering, and model evaluation belong to a separate project (Jupyter / Google Colab).

## Current CLI contract — source-driven export (2026-11-13)

**Latest change, not yet verified by local pytest:** The user replaced date-window selection with source-driven discovery. The current CLI no longer accepts `--start-date`/`--end-date`. It automatically inventories all monthly `raw/YYYY/*.nc` checkpoints for `hourly` or all monthly `transformed/YYYY/*.parquet` checkpoints for `daily`, groups available months by year, validates them, and writes/verifies the existing `YYYY_<resolution>.parquet` outputs with one dataset-level manifest.

```bash
python -m scripts.run_analytics --resolution hourly --variables all
python -m scripts.run_analytics --resolution daily --variables all
python -m scripts.run_analytics --resolution hourly --variables all --execute
python -m scripts.run_analytics --resolution daily --variables all --execute
```

Source-year partitions do **not** require contiguous calendar months or a completed campaign. Every discovered source month must be valid and contain the selected variables; malformed, incompatible and ambiguous months cause explicit failures rather than silent skips. `complete_year` requires all twelve months. Nonconsecutive months are listed explicitly under `source_months` in the manifest; first/last dates alone are not proof of uninterrupted coverage.

**Important migration constraint:** The canonical 2024 hourly/daily files already contain May–June only. If additional 2024 source months exist, the current no-overwrite policy correctly refuses to replace those files with different coverage. Do not delete or replace the existing exports without a separate guarded replacement decision. Rerunning the new date-free CLI on unchanged May–June source inventory should reuse the existing outputs and manifests.

The older dated CLI snippets below describe **historical implementation checkpoints only** and are superseded by this section and the current operator README. Keep them as historical evidence, not live run commands.

**New focused regression tests:** `test_analytics_selection.py` checks automatic discovery, missing-month gaps, incomplete-variable and ambiguous files; `test_analytics_cli.py` checks date-free dispatch and obsolete flag rejection; `test_analytics_release.py` checks gapped-month manifest coverage. Run focused tests and full `tests/unit` before accepting the changed CLI.

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
            │   │   ├── 2016_hourly.parquet
            │   │   ├── ...
            │   │   ├── 2025_hourly.parquet
            │   │   └── manifest.json
            │   └── daily/
            │       ├── 2016_daily.parquet
            │       ├── ...
            │       ├── 2025_daily.parquet
            │       └── manifest.json
            └── t2m_tp/
                ├── hourly/
                │   ├── 2016_hourly.parquet
                │   ├── ...
                │   ├── 2025_hourly.parquet
                │   └── manifest.json
                └── daily/
                    ├── 2016_daily.parquet
                    ├── ...
                    ├── 2025_daily.parquet
                    └── manifest.json
```

- Directory signatures must be deterministic, case-normalized, sorted, and order-independent: `tp,t2m` and `t2m,tp` produce the same identity.
- `all` means the **nine active source/provider variables** (`tp,t2m,d2m,swvl1,swvl2,swvl3,ssrd,u10,v10`) from current configuration, not an assumption that every historic file has all nine.
- The daily source schema additionally contains derived `wind_speed` and `wind_direction`. For the initial CLI contract, `all` selects only the nine provider variables; preserve required `valid_time`, `valid_time_local`, `latitude`, and `longitude` keys. Do not silently include or calculate derived wind columns. Document an explicit future decision if these need selectable export.
- Each `YYYY_hourly.parquet` or `YYYY_daily.parquet` contains only that year's **target** timestamps: UTC calendar year for hourly; UTC+7 local calendar year for daily. Do not leak adjacent source-context hours/days into a partition.
- The manifest in each `hourly/` or `daily/` directory identifies the requested year range and all constituent partitions; filenames alone do not encode the complete campaign.
- Each complete annual partition must be independently readable by Pandas, Polars, or DuckDB. A full dataset can be read with a glob over `*.parquet` without concatenating into one large file.
- Future year additions must not rewrite valid, unchanged earlier-year Parquet files.
- Never merge artifacts from different bounding boxes, provider datasets, incompatible schemas, or conflicting source identities.

## 3. Original interface proposal (historical; superseded)

Follow the repository's existing thin `scripts/` entrypoint + `pipeline/` domain logic convention:

```bash
# Preview only; no export is written
python -m scripts.run_analytics --resolution hourly --variables t2m,tp \
  --start-date 2024-05-01 --end-date 2024-06-30

# Produce the hourly raw-preserving export
python -m scripts.run_analytics --resolution hourly --variables t2m,tp \
  --start-date 2024-05-01 --end-date 2024-06-30 --execute

# Produce the daily already-transformed export
python -m scripts.run_analytics --resolution daily --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30 --execute
```

Proposed arguments:
- `--resolution {hourly,daily}` (required).
- `--variables all|comma,separated,codes` (default `all`).
- `--start-date` and `--end-date` (both required, inclusive `YYYY-MM-DD` dates; used for monthly pilots, full years, or multi-year periods). The analytics CLI does not infer periods from campaign settings.
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
- [x] Establish CLI date-only parameter validation, year-partition ownership, output naming and preview behavior. No implicit campaign date defaults.

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

#### P2 implementation update — 2026-11-12

**Status: P2 source selection and validation accepted based on user-reported local verification (2026-11-12).** No analytics Parquet or manifest is written in P2.

- `pipeline/analytics/selection.py` discovers one unambiguous existing monthly RAW/ transformed checkpoint per month, using the canonical dataset, Aceh bbox, month boundaries, and variable signature. Missing months, unsupported variables, and competing source generations are hard errors.
- Source variables are restricted to the nine provider variables; `all` does not include derived daily wind fields.
- RAW validation checks full target-month hourly coverage, ordering, timestamps, coordinates and spacing; identical overlapping context values are allowed for later deterministic month ownership, while different values raise an error.
- Daily validation checks the two time columns, UTC+7 offset, local midnight, day-by-day coverage, and stable local-day/grid identities. It reads time/grid columns only.
- `scripts/run_analytics.py` is an intentionally **read-only preview/validation CLI**. The CLI requires explicit `--start-date` and `--end-date`, with no year-based alternative. The `--execute` write mode is intentionally unavailable until P3.
- Offline tests: `tests/unit/pipeline/test_analytics_selection.py` and `tests/unit/pipeline/test_analytics_cli.py`. User ran these tests locally: **12 passed in 0.75s**.

Pilot commands (after activating the existing local environment):

```bash
python -m pytest tests/unit/pipeline/test_analytics_selection.py tests/unit/pipeline/test_analytics_cli.py -q

python -m scripts.run_analytics --resolution hourly --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30

python -m scripts.run_analytics --resolution daily --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30
```

The user ran both CLI previews locally with `--variables all` for **2024-05-01 through 2024-06-30**: both reported **P2 validation passed**, selecting one May and one June source checkpoint in their respective layers. The pilot verifies **May–June only**, not the completeness of calendar year 2024. Once successful, P3 must filter output strictly to target timestamps and retain pilot coverage in the manifest rather than publishing a falsely complete year.

**Open P2/P3 boundary:** Missing-value preservation and source-export numerical equivalence must be tested against the actual streamed output in P3/P4; P2 validates source identity and overlap values but does not serialize data.

### P2 — Reusable selection and validation

- [x] Add narrow analytics source selection/validation functions under `pipeline/` and a thin read-only `scripts/run_analytics.py` preview entrypoint; no `weather_cli` package.
- [x] Discover input artifacts by dataset, bbox, year, date partition, and variable signature; reject other sources/areas.
- [x] Validate selected variable presence in source schemas for every included partition; report missing/ambiguous files and variables.
- [x] Validate RAW coordinate/time axes and UTC+7 daily local-day/grid identities; real May–June 2024 preview confirmed by user.
- [ ] Apply output-row filtering by target timestamp in P3; P2 validates monthly source coverage and context boundaries.
- [ ] P2 compares overlapping RAW values and **fails** on conflicts; select canonical month-owned output rows and record overlap provenance in P3/P4.
- [ ] Verify NetCDF mask/`_FillValue` decoding and numerical/missing-value fidelity in P3/P4 using the actual export.
- [x] P2 CLI is preview-only and contains no source/output write path; local tests and both source previews passed.

**P2 gate accepted:** Source discovery and validation passed offline unit tests (12 passed) and two real May–June 2024 previews, per user-reported local output. No export serialization has been tested yet; output-row filtering, missing-value fidelity, and manifest correctness remain P3/P4 gates.

### CLI contract simplification — 2026-11-12

After successful user-reported P3 pilot exports (**19 focused unit tests passed; 2,203,320 hourly rows and 32,330 daily rows for 2024-05-01 through 2024-06-30**), the user chose **one explicit date-only CLI contract**: both `--start-date` and `--end-date` are required. The redundant `--start-year` and `--end-year` flags and campaign-derived default date range are removed. Annual Parquet partitioning remains unchanged. CLI date argument regression tests were updated; these new tests await local execution.

### P3 implementation checkpoint — 2026-11-12

**Status: code and offline tests committed on `dev`; local pytest and real May–June export still need verification.**

- `pipeline/analytics/export.py` streams RAW by <=24 hourly steps (selected NetCDF variables only) and transformed daily Parquet through bounded Arrow batches. It produces one target-year Parquet per resolution, keeping provider values unchanged and mandatory time/grid columns.
- Only month-owned target hours are written from RAW; previous-month context is excluded after P2 overlap validation. Daily rows are filtered by `valid_time_local`.
- All requested years, including partial coverage, use `analytics/<selection>/<resolution>/YYYY_<resolution>.parquet`. A partial-year file is **not** treated as complete: it retains exact target dates and `partial_year` in Parquet metadata and the manifest. A later full-year request for the same year is rejected until explicit safe replacement is agreed.
- Source units provided by RAW variable attributes and exact target period/coverage flags are included in Parquet schema metadata. No scientific transformations are performed.
- Writes are staged in a same-directory temporary file and published with an exclusive hard link. Existing destinations are refused; incomplete temporary files are cleaned up. **No `--force` or manifest-aware reuse in P3**; those require P4's provenance contract.
- `scripts/run_analytics.py --execute` performs export after source validation; without `--execute` it stays read-only. `--output` is an optional explicit destination dataset directory.
- Added `tests/unit/pipeline/test_analytics_export.py` and updated existing CLI tests. The assistant could not run the repo tests in the user's local environment; execution evidence is pending.

**Focused local test:**

```bash
python -m pytest tests/unit/pipeline/test_analytics_selection.py \
  tests/unit/pipeline/test_analytics_cli.py \
  tests/unit/pipeline/test_analytics_export.py -q
```

**Real pilot (run only after tests pass):**

```bash
python -m scripts.run_analytics --resolution hourly --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30 --execute

python -m scripts.run_analytics --resolution daily --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30 --execute
```

Pilot outputs now live directly under `analytics/all/hourly/` and `analytics/all/daily/`, with filenames `2024_hourly.parquet` and `2024_daily.parquet`. Do not run `--execute` twice without inspecting the first output: P3 correctly rejects already-published files rather than overwriting them. P4 adds dataset manifests/checksums and verified idempotent reuse.

**P3 gate:** Pending local tests and input/output fidelity checks. The manifest, per-source SHA-256 inventory and verified rerun reuse have been implemented in P4; local verification remains pending.

### P3 — Parquet export for both resolutions

- [x] Implement `hourly` NetCDF-to-tabular export retaining original numeric values, units (in metadata), UTC `valid_time`, and original `latitude/longitude`.
- [x] Implement `daily` concatenation of existing transformed Parquet without rerunning UTC+7 aggregation or unit conversion; retain both `valid_time` and `valid_time_local`.
- [x] Support `--variables` for both resolutions, keeping mandatory coordinate/time columns.
- [x] Write one Parquet per target calendar year, incrementally (chunked/streamed) without materializing an entire year or campaign in memory. Partition hourly by UTC year and daily by local UTC+7 year.
- [x] Preserve stable column naming, ordering, and numeric precision where supported; explicitly detect incompatible dtypes/schema.
- [x] Stage each annual Parquet to a temporary file and publish only after complete validation. Remove incomplete temporary files on failure without deleting valid prior partitions. Keep manifest consistency when some yearly partitions succeed and others fail.
- [x] P4 adds manifest/checksum-aware per-year reuse, verifying unchanged existing output and adopting valid P3 legacy files without rewriting them. New non-conflicting years are independent.
- [x] Reject conflicting annual output by default and keep exclusive Parquet publication. P4 intentionally does not expose `--force`; replacement remains a separate explicit design decision.

**Gate:** Each resolution yields one logical, year-partitioned dataset with independently readable annual Parquet files faithful to their respective sources.

### P4 implementation checkpoint — 2026-11-12

**Status:** Implementation committed to `dev`, verification on the user's host **not yet run**.

- `pipeline/analytics/verification.py` performs **streaming, row-for-row source/output equality** for the selected time/grid columns and provider variables, including missing positions. It rejects changes to output coverage metadata, schema, values, row counts, or source identity.
- One `manifest.json` is published in each physical dataset directory. Complete years live under `analytics/<variables>/<resolution>/manifest.json`; **partial-year exports** live in the same resolution directory, and manifest entries explicitly record `target_start`, `target_end`, and `coverage=partial_year`. A conflicting full-year request fails safely; filenames alone do not imply completeness.
- Each manifest records CDS ERA5-Land identity, bbox, selected variables, resolution, timezone and treatment, plus per-partition target date coverage, completion flag, column identities, row count, source/output SHA-256 digests and units. Daily units are those of the **upstream** transformed dataset, not a new conversion.
- Existing P3 Parquet files with **no manifest** are adopted only after the source/output equality check passes, preserving the existing Parquet bytes. If an existing source or output differs, the run fails rather than replacing it.
- The writer performs preflight on existing outputs; missing outputs are written, verified and then registered. A crash after the Parquet publish but before the manifest publish leaves an unregistered file that can be revalidated and adopted on the next run. Manifest publication uses a same-directory temporary file and `os.replace()`.
- Same-selection, unchanged reruns verify and reuse Parquet and manifest. **Do not enable `--force`** without a separately agreed multi-artifact replacement/recovery policy; the P4 implementation never overwrites Parquet.
- Added offline `tests/unit/pipeline/test_analytics_verification.py` for manifest, reuse, legacy adoption, source/output changes, missing values and failed JSON publication, and updated the existing export/CLI tests.

**Focused tests to run on the local `dev` checkout:**

```bash
python -m pytest tests/unit/pipeline/test_analytics_selection.py \
  tests/unit/pipeline/test_analytics_cli.py \
  tests/unit/pipeline/test_analytics_export.py \
  tests/unit/pipeline/test_analytics_verification.py -q
```

**After tests pass, adopt the two existing real May–June P3 outputs without deleting/re-exporting them:**

```bash
python -m scripts.run_analytics --resolution hourly --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30 --execute

python -m scripts.run_analytics --resolution daily --variables all \
  --start-date 2024-05-01 --end-date 2024-06-30 --execute
```

Expected new JSON files:

```text
analytics/all/hourly/manifest.json
analytics/all/daily/manifest.json
```

If the fidelity check reports a discrepancy, **stop** and investigate the source/output values; do not delete or overwrite existing checkpoints or outputs. A successful P3 row-count report alone does not prove source fidelity.

### P4 local feedback — 2026-11-13

The user reported **32 passed, 1 failed** in the focused P4 tests. The failing `test_legacy_pilot_with_changed_values_is_not_linked` had used `pd.read_parquet(...).to_parquet(...)` to mutate `tp`, unintentionally removing the `analytics_*` Parquet schema metadata. The production verifier correctly rejected the file at the **metadata** boundary, whereas the test expected a **value mismatch**. The test fixture has been corrected to modify the Arrow column while preserving the source schema metadata and assert the explicit source/output mismatch; rerun the focused tests locally. No validation rules were weakened.

The user also reported a successful **real hourly migration** to `analytics/all/hourly/2024_hourly.parquet` (**2,203,320 rows**, verification and manifest publication completed). The **daily** migration has not yet been reported. Avoid deleting or regenerating the hourly output simply because an unrelated test failed.

### Output naming simplification — 2026-11-13

**Approved layout:** Store each year directly under its resolution folder with a resolution-specific filename, e.g. `analytics/all/hourly/2024_hourly.parquet`, `analytics/all/daily/2024_daily.parquet`, plus `manifest.json` in each resolution folder. No new `pilot_<date>` directories are created.

**Important:** May–June 2024 remains **partial-year coverage**. The existing metadata/manifest verification rejects a later incompatible request for full-year 2024 rather than silently relabeling or overwriting the partial file.

**Legacy P3 migration:** The previously created `pilot_20240501_20240630/2024.parquet` files are validated against source first, then linked non-destructively to the canonical filenames on the same filesystem. Original pilot files/directories are intentionally left untouched as a recovery copy; cleanup, if desired, requires a separate explicit decision. If the legacy output differs, fail without migrating. The new manifest is created directly in the resolution directory.

**Verification:** Unit tests for flat paths, filename-based manifest keys, migration, incompatible-coverage rejection, and rerun protection have been revised; **local pytest is pending**.

### P4 acceptance evidence — 2026-11-13

**P4 pilot gate accepted based on user-reported local results.**

- Focused offline tests: `python -m pytest tests/unit/pipeline/test_analytics_selection.py tests/unit/pipeline/test_analytics_cli.py tests/unit/pipeline/test_analytics_export.py tests/unit/pipeline/test_analytics_verification.py -q` → **33 passed in 3.25s**.
- Hourly nine-variable May–June pilot: `analytics/all/hourly/2024_hourly.parquet`, **2,203,320 rows**, previously reported successfully verified/published with manifest.
- Daily nine-variable May–June pilot: `analytics/all/daily/2024_daily.parquet`, **32,330 rows**, newly reported **P4 export verification and manifest publication completed**.
- Both exports cover **2024-05-01 through 2024-06-30 only**, and must remain identified as `partial_year`.
- The reported outputs verify the P4 pilot workflow, not complete 2024 coverage or a whole-repository test run. P5 includes broader regression, docs and release checks.

### P4 — Manifest and fidelity verification

- [x] Publish one dataset-level `manifest.json` in each `<selection>/<resolution>/` directory, with a partition inventory for every published `YYYY_<resolution>.parquet`.
- [x] Record provider/dataset, bbox, requested target year range, resolution/timezone, variable list and source units, generation time, and **for each year**: actual coverage, expected coordinates/timestamps, row count, source artifact paths and SHA-256 digests, output SHA-256, and partition status.
- [x] State that hourly values are raw-preserving, whereas daily values were **already transformed upstream**. The export itself performs no scientific transformation.
- [ ] Track detailed overlap/partition-selection decisions and any source decoding choices sufficient for reproducibility; do not store quality statistics.
- [x] Verify input vs exported values and missing-value positions; test equality of the resulting column and time/grid identities, not only row totals or checksums.
- [x] Confirm the manifest partition inventory and filesystem contain the same committed annual artifacts, even after incremental reruns or interrupted writes. Never advertise a partial/incomplete campaign as complete.

**Gate:** Every annual partition can be traced to its exact input files, and the dataset manifest describes the validated published years.

### P5 implementation checkpoint — 2026-11-13

**Status: pilot release verification passed based on user-reported local results.**

- The user reran both canonical May–June 2024 `--execute` commands and reported successful verification/reuse of `2024_hourly.parquet` (**2,203,320 rows**) and `2024_daily.parquet` (**32,330 rows**) without path overrides. This is an end-to-end idempotent rerun on the real pilot.
- `docs/app_description.md` now owns the offline export requirements: separate RAW-preserving hourly vs already-transformed daily exports, required dates/variable selection, year-specific filenames, explicit `partial_year`, missing-value fidelity, SHA-256 provenance, and safe reuse/no overwrite.
- Root `README.md` documents the operator CLI, automatically determined storage paths, dataset-level manifests, and the limitations of partial-year reruns and existing annual-file replacement. `pipeline/README.md` describes domain module responsibilities.
- Root `.gitignore` already ignores `/data/`, which includes analytics files and temporary writers. No new ignore rule is necessary.
- Added `tests/unit/pipeline/test_analytics_release.py` for case/order-independent subset selection, manifest checksum tamper rejection, RAW context exclusion, and two-year output/manifest inventory.
- No changes to ETL/PostgreSQL/Airflow runtime or scientific transformations. No credentials or dataset files committed.
- **Release verification:** User ran focused analytics tests (**37 passed in 3.04s**) and the full `tests/unit` suite (**497 passed in 14.05s**), with no reported failures. The existing `git check-ignore` rule also returned the expected hourly analytics path. These are user-reported local results; no integration suite or full historical campaign is claimed.
- **Known limitation:** A partial `2024_<resolution>.parquet` cannot later be expanded into a complete 2024 file under the current guarded no-overwrite policy. This is intentional and must be resolved by a separate safe replacement design before using the tool for expanding partial annual partitions.

Recommended final verification:

```bash
git branch --show-current
git check-ignore data/cds_reanalysis-era5-land/bbox_n6p1_w94p9_s1p9_e98p3/analytics/all/hourly/2024_hourly.parquet

python -m pytest \
  tests/unit/pipeline/test_analytics_selection.py \
  tests/unit/pipeline/test_analytics_cli.py \
  tests/unit/pipeline/test_analytics_export.py \
  tests/unit/pipeline/test_analytics_verification.py \
  tests/unit/pipeline/test_analytics_release.py -q

python -m pytest tests/unit -q
```

### P5 local acceptance — 2026-11-13

The user confirmed a successful final regression: **37 focused analytics tests passed in 3.04s** and **497 unit tests passed in 14.05s**. `git check-ignore` returned the expected canonical hourly Parquet path, confirming that analytics output is excluded from Git by `/data/`. Both actual May–June hourly and daily exports had also passed verification and idempotent reruns in the canonical output directories.

**P5 is accepted for the two-month pilot.** Do not extrapolate this to completed historical coverage, live CDS/PostgreSQL integration testing, or an ability to extend an already published partial-year file. The remaining historical five-variable/mixed-schema scenarios and safe partial-year replacement are tracked as limitations rather than claimed as tested.

### P5 — Offline tests, documentation, and release

- [x] Add focused unit tests under `tests/unit/pipeline/` for selection, input validation, overlapping context, variable errors, missing values, timestamp boundaries, and numeric fidelity.
- [ ] Test both resolutions; `all`, subset, reordered and invalid variable lists; 5-variable historical inputs; mixed/missing months; incompatible grids/schema; duplicate/conflicting timestamps. **P5 additions cover subset order, context and year boundaries; historical 5-variable/export-mixed-schema cases require further verification.**
- [ ] Test preview read-only behavior, existing output protection, checksum changes, write interruption, **annual boundary placement**, independently readable year files, incremental addition of a new year, and per-year rerun idempotency using temporary directories. **No `--force` is offered; incremental-year cases added but local tests pending.**
- [x] Run focused tests first, then `python -m pytest tests/unit -q`: 37 focused and 497 repository unit tests passed per user-reported local output. No live CDS or PostgreSQL needed.
- [x] After the export contract is accepted, update the owning `docs/app_description.md` and operator-facing `README.md` / `pipeline/README.md` without duplicating full specifications across files.
- [x] Verify generated analytics files and temporary outputs remain ignored by Git. User confirmed `git check-ignore` returns the expected hourly analytics path; `/data/` also covers generated temp files. **Existing reset/destroy currently clears all of `data/`, including future `analytics/` exports**; decide explicitly whether this is acceptable before changing maintenance behavior.
- [x] Make small, focused commits on `dev` only. Document user-reported tests, remaining coverage limitations and commit hashes.

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
- Resolved: partial-year and full-year periods are supported using required `--start-date` and `--end-date`; year-only flags are not supported.
- Determine the safest coordinated replacement strategy for annual Parquet partitions + dataset manifest on the available filesystem before enabling `--force`.
