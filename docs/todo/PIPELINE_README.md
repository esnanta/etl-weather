# Pipeline README — Documentation Checklist

Branch: `dev`  
Created: 2026-10-19

Purpose: create `pipeline/README.md` as the pipeline-specific operational and architectural guide without changing pipeline behavior or duplicating Airflow, database, geospatial, query, or testing documentation.

## 1. Source-of-truth review

- [x] Confirm the work is limited to branch `dev`.
- [x] Review root `AGENTS.md` for architecture, scientific, artifact, persistence, provenance, validation, and Airflow-boundary contracts.
- [x] Review root `README.md` for project-level setup and cross-domain documentation boundaries.
- [x] Inventory the current `pipeline/` package structure.
- [x] Review `pipeline/runtime.py` to document the actual ETL composition and checkpoint boundaries.
- [x] Review request, storage, validation, transformation, loading, and repository modules needed to describe current behavior.
- [x] Keep experimental monthly aggregation outside the active daily pipeline README except for an explicit boundary note.

## 2. README content

- [x] Create `pipeline/README.md`.
- [x] State the pipeline purpose and its boundary relative to Airflow.
- [x] Document the active provider/dataset and provider variable codes.
- [x] Document the package structure and responsibility of each subpackage/module.
- [x] Document request normalization, validation, monthly partitioning, and deterministic `request_key` identity.
- [x] Document the ETL stage flow and the three runtime boundaries: `ensure_raw_checkpoint()`, `ensure_transformed_checkpoint()`, and `load()`.
- [x] Document RAW NetCDF and transformed Parquet checkpoint semantics, including resume validation and overwrite protection.
- [x] Document UTC → Asia/Jakarta (UTC+7) daily semantics.
- [x] Document accumulated variables (`tp`, `ssrd`) and daily-mean variables (`t2m`, `d2m`, `swvl1`).
- [x] Document the canonical transformed schema and output units.
- [x] Document provenance metadata and checksum expectations.
- [x] Document PostgreSQL persistence identity, UPSERT behavior, and unchanged-partition skip conditions.
- [x] Document the read-only daily-climate repository boundary for modelling.
- [x] Document missing-value behavior and validation invariants.
- [x] Document the generated artifact directory layout.
- [x] Document supported entry points for local execution and Airflow callers without duplicating Airflow scheduling documentation.
- [x] Add concise testing guidance that points to `tests/README.md` instead of duplicating the testing policy.
- [x] Add links to the project-level, orchestration, database/testing, and upstream ERA5-Land documentation where appropriate.

## 3. Consistency review

- [x] Verify names and examples against current `dev` code.
- [x] Verify the current Aceh bounding box example is N 6.1 / W 94.9 / S 1.9 / E 98.3.
- [x] Verify artifact paths match the deterministic storage implementation.
- [x] Verify the README does not introduce ARCO or inactive provider paths.
- [x] Verify the README does not claim monthly aggregation is part of the daily Airflow ETL.
- [x] Verify Airflow-specific scheduling, retries, pools, pacing, and campaign progress remain in `orchestration/README.md`.
- [x] Verify database bootstrap/setup details remain project/database documentation concerns rather than being duplicated here.
- [x] Verify no pipeline source behavior is changed as part of this documentation task.

## 4. Project documentation integration

- [x] Update the root `README.md` domain-documentation table so Pipeline links to `pipeline/README.md`.
- [x] Update the root project tree to show `pipeline/README.md`.
- [x] Re-read the resulting documentation for broken relative links and obvious contradictions.
- [x] Mark this checklist complete only after the README and root documentation references are committed on `dev`.
