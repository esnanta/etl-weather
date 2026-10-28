# Geospatial Refactor — Work Plan and Checklist

**Document role:** Track the narrow, behavior-preserving refactor of `geospatial/`. Requirements remain owned by [app_description.md](../app_description.md), and current operational instructions remain in [geospatial/README.md](../../geospatial/README.md).

**Branch:** `dev`  
**Created:** 2026-10-28  
**Status:** Baseline verified (operator-reported); implementation not started  
**Owner boundary:** `geospatial/` owns source validation, boundary imports, and grid mapping; `database/` owns PostgreSQL connections and canonical DDL.

## Scope and acceptance rules

This is a maintainability refactor, **not** a redesign of geospatial processing. Preserve the current CLI entrypoints, database targets, imported source bytes, SQL behavior, database table/view identifiers, and transactional/idempotent behavior. No changes to production climate observations or Airflow orchestration.

- **In scope:** use the existing `database.connection.managed_database_engine()` in both geospatial CLIs; audit whether spatial configuration aliases are used; perform only clearly justified import/docstring/CLI cleanup.
- **Out of scope:** rewrite `_MAPPING_SQL` or `_STATUS_SQL`; change `reader.py` validation/normalization; adjust PostGIS matching rules; change source GeoJSON, spatial DDL, views, source identifiers, or monthly aggregation; add abstractions or move CLI modules without demonstrated need.
- **Deferred separately:** verify the authoritative release/version of the Aceh boundary dataset (`source_version="unverified"`). Do not guess or silently change provenance during refactor.

## Checklist

### 1. Audit and baseline
- [x] Review `AGENTS.md`, `geospatial/README.md`, `tests/AGENTS.md`, and `docs/python_se_principle.md`.
- [x] Inspect `geospatial/config.py`, `importers/reader.py`, `importers/districts.py`, and `mapping.py`, plus relevant unit/integration test code.
- [ ] Search the current `dev` tree for all references to `SPATIAL_SCHEMA`, `SPATIAL_PROVINCE_TABLE`, `SPATIAL_DISTRICT_TABLE`, and `SPATIAL_GRID_REGION_TABLE`. Document each consumer before deciding whether any alias is unused.
- [ ] Record a precise pre-refactor commit SHA and confirm a clean working tree before modifications.
- [x] Run focused baseline `python -m pytest tests/unit/geospatial -q` → **30 passed in 0.65s** (operator-reported, 2026-10-28).
- [x] Record isolated PostgreSQL baseline `python -m pytest tests/integration/geospatial -q` → **6 passed in 1.47s** (operator-reported, 2026-10-28). Test configuration/identity is protected by test fixtures but was not independently inspected during execution.

### 2. Engine lifecycle refactor
- [ ] Update `geospatial/importers/districts.py::main()` to use `managed_database_engine(config)` rather than local `create_database_engine()/dispose()`.
- [ ] Update `geospatial/mapping.py::main()` in the same way.
- [ ] Preserve `import_districts()` and `map_grid_regions()` signatures, transactional boundaries, returned summaries, CLI flags, preview behavior, and error propagation.
- [ ] Add or update targeted unit tests ensuring the engine is closed on normal completion and when import/mapping raises, without connecting to production PostgreSQL.

### 3. Configuration and small cleanup
- [ ] Decide whether each `SPATIAL_*` alias in `geospatial/config.py` is necessary based on actual references, not appearance. Preserve imports that are part of existing contracts.
- [ ] Remove only proven dead aliases after updating consumers/tests; otherwise document why they stay.
- [ ] Optionally normalize imports and concise docstrings within touched modules. No new helper, module, or class unless repetition demonstrably justifies it.
- [ ] Confirm CLI default is still non-mutating and writes require explicit `--execute`.

### 4. Preserve geospatial contracts
- [ ] Source GeoJSON stays byte-for-byte unchanged; reader still rejects incorrect CRS, dimensions, duplicate IDs/names, invalid coordinates, and malformed features.
- [ ] District import still validates the complete source before writes, verifies database identity, UPSERTs atomically, preserves unaffected districts on partial imports, and rolls back on failure.
- [ ] Grid mapping still reads distinct canonical daily coordinates, uses `ST_Covers`, and distinguishes zero/one/multiple district candidates as `unmatched`/`matched`/`ambiguous`, without arbitrary assignment.
- [ ] Repeated mapping does not duplicate rows and retains `mapped_at` when classification is unchanged.
- [ ] Preview remains read-only; neither climate observations nor source boundaries are modified by mapping.
- [ ] PostGIS stays within `climate_data`; all DDL remains owned by `database/` and no Airflow dependency enters reusable geospatial modules.

### 5. Post-refactor verification
- [ ] Run focused unit tests: `python -m pytest tests/unit/geospatial -q`.
- [ ] Run full unit tests: `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q`.
- [ ] Run isolated geospatial integration tests: `python -m pytest tests/integration/geospatial -q`.
- [ ] Run full integration tests when appropriate: `python -m pytest tests/integration -q`.
- [ ] Compare post-refactor results with the newly recorded focused baseline and the prior project-wide evidence (397 unit passed; 17 integration passed on 2026-10-28). Prior counts are historical, not proof of this refactor.
- [ ] Inspect diff for accidental SQL/contract changes and run import/reference checks for affected modules.
- [ ] Update `geospatial/README.md` only if operational behavior or documented commands actually change.
- [ ] Record changed files, commit SHA(s), test commands/results, limitations, and reviewer findings; mark status **Closed** only when evidence supports it.

## Baseline facts from source inspection (not execution evidence)

- `importers/reader.py` validates GeoJSON entirely before the importer writes.
- `importers/districts.py` has one `engine.begin()` transaction for district/province persistence, with database identity checks and idempotent UPSERTs.
- `mapping.py` uses separate SQL for preview versus persistence; changing their shared classification SQL is intentionally deferred.
- Existing related coverage: `tests/unit/geospatial/` and `tests/integration/geospatial/` (including import rollback and repeated grid mapping).
- No new pytest command has been executed by the assistant for this workstream.

## Execution log

| Date | Step | Evidence | Status |
| --- | --- | --- | --- |
| 2026-10-28 | Source and documentation audit | Modules, README, rules, and test coverage inspected | Complete |
| 2026-10-28 | Focused unit baseline | Operator: 30 passed in 0.65s | Complete |
| 2026-10-28 | Geospatial integration baseline | Operator: 6 passed in 1.47s | Complete |
| — | Implementation | No code changes yet | Pending |
| — | Regression and final review | Awaiting implementation and tests | Pending |
