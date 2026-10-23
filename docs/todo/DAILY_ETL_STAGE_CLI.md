# Daily ETL — Independent Stage CLIs

Branch: `dev`  
Created: 2026-10-22  
Status: implemented on `dev`; verification pending execution in project environment

## Goal and scope

Expose independent CLI entry points for the **existing** daily CDS ERA5-Land extract, transform, and load boundaries. Keep `scripts.run_pipeline` as the complete ETL wrapper. This is a CLI/runtime-composition refactor, **not** a change to scientific processing, artifact identity, storage format, or database persistence.

Approved commands. **The documented default must be the complete pipeline first; independent stage commands are optional maintenance/recovery operations:**

```bash
# Default: complete daily ETL
python -m scripts.run_pipeline

# Optional: run only the needed stage
python -m scripts.run_extract
python -m scripts.run_transform
python -m scripts.run_load
```

All commands derive the request from the existing configured environment (`PIPELINE_START_DATE`, `PIPELINE_END_DATE`, provider/dataset, variables, and bbox), retain inclusive date semantics, and split requests into the **same deterministic calendar-month partitions**.

## Stage contracts

| Command | Prerequisite | Performs | Produces | Must not do |
| --- | --- | --- | --- | --- |
| `run_extract` | Valid configured request; no existing artifact prerequisite | Ensure/reuse and validate RAW checkpoint | RAW NetCDF | Transform or write PostgreSQL |
| `run_transform` | Existing **valid RAW NetCDF** for each request partition | Validate RAW, ensure/reuse and validate transformed checkpoint | Daily UTC+7 Parquet | Download from CDS or write PostgreSQL |
| `run_load` | Existing **valid transformed Parquet** for each request partition, including its RAW provenance/checksum contract | Validate transformed checkpoint, idempotent load | Canonical PostgreSQL observations | Download or transform |
| `run_pipeline` | Valid configured request | Existing extract → transform → load flow | RAW + Parquet + PostgreSQL | Change the established complete-ETL behavior |

Missing prerequisites must produce clear, nonzero failures; **do not silently auto-run upstream stages**. All four commands use existing normalization, validation, and deterministic `request_key` behavior. Multi-month commands process each monthly partition in deterministic order; completed earlier partitions remain independently resumable if a later partition fails.

## Idempotency and checkpoint invariants (acceptance-critical)

### Extract
- [ ] An absent RAW artifact is retrieved, validated, and saved at its deterministic path.
- [ ] An existing RAW artifact is **loaded and scientifically validated**; it is **not downloaded again or overwritten**.
- [ ] Corrupt/invalid RAW does **not** count as a successful checkpoint and is not silently replaced; fail explicitly.
- [ ] Repeated `run_extract` with identical request identity produces no additional CDS retrieval and no duplicate or mutated RAW artifact.
- [ ] Distinct request identities (period/variables/bbox/provider/dataset) remain distinct partitions; do not globally deduplicate unrelated requests.

### Transform
- [ ] Absent or invalid RAW causes a clear failure, **without** CDS network access.
- [ ] A missing transformed checkpoint is produced only from a validated RAW checkpoint.
- [ ] An existing transformed Parquet checkpoint is **validated and reused**, never silently overwritten.
- [ ] Validate RAW lineage: transformed `raw_artifact_path` and `raw_sha256` must match expected RAW identity/checksum; reject stale/mismatched provenance.
- [ ] A repeated `run_transform` on unchanged valid inputs does not regenerate or mutate Parquet; no database write occurs.
- [ ] Scientific UTC+7 daily semantics, complete hourly coverage, units, schema, missing-value rules, and deterministic artifact paths remain unchanged.

### Load
- [ ] Missing/invalid transformed checkpoint causes a clear failure, **without** extract or transform.
- [ ] Revalidate the transformed checkpoint and its RAW provenance before load; do not bypass checkpoint validation for reruns.
- [ ] Preserve per-`request_key` idempotency. A database-write skip is allowed **only when all match**: `request_key`, transformed artifact path, transformed SHA-256, and expected observation count.
- [ ] If any skip condition does not match, execute the normal transactional metadata and observation UPSERT path; do not silently skip or blind-append.
- [ ] Preserve canonical observation identity: `daily_climate_id + valid_time + latitude + longitude`; reruns must not add duplicate observations.
- [ ] Preserve transaction rollback on failure and SQL `NULL` for missing weather values.
- [ ] Repeated `run_load` on unchanged valid Parquet/database state makes no duplicate observations and skips unnecessary writes; a changed partition uses the existing verified UPSERT contract.

### Complete pipeline / equivalence
- [ ] `run_pipeline` preserves its current behavior, logs, validation, resume, and per-partition idempotency.
- [ ] Executing extract → transform → load separately yields the **same partition identities, artifacts, and canonical observations** as the complete pipeline for equivalent initial conditions (allowing expected timestamps/log differences for newly created artifacts).
- [ ] Partial failure followed by a rerun safely resumes completed checkpoints/loads; no repeated CDS submission for already valid RAW.
- [ ] A stage does not treat the mere existence of a file or matching row count as proof of correctness.

## Architecture and implementation checklist

- [x] Confirm the current `dev` baseline and call sites of `pipeline.runtime`, `scripts.run_pipeline`, and Airflow task wrappers before changing code.
- [x] Keep `scripts/` as **thin CLI entry points** and `pipeline/` as owner of extraction, transformation, validation, persistence, and runtime composition.
- [x] Add `scripts/run_extract.py`, `scripts/run_transform.py`, and `scripts/run_load.py`; reuse `build_cds_request()` and the existing application-environment validation convention.
- [x] Reuse the existing runtime boundaries `ensure_raw_checkpoint_partition()`, `ensure_transformed_checkpoint_partition()`, `load_request_partition()` rather than rebuilding ETL logic.
- [x] Provide small, shared batch-stage wrappers (proposed: `extract_monthly_batch`, `transform_monthly_batch`, `load_monthly_batch`) in `pipeline/runtime.py` or an equivalently cohesive runtime helper, using `split_request_by_month()` consistently. Do not duplicate request splitting in each CLI.
- [x] Keep `run_monthly_batch()` as the complete wrapper and preserve its existing per-month **extract → transform → load** order (rather than extracting all months first and changing scheduling/resume behavior).
- [x] Do **not** invoke other CLIs using subprocesses from `run_pipeline.py`; call shared runtime logic in-process.
- [x] Do not import Airflow into `pipeline/` or change Airflow task/DAG policy as part of this change. Verify its existing runtime call sites still work.
- [x] Preserve meaningful per-stage logging: request/partition identity, checkpoint status (created/resumed), input/output paths, and load processed/skipped outcome.
- [x] Avoid new CLI date arguments in this task; existing environment-driven period contract remains authoritative.
- [x] Keep the experimental monthly-climate aggregation CLI independent from this daily ETL work.

## Verification checklist

- [x] Add offline unit tests for each CLI's dispatch, environment validation, and errors.
- [ ] Verify extract does not invoke transform/load; transform does not invoke CDS/load; load does not invoke CDS/transform.
- [ ] Verify a missing upstream prerequisite exits nonzero without hidden automatic recovery.
- [x] Verify one-month, partial-month, and multi-month requests use the same normalized partition identities for all commands.
- [ ] Verify repeated valid RAW and Parquet stage runs are resume/no-overwrite operations; corrupt or stale checkpoints fail validation.
- [ ] Verify `run_pipeline` regression: same execution ordering and existing CLI contract.
- [x] Verify retry after a failure in a later partition preserves and reuses earlier successful checkpoints.
- [ ] Preserve existing scientific/provenance unit tests and Airflow runtime-boundary tests.
- [ ] Add or retain isolated PostgreSQL integration evidence for first load, unchanged rerun/skip, mismatched artifact metadata triggering UPSERT, no duplicates, and rollback on failure.
- [ ] Require all newly added stage-CLI tests and relevant regression tests to pass before marking implementation complete; failures must be fixed and rerun, not merely reported.
- [ ] Run relevant suites with quiet output, for example:
  ```bash
  source activate-airflow.sh -q
  ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q
  python -m pytest tests/integration/pipeline -q
  ```
- [ ] Record exact test commands, pass/fail counts, and any unexecuted integration coverage after implementation; do not claim passing tests before execution.
- [ ] Verify the published README commands agree with implemented `python -m` entry points and test coverage.

## Documentation and completion

- [x] Update root `README.md`, replacing “2. Run Pipeline Manually” with “2. Run Daily ETL”. **Show `python -m scripts.run_pipeline` first as the normal/default full workflow; only afterward describe `run_extract`, `run_transform`, and `run_load` as optional commands used when a specific stage is needed.** Include prerequisite/output semantics and clarify that no upstream stage is triggered implicitly.
- [x] Update `pipeline/README.md` entry points and stage prerequisites, referencing project-level commands without duplicating the full CLI guide.
- [x] Update the `AGENTS.md` CLI convention to recognize independent daily-stage entry points and complete wrapper, without duplicating architectural policy.
- [ ] Update this checklist as work is completed; record any agreed deviations and test evidence.
- [x] Modify **only branch `dev`**. Do not implement this design until the separately requested code-change step.

## Non-goals

No scientific formula/unit/timezone changes, database schema changes, alternative providers/ARCO, Airflow scheduling/pacing/retries changes, new dependencies, destructive resets, or production CDS/database execution as part of defining this plan.


## Implementation note (2026-10-23)

Stage CLI modules, monthly batch dispatch, and focused offline tests were added on `dev`. The full `run_monthly_batch()` logic remains unchanged. No CDS or PostgreSQL operation was initiated. **Pytest has not been executed in the project virtual environment here**; existing and new regression tests remain pending local verification before acceptance. Integration/idempotency checks are not marked passed.
