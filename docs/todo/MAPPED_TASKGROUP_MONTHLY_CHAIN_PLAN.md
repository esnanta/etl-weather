# Monthly Mapped TaskGroup Refactor Plan

This document prepares the orchestration-only refactor from stage-level dynamic
mapping to a **Mapped TaskGroup per calendar month**.

The purpose is to let one month continue through transformation and load as soon
as its RAW checkpoint is ready, while other months may still be waiting for
scheduled earliest-start limits, CDS submission pacing, provider queueing, or a
bounded extraction retry.

This is a planning and implementation checklist. It does not authorize changes
to scientific ETL behavior.

---

## 1. Authority and Preconditions

This work must follow, in order:

1. [`../../AGENTS.md`](../../AGENTS.md)
2. [`../../tests/AGENTS.md`](../../tests/AGENTS.md)
3. [`../AIRFLOW_BEST_PRACTICE.md`](../AIRFLOW_BEST_PRACTICE.md)
4. [`../AIRFLOW_CODING_GUIDELINES.md`](../AIRFLOW_CODING_GUIDELINES.md)

The requested repository-local reference `orchestration/AGENTS.md` is not
present on branch `dev` at the time this plan is created. Do not invent rules
for that missing file.

Precondition checklist:

- [x] Read and apply the current root `AGENTS.md`.
- [x] Read and apply `tests/AGENTS.md`.
- [x] Read the current Airflow best-practice and coding-guideline documents.
- [x] Confirm the current DAG still maps each stage separately.
- [x] Confirm the active DAG uses `max_active_runs=1`,
      `max_active_tasks=2`, and the one-slot `cds_api` pool.
- [x] `orchestration/AGENTS.md` is intentionally not required for this refactor;
      orchestration rules are covered by root `AGENTS.md` plus the Airflow
      best-practice/coding-guideline documents.
- [ ] Freeze DAG graph deployment while any current DagRun is active.

Only branch `dev` may be modified for this work.

---

## 2. Problem Statement

The current mapped graph is stage-oriented:

```text
build_monthly_requests
        |
        v
attempt_raw_checkpoint [Jan, Feb, Mar]
        |
        v
resolve_raw_checkpoint [Jan, Feb, Mar]
        |
        v
ensure_transformed_checkpoint [Jan, Feb, Mar]
        |
        v
load [Jan, Feb, Mar]
```

This structure makes the quarter observable by stage rather than by independent
monthly chain. A month that already has a valid RAW checkpoint should not need
to remain operationally idle merely because another mapped month is still
waiting for its scheduled start or CDS pacing window.

The target behavior is depth-first by calendar month:

```text
build_monthly_requests
        |
        +-------------------+-------------------+
        |                   |                   |
        v                   v                   v
 monthly_etl[Jan]       monthly_etl[Feb]       monthly_etl[Mar]
        |                   |                   |
 attempt RAW            attempt RAW            attempt RAW
        |                   |                   |
 resolve RAW            resolve RAW            resolve RAW
        |                   |                   |
 transform              transform              transform
        |                   |                   |
 load                   load                   load
```

Expected operational result:

```text
January RAW complete
        |
        +--> January transform
                 |
                 +--> January load

while February or March may still be deferred for:
- earliest-start policy;
- CDS submission pacing;
- provider queueing;
- first-pass retry coordination.
```

---

## 3. Locked Contracts — Must Not Change

This refactor is orchestration-only.

- [ ] Do not change ERA5-Land variables, units, UTC+7 daily semantics, or
      accumulation formulas.
- [ ] Do not change the production bounding box or request identity.
- [ ] Do not change RAW or transformed artifact naming/layout.
- [ ] Do not overwrite RAW checkpoints.
- [ ] Do not bypass RAW/transformed validation on resume.
- [ ] Do not change PostgreSQL climate schema or idempotency semantics.
- [ ] Do not move scientific transformation, validation, storage, or SQL
      persistence into the DAG.
- [ ] Do not introduce Airflow imports into `pipeline/`.
- [ ] Do not pass DataFrame, xarray, NetCDF, Parquet bytes, or other large
      objects through XCom.
- [ ] Do not make `run_id`, map index, retry number, or wall-clock time part of
      request/artifact identity.
- [ ] Preserve scheduled/manual mode behavior and durable campaign progress.
- [ ] Preserve the PostgreSQL submission pacing gate.
- [ ] Preserve bounded extraction retry and manual-repair behavior.

---

## 4. Design Decision

Use one dynamically mapped TaskGroup for one monthly request specification.

Conceptual shape:

```text
@task_group
monthly_etl(monthly_job)
    |
    +-- attempt_raw_checkpoint
    |
    +-- resolve_raw_checkpoint
    |
    +-- ensure_transformed_checkpoint
    |
    +-- load
```

Then map the group over the three monthly jobs selected for the quarter.

The goal is **depth-first monthly execution**, not higher CDS concurrency.

The one-slot `cds_api` pool remains the provider-protection boundary.
Transformation and load are allowed to use available DAG capacity while another
month's extraction is deferred.

Checklist:

- [x] Select Mapped TaskGroup per month as the preferred design.
- [x] Confirm the exact Airflow 3 API syntax supported by the pinned dependency.
      `apache-airflow==3.3.2` supports `from airflow.sdk import task_group` and
      dynamic TaskGroup mapping with `@task_group` plus `.expand(...)`.
- [ ] Keep the DAG declaration thin; extract helper construction only when it
      improves readability and remains orchestration-specific.
- [ ] Keep task IDs stable where practical for operator familiarity.
- [ ] If TaskGroup qualification changes full task IDs, document the change and
      update tests/operational examples deliberately.
- [ ] Preserve `map_index_template` or provide an equally clear YYYY-MM
      observability label.

---

## 5. Extraction and Retry Semantics

The current extraction contract has two orchestration stages:

```text
attempt_raw_checkpoint
        |
        v
resolve_raw_checkpoint
```

The first pass has no Airflow retry. A failed first pass is represented as
small control metadata. The resolver may wait for the durable first-pass
barrier before retrying the failed month.

This contract must survive the TaskGroup refactor.

Checklist:

- [ ] Keep `attempt_raw_checkpoint` with `retries=0`.
- [ ] Keep `resolve_raw_checkpoint` as the bounded retry boundary.
- [ ] Preserve deferrable waits for scheduled earliest-start time.
- [ ] Preserve deferrable waits for CDS submission pacing.
- [ ] Preserve worker-free first-pass barrier polling.
- [ ] A successful first attempt must pass through its resolver immediately.
- [ ] A failed month must not stop other months from completing their first
      extraction attempts.
- [ ] A failed month may retry only under the existing durable barrier contract.
- [ ] Terminal extraction failure must remain durable progress evidence and
      block unsafe campaign advancement.

Important validation question during implementation:

> Does the existing quarter-wide first-pass barrier still provide the intended
> fairness once each month is inside an independently executing mapped
> TaskGroup?

Do not remove or weaken the barrier merely to gain speed. Any change to the
barrier requires separate evidence and tests.

---

## 6. Transformation and Load Concurrency

The refactor should make useful local work happen while provider work waits.

Desired behavior:

```text
cds_api pool slot
    -> only protects provider retrieval

available DAG task capacity
    -> may run transformation or load for a RAW-complete month
```

Initial concurrency policy remains conservative:

```text
max_active_runs = 1
max_active_tasks = 2
cds_api pool    = 1
```

Checklist:

- [ ] Keep `max_active_runs=1`.
- [ ] Keep `max_active_tasks=2` for the first TaskGroup pilot.
- [ ] Keep `cds_api` pool size at one.
- [ ] Verify one transform/load can execute while another month's RAW task is
      deferred or waiting.
- [ ] Do not increase concurrency until memory, disk, PostgreSQL, and provider
      behavior are measured under the new graph.
- [ ] Confirm load retry remains bounded and continues to reuse the validated
      transformed checkpoint.

---

## 7. Monthly Earliest-Start Policy

The current scheduled policy assigns:

```text
month 1 -> +0 minutes
month 2 -> +15 minutes
month 3 -> +30 minutes
```

This TaskGroup refactor and the earliest-start policy are related but distinct
changes.

The TaskGroup graph was first implemented and verified independently. After
observing approximately 11 minutes for one monthly RAW retrieval and a 300-second
provider submission interval, monthly spacing is now treated as a separate
reviewed optimization.

- [x] Keep the TaskGroup refactor independent from the initial spacing change.
- [x] Use the observed monthly retrieval duration as evidence for reducing idle
      time.
- [x] Replace hour-based spacing with explicit minute-based configuration.
- [x] Set scheduled monthly lower bounds to +0, +15, and +30 minutes.
- [x] Keep `SUBMISSION_INTERVAL_SECONDS` independent from monthly start slots.
- [x] Verify the 15-minute slot policy with unit tests: 31 focused tests and
      278 full unit tests passed.
- [ ] Measure quarter elapsed time and deferral behavior during the clean pilot.
- [ ] Revisit the 15-minute value only after pilot evidence shows it is too
      conservative or too aggressive.

This separation makes it possible to determine whether elapsed-time improvement
comes from dependency restructuring or from a pacing/configuration change.

---

## 8. Code Areas Expected to Change

Expected orchestration files:

- [x] `orchestration/dags/cds_era5_land_etl.py`
      - introduce the monthly TaskGroup;
      - map the group over monthly jobs;
      - preserve explicit retry, timeout, pool, and concurrency policy.
- [ ] Small orchestration helper module only if needed to keep the DAG thin.
- [x] Existing RAW operator changed only as required for TaskGroup mapping:
      mapped references are template-resolvable and request/month validation
      occurs during task execution rather than DAG parsing.

Expected test files:

- [ ] `tests/unit/orchestration/test_dag.py`
      - update graph assertions for TaskGroup-qualified tasks;
      - prove monthly depth-first dependency structure;
      - retain policy assertions for pool/retry/timeouts/concurrency.
- [ ] Add/update the narrowest orchestration unit tests needed for mapping
      payloads or helper behavior.
- [ ] Keep required module-level test docstrings if test modules are materially
      revised.

Files that should not change unless a concrete blocker is discovered:

- [ ] `pipeline/`
- [ ] scientific tests under `tests/unit/pipeline/`
- [ ] PostgreSQL climate persistence schema
- [ ] artifact path/identity logic
- [ ] campaign progress identity
- [ ] submission pacing repository semantics

If a blocker appears to require `pipeline/` changes, stop and review that
change separately before implementation.

---

## 9. Automated Test Plan

### 9.1 DAG structure

- [ ] DAG imports without external I/O.
- [ ] Exactly one monthly TaskGroup definition is mapped over selected months.
- [ ] Each mapped month has the chain:
      `attempt -> resolve -> transform -> load`.
- [ ] No cross-month dependency forces January transform/load to wait for
      February or March RAW completion.
- [ ] Extraction tasks remain in `cds_api`.
- [ ] `max_active_runs=1`.
- [ ] `max_active_tasks=2`.
- [ ] Task execution timeouts remain explicit.
- [ ] Retry policy remains explicit by boundary.

### 9.2 Mapping and XCom

- [ ] Monthly mapping receives only small JSON-safe job metadata.
- [ ] Resolver receives only the small first-attempt result/control metadata.
- [ ] Transformation receives checkpoint/control metadata, never the dataset.
- [ ] Load receives transformed checkpoint/control metadata, never a DataFrame.
- [ ] Request identity is unchanged by TaskGroup/map identity.

### 9.3 Retry and deferral

- [ ] Scheduled earliest-start waits still defer without holding a worker.
- [ ] CDS pacing waits still defer without holding a worker.
- [ ] Successful first RAW attempt reaches downstream work without waiting for a
      failed sibling month's retry.
- [ ] Failed first-pass month waits for the intended durable barrier.
- [ ] Resolver retry remains bounded.
- [ ] Load retry remains bounded.

### 9.4 Regression gates

Run at minimum:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

and the isolated PostgreSQL orchestration/persistence integration set documented
in `orchestration/README.md`.

- [x] Unit suite passes after runtime-reset enhancement: 283 passed.
- [x] PostgreSQL integration suite passes: 9 passed.
- [ ] No new unexpected warnings.
- [ ] Core pipeline tests still run without importing Airflow.

Do not treat a successful live Airflow run as a substitute for these tests.

---

## 10. Documentation Updates After Code Is Proven

The current Airflow policy documentation describes the stage-mapped linear
graph. After the implementation and tests pass:

- [x] Update `docs/AIRFLOW_BEST_PRACTICE.md` to describe the monthly
      depth-first TaskGroup while retaining the same runtime boundaries.
- [x] Update `docs/AIRFLOW_CODING_GUIDELINES.md` only where the graph/task
      naming or mapping guidance was stale.
- [x] Update `orchestration/README.md` task-flow and operational examples.
- [x] Update `docs/todo/IMPLEMENTATION_PLAN.md` so Stage 8A acceptance criteria
      match the new graph.
- [x] Do not duplicate scientific contracts into Airflow documentation.

---

## 11. Clean Pilot Acceptance Criteria

Do not deploy this graph into an active DagRun.

Before pilot:

- [x] All previous DagRuns are terminal; reset preflight reported queued=0 and running=0.
- [x] Unit and integration gates pass: 283 unit tests and 9 selected PostgreSQL integration tests.
- [x] Campaign/progress/pacing state was inspected before reset; clean reset then recreated PostgreSQL and cleared data/log/runtime state.
- [ ] DAG version is frozen for the pilot.

During one scheduled quarter:

- [ ] Three mapped monthly TaskGroups are created for the selected quarter.
- [ ] January can transform/load immediately after its RAW checkpoint is ready,
      even while February/March extraction is deferred or waiting.
- [ ] The `cds_api` pool never permits concurrent provider retrieval beyond its
      configured slot count.
- [ ] Provider POSTs still obey durable pacing.
- [ ] A deferred RAW task does not consume a worker slot.
- [ ] A successful month is not held behind an unrelated failed/waiting sibling
      month except where the explicit first-pass barrier contract requires it.
- [ ] PostgreSQL progress and persisted climate evidence remain correct.
- [ ] Campaign quarter becomes complete only after all three months are valid.
- [ ] Record DagRun elapsed time and compare it with the previous stage-mapped
      behavior.

Exit condition:

> The quarter completes with depth-first monthly ETL, unchanged scientific and
> persistence contracts, valid pacing/retry behavior, and measurably reduced
> unnecessary idle time.

---

## 12. Implementation Order

Use this sequence and mark each item as it is completed:

- [x] Approve Mapped TaskGroup per month as the target architecture.
- [x] Create this implementation plan before editing scripts.
- [x] Treat root `AGENTS.md` and Airflow policy documents as the orchestration
      authority; no separate `orchestration/AGENTS.md` is required.
- [x] Confirm pinned Airflow TaskGroup dynamic-mapping API and current graph
      behavior.
- [x] Update focused DAG-structure tests alongside the graph change.
- [x] Refactor the DAG to a mapped monthly TaskGroup.
- [x] Make only the minimum wrapper/helper changes required: RAW mapped inputs
      are resolved/validated at task runtime so TaskGroup references remain
      parse-safe.
- [x] Run focused orchestration tests: 13 passed.
- [x] Run the full unit regression gate: 278 passed.
- [x] Run selected PostgreSQL integration tests: 9 passed.
- [x] Review diff for accidental pipeline/scientific changes; no `pipeline/`
      or scientific-contract files changed.
- [x] Update Airflow operational/policy documentation.
- [ ] Start a clean scheduled-quarter pilot under one unchanged DAG version.
- [ ] Verify depth-first January transform/load while later months wait.
- [ ] Record elapsed-time and resource observations.
- [x] Review monthly start spacing as a separate optimization after unit and
      integration graph gates passed; adopt +0/+15/+30 minutes for the pilot.
- [ ] Continue to the one-year/four-quarter pilot only after this gate passes.

---

## 13. Definition of Done

This refactor is complete only when all of the following are true:

- [ ] Each calendar month is an independently progressing mapped TaskGroup.
- [ ] A RAW-complete month can transform and load without waiting for unrelated
      sibling months.
- [ ] Existing provider pacing, checkpoint, retry, validation, progress, and
      persistence contracts remain intact.
- [ ] No scientific behavior changed.
- [ ] Automated unit and integration gates pass.
- [ ] Documentation describes the new graph accurately.
- [ ] One clean scheduled quarter proves the behavior on the active DAG version.
- [ ] The result is ready for the 2016 one-year campaign pilot.
