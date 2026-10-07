# Apache Airflow Implementation Plan

This document tracks the remaining work required to operate the ERA5-Land
historical campaign safely with Apache Airflow.

It follows the architecture and engineering contracts in
[`../../AGENTS.md`](../../AGENTS.md), the current pipeline behavior in
[`../../README.md`](../../README.md), and the Airflow-specific policies in:

- [`../AIRFLOW_BEST_PRACTICE.md`](../AIRFLOW_BEST_PRACTICE.md)
- [`../AIRFLOW_CODING_GUIDELINES.md`](../AIRFLOW_CODING_GUIDELINES.md)

Airflow orchestrates the existing pipeline. Scientific transformation,
validation, deterministic checkpoint behavior, request identity, artifact
layout, provenance, and canonical PostgreSQL persistence remain owned by
`pipeline/`.

This file is an implementation and rollout checklist. Detailed operating
commands belong in `orchestration/README.md`.

---

## 1. Current Scope

The active historical campaign is:

```text
dataset  : reanalysis-era5-land
period   : 2016-01-01 -> 2025-12-31
unit     : one calendar quarter per scheduled DagRun
months   : 120
quarters : 40
timezone : Asia/Jakarta
```

The active provider variables remain:

```text
tp
t2m
swvl1
ssrd
d2m
```

The configured Aceh bounding box remains:

```text
north : 6.1
south : 1.9
west  : 94.9
east  : 98.3
```

The historical campaign uses one DAG:

```text
cds_era5_land_etl
```

No yearly DAGs, quarterly DAG definitions, rolling ten-year window, ARCO path,
or separate ongoing-ingestion DAG are part of this campaign.

---

## 2. Current Architecture

The responsibility refactor supersedes older path references in previous
versions of this plan.

Current orchestration structure:

```text
orchestration/
├── config.py
├── campaign/
│   ├── policy.py
│   ├── repair.py
│   └── reporting.py
├── dags/
│   └── cds_era5_land_etl.py
├── monitoring/
├── operators/
│   └── raw_checkpoint.py
├── progress/
│   ├── definition.py
│   └── tracking.py
├── repositories/
│   ├── database.py
│   ├── load_evidence.py
│   ├── pipeline_progress.py
│   └── submission_pacing.py
├── scheduling/
│   ├── intervals.py
│   ├── requests.py
│   ├── run_mode.py
│   ├── schedule.py
│   └── submission_gate.py
└── tasks/
```

Operational documentation remains in `orchestration/README.md`. Project-wide
Airflow policy remains under `docs/`; there is no required
`orchestration/docs/` directory in the current layout.

The active mapped flow is:

```text
manual dates or scheduled quarter
              |
              v
     build_monthly_requests
              |
              v
     attempt_raw_checkpoint
              |
              v
     resolve_raw_checkpoint
              |
              v
ensure_transformed_checkpoint
              |
              v
             load
```

Validation remains inside pipeline runtime boundaries. NetCDF, Parquet, xarray,
and Pandas objects never cross XCom.

---

## 3. Completed Baseline — Stages 0–7

Stages 0–7 are complete and are no longer active implementation work.

### Stage 0 — Branch and baseline guard

- [x] Work only on branch `dev`.
- [x] Preserve scientific contracts and deterministic request/artifact identity.
- [x] Establish unit and isolated PostgreSQL integration baselines.

### Stage 1 — Airflow-neutral runtime handoff

- [x] JSON-safe request specifications.
- [x] Separate RAW, transformed, and load runtime boundaries.
- [x] Deterministic checkpoint rehydration across task processes.
- [x] Small control/status payloads only.
- [x] Core pipeline remains independent from Airflow imports.

### Stage 2 — Request and interval configuration

- [x] Preserve dataset, variables, timezone, and Aceh bounding box.
- [x] Explicit historical date validation.
- [x] Monthly deterministic request splitting.
- [x] Local-day interval handling remains pipeline compatible.

### Stage 3 — Initial manual ETL DAG

- [x] One DAG: `cds_era5_land_etl`.
- [x] Explicit manual start/end dates.
- [x] Dynamic monthly mapping.
- [x] Bounded retries and execution timeouts.
- [x] One-slot `cds_api` pool.
- [x] Request identity independent from Airflow run ID and wall-clock time.

The original `schedule=None` design was the manual baseline only. It is
superseded by the configurable campaign schedule described below.

### Stage 4 — Local Airflow runtime

- [x] Single project `requirements.txt`.
- [x] Repository-local Airflow runtime.
- [x] SQLite Airflow metadata retained for local development.
- [x] Shared file-backed Airflow JWT secret.
- [x] Runtime files and secrets excluded from Git.

### Stage 5 — Airflow operating documentation

- [x] Setup/start/stop/trigger/status guidance.
- [x] Manual and scheduled mode guidance.
- [x] Shared-filesystem assumption.
- [x] Safe retry/resume guidance.
- [x] Backup/restart guidance for PostgreSQL progress, pacing, and artifacts.

### Stage 6 — Tests

- [x] DAG import and graph tests.
- [x] Mapping, retry, timeout, pool, schedule, and mode tests.
- [x] Progress/reconciliation tests.
- [x] CDS submission pacing tests.
- [x] PostgreSQL integration coverage for persistence/progress/pacing.
- [x] Pipeline tests remain runnable without importing Airflow.

The exact test count is not a design contract. The latest local baseline must
be rerun before the one-year campaign pilot and recorded in the operator notes.

### Stage 7 — Controlled monthly and quarterly pilots

- [x] Fresh monthly run.
- [x] Same-month idempotent rerun.
- [x] Multi-month run.
- [x] Peak-RSS instrumentation.
- [x] Disk-space warning.
- [x] Live CDS pacing/deferral observed.
- [x] Manual recovery observed.
- [x] Persisted monthly evidence independently checked.

Historical live runs remain useful evidence, but runs that crossed DAG versions
are not accepted as proof of the current graph.

---

## 4. Stage 8 — Scheduled Historical Campaign

Stage 8 is the only active implementation/rollout stage.

The campaign contract is:

```text
scheduled DagRun
      |
      v
select earliest eligible incomplete quarter
      |
      v
map exactly three calendar months
      |
      v
persist/reconcile monthly evidence
      |
      v
complete quarter only when all 3 months are valid
```

Manual repair uses the same DAG and an explicit complete calendar month or
reviewed explicit range.

### 8.1 Completed campaign mechanics

- [x] Scheduled/manual mode is derived from DagRun type.
- [x] Scheduled selection uses the earliest eligible incomplete quarter.
- [x] Campaign identity is fixed to the configured 2016–2025 range.
- [x] Each automatic run maps exactly three whole calendar months.
- [x] `max_active_runs=1` prevents overlapping active DagRuns.
- [x] Progress is stored in PostgreSQL rather than inferred from Airflow history.
- [x] Parent completion requires verified persisted detail evidence.
- [x] Manual whole-month repair reconciles against the same campaign identity.
- [x] Atomic selection claims prevent competing selectors from taking the same
      quarter.
- [x] A terminal failed detail blocks automatic advancement and requires
      diagnosis/manual repair.
- [x] Scheduled monthly earliest-start lower bounds are +0h, +1h, and +2h.
- [x] Manual runs do not inherit scheduled monthly offsets.
- [x] Extraction uses a first pass followed by a durable first-pass barrier
      before retrying a failed month.
- [x] Retry count remains bounded.
- [x] CDS pacing uses a PostgreSQL global submission clock.
- [x] CDS pacing applies to actual POST submissions, including context chunks
      and retriable submission attempts.
- [x] Pacing deferral releases the worker.
- [x] Scheduler cadence and CDS submission interval are independently
      configurable.
- [x] When every configured quarter is complete, scheduled selection returns no
      monthly jobs; additional ticks are therefore safe no-ops.

### 8.2 Small observability gap before the one-year pilot

Existing logs already include selected quarter/months/mode, CDS
`reserved`/`deferred` decisions, timestamps, and bounded failure reasons.

Before the one-year campaign pilot, review whether one small DagRun-level
summary should be added. Do not add a large monitoring subsystem.

Desired completion summary:

```text
quarter
mode
months
DagRun elapsed time
quarter completion result
campaign completed/remaining quarter count
failed month/reason when applicable
```

Checklist:

- [ ] Confirm whether the existing Airflow/task logs plus
      `scripts/report_campaign_progress.py` are already sufficient for the
      one-year pilot.
- [ ] If not sufficient, add only a small completion-summary log at the
      orchestration boundary. Do not duplicate the campaign report in every
      mapped task and do not put the full report in XCom.
- [ ] Add/adjust focused offline log assertions only if logging behavior changes.

This observability review must not delay the pilot unless a missing signal would
prevent us from deciding whether a quarter safely completed.

### 8.3 Scheduler cadence evaluation

Scheduler cadence and provider submission pacing remain separate controls:

```text
ERA5_CAMPAIGN_SCHEDULE_CADENCE
    = how often Airflow creates a scheduled campaign opportunity

SUBMISSION_INTERVAL_SECONDS
    = minimum spacing between actual CDS submission POSTs
```

Current supported campaign cadences are 30 minutes, 1 hour, and 2 hours.
The current default is 2 hours. The CDS submission interval default is 1800
seconds.

A previous clean 2016 Q1 run took approximately 2h 45m 52s. One observation is
not enough to choose a final cadence.

For the one-year pilot:

- [ ] Keep `max_active_runs=1`.
- [ ] Start with the currently reviewed cadence unless the clean pre-pilot
      evidence shows a clear operational problem.
- [ ] Record scheduled tick time, actual DagRun start/end, queue delay, and
      quarter elapsed time for each of the four quarters.
- [ ] Record CDS submission timestamps, deferrals, provider errors, and retries.
- [ ] After at least four clean quarter observations, decide whether the
      campaign cadence should remain unchanged or be extended.
- [ ] If a cadence longer than the currently supported set is justified,
      change it as a separate reviewed orchestration configuration task; do not
      change scientific contracts or progress identity.

Cadence evaluation is an operational decision, not a prerequisite for
scientific correctness.

---

## 5. Stage 8A — Clean Current-Graph Scheduled Pilot

Before running a full calendar year, execute one clean scheduled quarter under
one unchanged DAG version.

This replaces older duplicated "real scheduled pilot" checklist items.

Preconditions:

- [ ] All previous DagRuns are terminal.
- [ ] No DAG graph deployment occurs while the pilot is active.
- [ ] Scheduling state, progress state, pacing state, disk headroom, and
      PostgreSQL connectivity are inspected.
- [ ] Unit/orchestration tests and selected PostgreSQL integration tests pass.
- [ ] The target quarter is the earliest incomplete quarter for the configured
      campaign state.

Verify during/after the pilot:

- [ ] Three monthly jobs are mapped for one quarter.
- [ ] Scheduled earliest-start lower bounds are +0h/+1h/+2h.
- [ ] Provider POST timestamps respect the durable pacing gate.
- [ ] Context chunks and target chunks resume correctly after deferral.
- [ ] The first-pass barrier behaves as designed.
- [ ] Any retry/resume behavior is bounded and checkpoint-safe.
- [ ] Transformation and load complete for all successful monthly partitions.
- [ ] PostgreSQL observation counts match progress evidence.
- [ ] Artifact paths/checksums match persisted load evidence.
- [ ] Campaign report marks the quarter complete only after all three months
      verify.
- [ ] No later quarter is skipped.
- [ ] Record total DagRun elapsed time and queue/deferral observations.

Exit condition: one quarter completes under the current DAG version with
Airflow state, PostgreSQL progress, persisted climate data, and artifact
evidence all agreeing.

---

## 6. Stage 8B — One-Year Campaign Pilot

After Stage 8A succeeds, the next rollout gate is a one-calendar-year campaign.

### Pilot scope

Use the first campaign year:

```text
2016-01-01 -> 2016-12-31
```

The pilot is **not** one manual twelve-month DagRun. It must exercise normal
scheduled campaign behavior across four quarter selections:

```text
2016 Q1 -> Jan, Feb, Mar
2016 Q2 -> Apr, May, Jun
2016 Q3 -> Jul, Aug, Sep
2016 Q4 -> Oct, Nov, Dec
```

If campaign state already contains verified 2016 partitions, decide explicitly
whether to retain that evidence or recreate/reset the development campaign
state before starting this pilot. Do not silently mix old and new pilot
evidence.

### One-year pilot checklist

Before enabling the four-quarter run:

- [ ] Stage 8A clean scheduled pilot is complete.
- [ ] Decide and record whether the pilot uses clean/recreated campaign state or
      intentionally retained verified state.
- [ ] Record initial campaign report.
- [ ] Record initial capacity snapshot.
- [ ] Confirm sufficient artifact/log disk headroom.
- [ ] Confirm PostgreSQL backup/recovery procedure is understood.
- [ ] Freeze DAG graph changes for the duration of an active DagRun.

For each quarter:

- [ ] Confirm the selected quarter is the earliest incomplete quarter.
- [ ] Record mapped months.
- [ ] Record DagRun start/end and elapsed time.
- [ ] Record important CDS pacing decisions and provider failures/retries.
- [ ] Confirm all three monthly transformed/load results against PostgreSQL.
- [ ] Confirm quarter progress becomes complete only after persisted evidence.
- [ ] If a terminal month fails, stop automatic advancement, diagnose, perform
      bounded manual repair, reconcile, then continue.
- [ ] Do not clear durable progress or overwrite RAW data as a retry strategy.

After 2016 Q4:

- [ ] Campaign report shows all four 2016 quarters complete.
- [ ] All 12 expected monthly partitions have valid persisted evidence.
- [ ] No month is duplicated by request identity.
- [ ] Observation counts agree with the corresponding progress detail rows.
- [ ] Transformed artifact paths/checksums agree with persisted load metadata.
- [ ] Review disk growth, PostgreSQL growth, task memory observations, queue
      delay, CDS errors/retries, and quarter elapsed times.
- [ ] Complete the scheduler cadence decision using the four-quarter evidence.
- [ ] Record any manual intervention required during the year and whether the
      recovery procedure was sufficient.

Exit condition: one full calendar year completes through normal quarterly
scheduled operation and can be independently reconstructed from artifacts,
PostgreSQL observations, and durable campaign progress.

---

## 7. Stage 8C — Full 2016–2025 Campaign

Do not enable the remaining campaign simply because Stage 8A passes. Stage 8B
is the rollout gate.

Preconditions:

- [ ] Stage 8B one-year campaign pilot passes.
- [ ] Scheduler cadence decision is recorded.
- [ ] Remaining observability is sufficient for unattended operation.
- [ ] Capacity observations do not show an unresolved local storage/memory
      blocker.
- [ ] Recovery procedure has been exercised at least once if a real failure
      occurred during the one-year pilot.

Then continue the same campaign policy from the earliest incomplete quarter
through 2025 Q4.

During the campaign:

- [ ] Track completed/remaining/failed quarters.
- [ ] Track failed months and manual repairs.
- [ ] Track last successful persisted load.
- [ ] Track quarter elapsed time and meaningful provider error/retry behavior.
- [ ] Periodically inspect artifact disk, PostgreSQL size, and Airflow runtime
      health.
- [ ] Do not deploy a changed DAG graph into an active DagRun.

Final verification:

- [ ] All 40 configured quarters are complete.
- [ ] All 120 expected monthly partitions are represented by valid persisted
      evidence.
- [ ] No failed campaign details remain.
- [ ] Expected database partitions and observation counts agree with progress.
- [ ] Additional scheduled ticks map no work.
- [ ] Pause the historical campaign after final verification and record the
      completion state.

Exit condition: the bounded 2016–2025 historical campaign is complete,
reconciled, recoverable, and no new period beyond 2025 can be inferred from
the wall clock.

---

## 8. Explicitly Deferred — Future Ongoing Ingestion

Ongoing ingestion after 2025 is not part of the historical campaign and must
not be designed while the one-year rollout gate is still unresolved.

Deferred decisions:

- [ ] Define the relationship between future ingestion intervals and the
      completed-local-day availability lag.
- [ ] Decide whether the existing DAG mode contract should be extended or a
      different design is justified.
- [ ] Define freshness and last-success observability.
- [ ] Define bounded alert/retry behavior for new periods.
- [ ] Test delayed availability, resume, and idempotency.

No second DAG is authorized by this section.

---

## 9. Explicit Non-Goals

- No separate Airflow validation tasks.
- No DataFrame, xarray Dataset, NetCDF bytes, or Parquet bytes in XCom.
- No scientific transformation or canonical climate SQL persistence in the DAG.
- No Airflow imports in `pipeline/`.
- No scheduler catchup as the historical bootstrap mechanism.
- No unpaced 120-month DagRun.
- No 40 quarterly DAG definitions.
- No ten yearly DAG definitions.
- No implicit rolling ten-year range.
- No distributed workers with isolated local artifact filesystems.
- No restoration of inactive ARCO paths.
- No new monitoring platform merely to run the one-year pilot.

---

## 10. Immediate Work Order

The current work order is intentionally short:

```text
1. reconcile this implementation plan              DONE
2. review only the small observability gap          NEXT
3. run one clean current-graph scheduled quarter    NEXT
4. run the 2016 one-year / four-quarter pilot       NEXT
5. evaluate cadence from four-quarter evidence
6. continue 2017-2025 only after the one-year gate
7. design ongoing ingestion only after campaign evidence exists
```

Do not reopen completed Stages 0–7 unless a regression provides concrete
evidence that one of their contracts no longer holds.
