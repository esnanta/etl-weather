# Quarter Cooldown Refactor Plan

## 1. Purpose

Refactor scheduled ERA5-Land campaign orchestration so the next campaign quarter
cannot start immediately after the previous quarter completes.

The target policy is:

```text
quarter N starts
    ↓
three mapped months run depth-first
    ↓
all expected month partitions complete and validate
    ↓
quarter N becomes complete
    ↓
wait quarter cooldown
    ↓
scheduler tick checks eligibility
    ↓
quarter N+1 may be selected
```

This is an orchestration-only change. It must not change ERA5-Land retrieval
semantics, scientific transformation, artifact identity, PostgreSQL climate
persistence, or request-key behavior.

---

## 2. Current Behavior

The current scheduler cadence is clock-based.

For example:

```env
ERA5_CAMPAIGN_SCHEDULE_CADENCE=45m
```

means Airflow creates scheduled campaign opportunities every 45 minutes. It
does **not** mean "wait 45 minutes after one quarter finishes."

Inside one selected quarter, mapped months currently use:

```python
MONTHLY_SLOT_SPACING_MINUTES = 15
```

so a quarter selected at 21:00 gets earliest-start lower bounds:

```text
month 1 → 21:00
month 2 → 21:15
month 3 → 21:30
```

Provider submissions are independently constrained by:

```env
SUBMISSION_INTERVAL_SECONDS=300
```

which means at least five minutes between actual CDS submission POST requests.

These three clocks are independent today.

---

## 3. Target Behavior

Keep the three existing timing responsibilities separate and add a fourth,
quarter-level eligibility rule.

Approved pilot policy:

```text
CAMPAIGN_SCHEDULE_INTERVAL_MINUTES = 10 minutes
QUARTER_COOLDOWN_MINUTES           = 10 minutes
MONTHLY_SLOT_SPACING_MINUTES       = 15 minutes
SUBMISSION_INTERVAL_MINUTES        = 5 minutes
```

Interpretation:

```text
provider pacing
    → minimum interval between CDS POST requests

monthly slot spacing
    → earliest-start offsets for the three months inside one quarter

quarter cooldown
    → minimum delay after a completed quarter before the next quarter is eligible

scheduler cadence
    → how often Airflow checks whether an eligible quarter exists
```

Example:

```text
Q1 selected at 21:00

21:00  January earliest-start
21:15  February earliest-start
21:30  March earliest-start

21:47  Q1 becomes complete + valid
21:57  Q1 cooldown expires

22:00  next scheduler tick
       Q2 becomes eligible and may be selected
```

Q2 must never be selected before Q1 is complete and its cooldown has expired.

---

## 4. Invariants That Must Not Change

- [ ] Keep one mapped monthly TaskGroup per month.
- [ ] Keep depth-first monthly execution:
      `attempt_raw_checkpoint -> resolve_raw_checkpoint ->
      ensure_transformed_checkpoint -> load`.
- [ ] Keep `max_active_runs=1`.
- [ ] Keep `max_active_tasks=2`.
- [ ] Keep the one-slot `cds_api` pool.
- [ ] Keep `MONTHLY_SLOT_SPACING_MINUTES=15` unless separately approved.
- [ ] Keep `SUBMISSION_INTERVAL_SECONDS` semantics unchanged.
- [ ] Keep provider pacing durable in PostgreSQL.
- [ ] Keep campaign progress identity cadence-neutral.
- [ ] Keep manual repair behavior unchanged.
- [ ] Keep failed quarter behavior: a failed partition blocks automatic campaign
      advancement until repaired/reconciled.
- [ ] Do not modify `pipeline/`.
- [ ] Do not change scientific aggregation, validation, artifact naming, or
      persistence behavior.

---

## 5. Existing Data We Can Reuse

The parent `pipeline_progress` record already contains:

```text
execution_status
validation_status
completed_at
selection_owner
selection_claimed_at
```

No new PostgreSQL table or column should be required for quarter cooldown.

The cooldown source of truth should be the previous campaign quarter's durable
parent progress record:

```text
execution_status == "complete"
validation_status == "valid"
completed_at IS NOT NULL
```

The next quarter becomes time-eligible only when:

```text
now >= previous_quarter.completed_at + QUARTER_COOLDOWN_MINUTES
```

Do not derive the cooldown from Airflow task end times or DagRun end times.
PostgreSQL campaign progress remains the durable campaign truth.

---

## 6. Configuration Design

### 6.1 Source-controlled timing policy

All duration/interval policy is owned by root `config.py`:

```python
CAMPAIGN_SCHEDULE_INTERVAL_MINUTES = 10
QUARTER_COOLDOWN_MINUTES = 10
MONTHLY_SLOT_SPACING_MINUTES = 15
SUBMISSION_INTERVAL_MINUTES = 5
```

`ERA5_CAMPAIGN_SCHEDULE_ENABLED` remains an environment switch because it
controls whether scheduled mode is enabled, not how long a timing interval is.

- [x] Set `CAMPAIGN_SCHEDULE_INTERVAL_MINUTES = 10`.
- [x] Set `QUARTER_COOLDOWN_MINUTES = 10`.
- [x] Keep `MONTHLY_SLOT_SPACING_MINUTES = 15`.
- [x] Set `SUBMISSION_INTERVAL_MINUTES = 5`.
- [x] Remove scheduler cadence and provider pacing duration overrides from
      `env-example`.
- [x] Keep timing policy source-controlled unless configuration policy is
      separately changed.

A quarter completing at 21:47 becomes eligible at 21:57 and will normally be
picked by the first 10-minute scheduler tick at or after 21:57.

---

## 7. Selection Policy Refactor

Current selection walks campaign quarters and returns the earliest incomplete,
eligible quarter.

Refactor `select_earliest_incomplete_quarter(...)` so sequential campaign
quarters obey predecessor completion + cooldown.

### Eligibility rules

For the first campaign quarter:

- [ ] No predecessor cooldown applies.
- [ ] Existing availability-lag policy still applies.
- [ ] Existing claim/failure rules still apply.

For every later quarter:

- [ ] Resolve the immediately preceding campaign quarter.
- [ ] Reconcile predecessor progress before evaluating advancement.
- [ ] Require predecessor `execution_status == "complete"`.
- [ ] Require predecessor `validation_status == "valid"`.
- [ ] Require predecessor `completed_at is not None`.
- [ ] Compute:
      `eligible_at = predecessor.completed_at + cooldown`.
- [ ] If current time is before `eligible_at`, return no selection without
      claiming the next quarter.
- [ ] Once cooldown expires, continue with existing availability, failure, and
      claim rules.
- [ ] Never skip over an incomplete or invalid predecessor to select a later
      quarter.

### Failure behavior

- [ ] A predecessor with failed partitions continues to produce the existing
      manual-repair block.
- [ ] A predecessor still running/pending must block advancement without
      treating it as an error.
- [ ] Cooldown waiting must not mark any progress row failed.
- [ ] Cooldown waiting must not create a claim on the next quarter.
- [ ] Repeated scheduler ticks during cooldown must be idempotent.

---

## 8. Time Semantics

Cooldown calculations must use timezone-aware timestamps.

- [ ] Use the durable PostgreSQL `completed_at` timestamp.
- [ ] Compare against a timezone-aware current instant.
- [ ] Do not use local wall-clock string arithmetic.
- [ ] Keep existing `Asia/Jakarta` campaign semantics for reporting/display.
- [ ] Store/compare instants without introducing DST or naive-datetime behavior.

For deterministic unit tests, selection should support an injected current time
or equivalent pure helper rather than relying only on wall-clock time.

---

## 9. Observability

Add explicit logs so operators can tell the difference between scheduler
cadence, quarter cooldown, monthly earliest-start, and provider pacing.

Recommended messages:

```text
Campaign quarter cooldown active |
previous_quarter=2016 Q1 |
completed_at_local=... |
cooldown=10m |
eligible_at_local=... |
wait=7m 12s
```

When cooldown has expired:

```text
Campaign quarter cooldown satisfied |
previous_quarter=2016 Q1 |
next_quarter=2016 Q2 |
eligible_at_local=...
```

Selection remains:

```text
Selected scheduled campaign quarter 2016 Q2 |
mode=scheduled |
progress_id=... |
months=2016-04, 2016-05, 2016-06
```

- [ ] Reuse the human-readable duration helper where practical.
- [ ] Keep Airflow's own trigger/scheduler logs unchanged.
- [ ] Do not log secrets or CDS credentials.

---

## 10. Unit Tests

### Configuration

- [ ] `QUARTER_COOLDOWN_MINUTES == 10`.
- [ ] New scheduler cadence value resolves to the intended schedule.
- [ ] Existing cadence values remain valid.
- [ ] Invalid cadence still fails explicitly.

### Campaign selection

Add focused tests for:

- [ ] First campaign quarter can be selected without predecessor cooldown.
- [ ] Q2 cannot be selected while Q1 is pending.
- [ ] Q2 cannot be selected while Q1 is running.
- [ ] Q2 cannot be selected while Q1 is failed.
- [ ] Q2 cannot be selected when Q1 is complete but invalid.
- [ ] Q2 cannot be selected when Q1 is complete/valid but `completed_at` is
      missing.
- [ ] Q2 cannot be selected at `completed_at + 9m59s` for a 10-minute
      cooldown.
- [ ] Q2 becomes eligible exactly at `completed_at + 10m`.
- [ ] Q2 remains eligible after the cooldown boundary.
- [ ] A cooldown-blocked scheduler tick does not claim Q2.
- [ ] Repeated cooldown-blocked ticks remain idempotent.
- [ ] Q3 cannot skip Q2.
- [ ] Existing failed-quarter manual repair behavior remains unchanged.
- [ ] Existing availability-lag behavior remains unchanged.

### Monthly behavior regression

- [ ] Scheduled quarter still maps exactly three months.
- [ ] Earliest starts remain `+0/+15/+30`.
- [ ] Manual runs still have no scheduled monthly earliest-start.
- [ ] Depth-first task dependencies remain unchanged.
- [ ] Provider pacing behavior remains unchanged.

---

## 11. PostgreSQL Integration Tests

No schema migration is expected because `pipeline_progress.completed_at`
already exists.

Add/extend integration coverage to prove durable cooldown behavior:

- [ ] Reconcile a quarter to complete + valid and verify `completed_at`.
- [ ] Before cooldown expiry, next quarter is not selected.
- [ ] At/after cooldown expiry, next quarter can be selected.
- [ ] Restart/recreate application objects without losing cooldown truth.
- [ ] A repaired quarter uses its reconciled durable completion timestamp.
- [ ] Concurrent selection cannot claim the same next quarter twice.
- [ ] Cooldown logic does not alter climate-data idempotency.

---

## 12. Documentation Updates

After implementation passes tests:

- [ ] Update `orchestration/README.md` timing section.
- [ ] Clearly distinguish four timing controls:

```text
scheduler cadence
quarter cooldown
monthly slot spacing
CDS submission pacing
```

- [ ] Add one 2016 Q1 -> Q2 worked timeline.
- [ ] Explain that Q2 cannot begin before Q1 is complete + valid.
- [ ] Explain that depth-first execution still applies inside Q1.
- [ ] Explain that deferred monthly tasks are expected and do not occupy a
      worker while waiting.
- [ ] Update `env-example` allowed cadence values only if scheduler cadence
      support changes.
- [ ] Keep `QUARTER_COOLDOWN_MINUTES` documented as a root-config constant if
      it remains source-controlled.

---

## 13. Implementation Order

### C1 — Freeze current runtime behavior

- [ ] Stop clean-pilot work before changing scheduler policy.
- [ ] Ensure no active DagRun will span old/new policy code.
- [ ] Record current DAG blob/reference for comparison.
- [ ] Run focused baseline scheduler/campaign tests.

### C2 — Centralize timing configuration

- [x] Add `CAMPAIGN_SCHEDULE_INTERVAL_MINUTES=10` to root `config.py`.
- [x] Add `QUARTER_COOLDOWN_MINUTES=10` to root `config.py`.
- [x] Keep `MONTHLY_SLOT_SPACING_MINUTES=15`.
- [x] Replace the environment pacing value with
      `SUBMISSION_INTERVAL_MINUTES=5`.
- [x] Remove scheduler cadence/provider pacing duration knobs from
      `env-example` and activation logging.
- [x] Update focused schedule/pacing tests for fixed source-controlled timing.
- [ ] User verifies the focused timing tests locally.

### C3 — Add pure cooldown eligibility helper

- [x] Implement a small testable helper that accepts predecessor completion time,
      current time, and cooldown duration.
- [x] Return explicit eligibility metadata including `eligible_at` and
      remaining duration.
- [x] Keep helper independent from Airflow runtime objects.
- [x] Move human-readable wait formatting into an Airflow-neutral shared
      scheduling helper and reuse it from RAW deferral logging.

### C4 — Integrate cooldown into campaign selection

- [x] Reconcile predecessor progress through the existing ordered selector.
- [x] Enforce predecessor complete + valid before automatic advancement.
- [x] Enforce `completed_at + cooldown`.
- [x] Avoid claiming the next quarter while cooldown is active.
- [x] Block automatic advancement if a complete predecessor lacks
      `completed_at`.
- [x] Preserve existing failure, availability, and claim behavior.
- [x] Add explicit cooldown active/satisfied observability logs.
- [x] Add deterministic unit coverage for the 9m59s and exact 10m boundaries.
- [x] User verifies focused campaign/scheduler tests locally: 66 passed in 4.92s
      on 2026-10-08.

### C5 — Focused tests

- [x] User verification on 2026-10-08: 66 focused orchestration tests passed
      in 4.92s.

Run:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest \
  tests/unit/orchestration/test_schedule.py \
  tests/unit/orchestration/test_campaign_policy.py \
  tests/unit/orchestration/test_progress_wiring.py \
  tests/unit/orchestration/test_dag.py \
  -q
```

Adjust exact test filenames only if the repository's current campaign-policy
coverage uses a different file.

### C6 — Full unit regression

- [x] User verification on 2026-10-08: full unit suite passed 325 tests in 7.55s.

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

### C7 — PostgreSQL integration gate

Run the existing orchestration integration suite plus new cooldown coverage.

At minimum:

```bash
python -m pytest \
  tests/integration/pipeline/test_postgresql_idempotency.py \
  tests/integration/orchestration/test_pipeline_progress_reconciliation.py \
  tests/integration/orchestration/test_submission_pacing.py \
  tests/integration/orchestration/test_orchestration_database_bootstrap.py \
  -q
```

- [ ] Add the cooldown integration test path to this gate once created.

### C8 — Documentation

- [ ] Update `orchestration/README.md`.
- [ ] Update `env-example` only for approved environment-facing cadence
      changes.
- [ ] Keep this checklist updated after each completed phase.

### C9 — Clean destroy/bootstrap pilot

Only after unit + integration gates pass:

- [ ] Destroy local development runtime/data using `destroy_all.py`.
- [ ] Bootstrap Airflow without manual cleanup.
- [ ] Start Airflow with the approved scheduler cadence.
- [ ] Verify one DAG version is used for the whole pilot.
- [ ] Verify Q1 runs Jan/Feb/Mar depth-first with `+0/+15/+30` earliest starts.
- [ ] Verify Q1 becomes complete + valid in PostgreSQL.
- [ ] Record Q1 `completed_at`.
- [ ] Verify Q2 is not selected before cooldown expiry.
- [ ] Verify the first scheduler tick at/after cooldown expiry selects Q2.
- [ ] Verify no overlapping active quarter DagRuns.
- [ ] Verify mapped task labels and human-readable defer logs remain correct.
- [ ] Verify no scientific/pipeline behavior changed.

---

## 14. Worked Pilot Example

Assume:

```text
campaign                    = 2016 only
scheduler interval          = 10m
quarter cooldown            = 10m
monthly slot spacing        = 15m
CDS submission pacing       = 5m
```

Expected high-level sequence:

```text
21:00  scheduler selects 2016 Q1
       Jan earliest-start = 21:00
       Feb earliest-start = 21:15
       Mar earliest-start = 21:30

       Jan: attempt → resolve → transform → load
       Feb: attempt → resolve → transform → load
       Mar: attempt → resolve → transform → load

21:47  PostgreSQL marks 2016 Q1 complete + valid
       completed_at = 21:47

21:50  scheduler tick
       Q2 blocked by cooldown
       eligible_at = 21:57

22:00  scheduler tick
       cooldown satisfied
       scheduler selects 2016 Q2

       Apr earliest-start = 22:00
       May earliest-start = 22:15
       Jun earliest-start = 22:30
```

The exact wall-clock completion times will vary. The required invariant is:

```text
Q2 selection time >= Q1 completed_at + quarter cooldown
```

---

## 15. Definition of Done

This refactor is complete only when:

- [ ] Quarter cooldown is enforced from durable PostgreSQL progress.
- [ ] Q2 cannot start before Q1 is complete + valid.
- [ ] Q2 cannot start before Q1 `completed_at + cooldown`.
- [ ] No new PostgreSQL schema is required unless implementation proves
      otherwise and receives separate approval.
- [ ] Depth-first monthly execution remains unchanged.
- [ ] Monthly `+0/+15/+30` earliest-start behavior remains unchanged.
- [ ] CDS five-minute pacing remains independent and unchanged.
- [ ] Manual runs and manual repair remain unchanged.
- [ ] Failed-quarter blocking remains unchanged.
- [ ] Unit tests pass.
- [ ] PostgreSQL integration tests pass.
- [ ] Documentation explains all four timing controls.
- [ ] A clean destroy/bootstrap scheduled pilot proves Q1 -> cooldown -> Q2
      behavior without manual intervention.
- [ ] No files under `pipeline/` are modified.
