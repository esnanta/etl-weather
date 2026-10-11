# Active Work Plan

Last reviewed: 2026-10-11 (Asia/Jakarta)  
Branch: `dev` only

This file contains only work that is still active or intentionally deferred.
Completed refactor/audit plans are removed from `docs/todo/` rather than kept as
historical checklists.

## 1. Current implemented baseline

The current `dev` implementation already provides:

- one Airflow DAG: `cds_era5_land_etl`;
- one mapped monthly TaskGroup per calendar month;
- task flow:
  `build_monthly_requests -> monthly_etl[YYYY-MM] ->
  attempt_raw_checkpoint -> resolve_raw_checkpoint ->
  ensure_transformed_checkpoint -> load`;
- durable PostgreSQL campaign progress and CDS submission pacing;
- one application schema: `climate_data`;
- root `config.py` as the single lightweight configuration source;
- guarded reset/destroy maintenance commands;
- canonical daily-climate persistence;
- source-controlled orchestration timing:
  - scheduler check every 10 minutes;
  - 10-minute quarter cooldown;
  - monthly earliest-start offsets of +0/+15/+30 minutes;
  - minimum 5-minute spacing between CDS submission POST requests;
- `max_active_runs=1`, `max_active_tasks=2`, and one-slot `cds_api` pool;
- schedule disabled unless `ERA5_CAMPAIGN_SCHEDULE_ENABLED=true`.

Scientific ETL behavior remains owned by `pipeline/`.

## 2. Remaining rollout work

### R1 — Clean current-graph scheduled quarter

Run one clean scheduled quarter using the current DAG and current timing policy
before treating the historical campaign as ready for unattended continuation.

- [ ] Confirm previous DagRuns are terminal and no DAG deployment is in progress.
- [ ] Run the current unit suite with campaign scheduling disabled.
- [ ] Run the isolated PostgreSQL integration suite.
- [ ] Record the initial campaign-progress report and capacity snapshot.
- [ ] Enable scheduled mode for the pilot.
- [ ] Confirm exactly three calendar months are mapped for the selected quarter.
- [ ] Confirm monthly lower bounds are +0/+15/+30 minutes.
- [ ] Confirm provider submissions obey the durable 5-minute pacing gate.
- [ ] Confirm successful months can continue depth-first while another month is
      deferred or retried.
- [ ] Confirm retry/resume remains bounded and checkpoint-safe.
- [ ] Confirm persisted daily-climate rows agree with progress/load evidence.
- [ ] Confirm the quarter becomes complete only after all three monthly
      partitions are valid.
- [ ] Record DagRun elapsed time, deferrals, retries, provider errors, and any
      manual intervention.
- [ ] Disable scheduled mode again while reviewing the pilot result.

Exit condition: Airflow state, PostgreSQL progress, daily-climate persistence,
and artifact evidence agree for one quarter under the current `dev` graph.

### R2 — One-year scheduled campaign gate

After R1 succeeds, exercise the normal scheduler across one complete calendar
year (four sequential quarters).

- [ ] Decide explicitly whether the one-year gate starts from retained verified
      campaign state or a clean development campaign state.
- [ ] Process four sequential quarters through normal scheduled selection.
- [ ] Verify every selected quarter is the earliest eligible incomplete quarter.
- [ ] Verify the 10-minute completion-relative quarter cooldown.
- [ ] Record quarter elapsed time, queue/defer behavior, provider failures, and
      retries.
- [ ] Verify all 12 monthly daily-climate partitions and progress evidence.
- [ ] Review disk growth, PostgreSQL growth, and task peak-memory observations.
- [ ] Exercise the documented manual-repair path if a real terminal monthly
      failure occurs.
- [ ] Decide whether the current 10/10/15/5-minute timing policy is operationally
      suitable based on the observed year; change it only as a separate reviewed
      orchestration change if evidence justifies it.

Exit condition: one complete year is recoverable and independently verifiable
from artifacts, PostgreSQL daily observations, and durable campaign progress.

### R3 — Complete the bounded 2016–2025 historical campaign

Proceed only after R2 passes.

- [ ] Continue from the earliest incomplete quarter through 2025 Q4.
- [ ] Track completed, remaining, and failed quarters.
- [ ] Track failed months and manual repairs.
- [ ] Periodically inspect artifact storage, PostgreSQL size, and Airflow health.
- [ ] Do not deploy a changed DAG graph into an active DagRun.
- [ ] Verify all 40 configured quarters are complete.
- [ ] Verify all 120 expected monthly request partitions have valid persisted
      daily-climate evidence.
- [ ] Verify no failed campaign details remain.
- [ ] Verify additional scheduled ticks select no work.
- [ ] Disable the bounded historical schedule after final verification and record
      the completion state.

## 3. EDA before monthly-climate database persistence

The canonical daily dataset must be examined through exploratory data analysis
before defining or creating PostgreSQL monthly-climate tables.

The root `README.md` already records the intended names
`climate_data.monthly_climate` and
`climate_data.monthly_climate_detail`. Those names are documentation of the
intended downstream layer, **not authorization to create the tables now**.

### E1 — Daily-climate EDA

- [ ] Define the EDA questions and acceptance criteria for the canonical
      `daily_climate` / `daily_climate_detail` dataset.
- [ ] Evaluate temporal completeness by local day and month.
- [ ] Evaluate grid-point coverage and stable spatial membership.
- [ ] Characterize missing-value patterns without replacing missing values with
      zero.
- [ ] Inspect distributions and plausible ranges for `tp`, `ssrd`, `t2m`,
      `d2m`, and `swvl1`.
- [ ] Inspect suspicious outliers, discontinuities, and boundary effects.
- [ ] Verify daily UTC+7 semantics remain suitable for downstream monthly
      statistics and modelling.
- [ ] Record EDA conclusions and any required data-quality rules before changing
      monthly persistence design.

### E2 — Monthly layer, explicitly deferred

Do **not** create `monthly_climate` or `monthly_climate_detail` during the
current rollout work.

After E1 is reviewed:

- [ ] Decide whether the existing monthly Parquet/statistics contract should be
      retained unchanged.
- [ ] Decide the exact PostgreSQL monthly schema, keys, constraints, and
      idempotency contract.
- [ ] Add monthly persistence and its PostgreSQL integration tests only after
      that design is approved.
- [ ] Reconcile the root `README.md` wording with the final implemented monthly
      persistence state.

## 4. Future work outside the current historical rollout

These items remain intentionally deferred until the historical campaign and EDA
provide enough evidence:

- [ ] Design ongoing ingestion for periods after the bounded 2016–2025 campaign.
- [ ] Define freshness/last-success observability for ongoing ingestion.
- [ ] Define delayed-availability and retry policy for new periods.
- [ ] Decide whether ongoing ingestion should extend the existing DAG contract
      or justify a different design.

No second DAG is authorized by this TODO.

## 5. Working rules

- Work only on `dev`.
- Keep live CDS work disabled except during an explicitly reviewed rollout step.
- Do not change `pipeline/` scientific behavior as part of orchestration
  rollout.
- Do not create monthly PostgreSQL tables before the daily-climate EDA gate.
- Keep completed plans out of `docs/todo/`; Git history is the historical
  record.
- Update this file when a remaining item is completed, and remove completed
  sections rather than accumulating historical implementation logs.
