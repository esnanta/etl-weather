# Active Work Plan

Last reviewed: 2026-10-11 (Asia/Jakarta)  
Branch: `dev` only

Only unfinished work for the current ERA5-Land ETL campaign is tracked here.
Completed implementation and pilot checklists are removed; prior evidence
remains in Git history.

## 1. Current ETL scope

- Dataset: Copernicus CDS `reanalysis-era5-land`, hourly input.
- Region: Aceh bbox N6.1 / W94.9 / S1.9 / E98.3.
- Variables: `tp`, `t2m`, `swvl1`, `ssrd`, `d2m`.
- Canonical output: validated UTC+7 daily observations and artifacts, with
  PostgreSQL `climate_data.daily_climate` and
  `climate_data.daily_climate_detail`.
- Airflow: one DAG `cds_era5_land_etl`, depth-first monthly TaskGroups,
  durable progress and provider submission pacing.
- Current timing policy (root `config.py`): scheduler tick 10 minutes, quarter
  cooldown 10 minutes, month slots +0/+15/+30 minutes, provider POST pacing
  5 minutes.
- The one-year scheduled campaign gate is **complete**, as confirmed by the
  operator on 2026-10-11. No new pilot is required merely to repeat that gate.

## 2. Remaining work — historical campaign 2016–2025

The next scope is completion of the bounded historical campaign, continuing
from the earliest incomplete quarter. Do not restart completed quarters merely
because old plans have been cleaned up.

- [ ] Verify current persisted progress and the earliest incomplete quarter
      before continuation.
- [ ] Continue eligible quarters through 2025 Q4 using the verified scheduler.
- [ ] Track completed, remaining, and failed quarters and required manual repairs.
- [ ] Inspect artifact disk space, PostgreSQL growth, and Airflow health
      periodically.
- [ ] Never deploy a changed DAG graph into an active DagRun.
- [ ] Verify all 40 campaign quarters and 120 monthly daily-data partitions
      have valid persisted evidence.
- [ ] Reconcile PostgreSQL daily observations with progress and checkpoint
      evidence; verify no failed detail remains.
- [ ] Confirm that subsequent scheduler ticks select no work when complete.
- [ ] Disable the historical schedule after final verification and record the
      campaign completion state.

The one-year success is operator-reported; this document does not claim a
new live run or independent PostgreSQL verification during this cleanup.

## 3. Outside this ETL scope

**Exploratory Data Analysis (EDA)** is downstream analytical work, not part of
the present ETL implementation or an acceptance gate for completing the
historical campaign. Do not introduce EDA tasks into the ETL DAG or TODO.

**Monthly persistence** is deferred. Do not create
`climate_data.monthly_climate` or `climate_data.monthly_climate_detail`
as part of the current ETL campaign. If monthly data is needed later, it may
be obtained by queries/aggregation over canonical daily observations; dedicated
monthly tables are optional and require a separate demonstrated need and
design decision. Neither EDA nor physical monthly tables are prerequisites
for the current campaign.

**Ongoing ingestion beyond 2025** is separate from the bounded historical
campaign. Future design decisions on freshness, availability, retries, and DAG
organization should be made only if that new scope is approved.

## 4. Working rules

- Read and change only branch `dev`.
- Preserve the ERA5-Land scientific, temporal, spatial, artifact, and
  idempotency contracts in `pipeline/`.
- Keep Airflow orchestration limited to scheduling, coordination, monitoring,
  and recovery.
- Do not reset existing validated daily data to repeat a completed rollout gate.
- Remove finished tasks from this document rather than accumulating historical
  checklists.
