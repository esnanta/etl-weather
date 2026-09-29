# Orchestration Refactor Plan and Checklist

Last updated: 2026-09-29 (Asia/Jakarta)  
Branch: `dev` only  
Status: Planning recorded; implementation and database reset have not started.  
Reviewed baseline: `65e70c31ece2bf90ba159e36021e25a7d184f457`

## 1. Purpose and ownership

Refactor orchestration for clear responsibilities and a single source for each
configuration value while preserving ETL and scheduling behavior. Recreate the
unused development PostgreSQL database with one application schema as an explicit,
approved storage change.

This document is the active checklist for this refactor. Read and update it on
every subsequent fix or implementation step. Record work in the same change as
the code whenever practical. Do not mark an item complete without evidence.

[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) retains the earlier Airflow
implementation and pilot history; its historical passes are not verification of
this refactor. Do not maintain a duplicate refactor checklist there.

References:

- [Root AGENTS.md](../../AGENTS.md)
- [Root README.md](../../README.md)
- [Orchestration README.md](../README.md)
- [Airflow best practices](../../docs/AIRFLOW_BEST_PRACTICE.md)
- [Airflow coding guidelines](../../docs/AIRFLOW_CODING_GUIDELINES.md)
- [Testing rules](../../tests/AGENTS.md)

Explicit user decisions below govern this work. Refresh the actual `dev` code
before implementation; do not restore old behavior from stale documentation.

## 2. Decisions and scope

| Decision | Scope |
| --- | --- |
| Separate configuration by responsibility | Shared configuration at the root; operational configuration in orchestration |
| One PostgreSQL database | Use the configured project database for weather data, progress, and pacing |
| Recreate development PostgreSQL state | User states the database is not in use and permits deletion and recreation; no legacy-data migration is required |
| One application schema | Proposed target: `climate_data`, retaining canonical weather table names |
| Airflow metadata remains SQLite | Preserve the existing backend and metadata file; no PostgreSQL migration or SQLite reset |
| Preserve behavior | Scientific output, request identity, checkpoints, DAG graph, retry, pacing, and manual recovery stay unchanged |
| Notify before touching other areas | Before editing outside `orchestration/`, identify exact files, reasons, and impact, especially for `pipeline/` |
| Current step is documentation only | Do not reset a database, refactor code, submit CDS requests, or enable the campaign in this step |

The schema consolidation and loss of old development PostgreSQL state are the
intentional exceptions to preservation. Record these separately from structural
code movement.

Database reset does not authorize deletion of RAW/Parquet files, chunk caches,
Airflow history, logs, credentials, or the JWT key. Existing artifacts remain
eligible for normal validated resume. A completely fresh download is a different
operation and must have an explicit scope.

### Scope notifications required before implementation

| Area outside orchestration | Possible change and reason |
| --- | --- |
| Root `config.py` | Add shared values, including the one application schema definition |
| `scripts/config.py` and its callers | Relocate shared configuration and update imports/request construction |
| `scripts/report_campaign_progress.py`, `scripts/inspect_campaign_capacity.py` | Update imports and schema references if the dependency audit finds them |
| `tests/` | Adapt imports and schema expectations; protect existing behavior |
| Root and `docs/` documentation | Correct affected configuration/schema references and outdated graph guidance |
| `pipeline/database/config.py` | Only if needed, import the authoritative shared schema constant instead of defining it again |

This table is a planning inventory, not notice that all listed files will change.
Before each implementation batch, report the exact required files. Keep scientific
transformation, extraction, validation, loaders, storage, and request algorithms
outside the refactor. Any newly discovered need to change their behavior must be
reported separately before work.

## 3. Configuration design

Use one definition per setting, with separate owners:

| Owner | Responsibility |
| --- | --- |
| Root `config.py` | Shared dataset, bbox, variable selection, shared timezone name where appropriate, and application schema name |
| `orchestration/config.py` | Campaign range, enable/cadence names and defaults, cron mapping, pacing settings, monthly slot spacing, availability lag, disk threshold, and shared orchestration policy values |
| Existing `pipeline.database.DatabaseConfig` | PostgreSQL credentials and connection configuration; reuse rather than inventing another implementation |
| `orchestration/schedule.py` | Schedule resolution and validation using configuration |
| `orchestration/database.py` | Runtime engine lifecycle and coordination of operational table initialization |

Keep root configuration lightweight and independent of orchestration. Audit
transitive imports to avoid cycles: if `pipeline.database` consumes root schema
settings, root configuration must not import it back. Prefer simple shared values;
construct typed requests through a separate helper where necessary.

`build_cds_request()` is a request factory, not a configuration constant. Choose
its final home after auditing standalone callers. Preserve its defaults, variable
copying, validation, and API-key behavior. Do not create a second request-key or
partitioning implementation.

Every module-level variable/constant introduced or touched must have an English
comment above it explaining purpose and, where relevant, unit/default rationale.
Every new or reorganized module needs an English purpose docstring. Do not turn
every internal constant into a new environment setting.

Preserve environment variable names, defaults, accepted values, error behavior,
and evaluation timing. Schedule settings are resolved when the DAG is parsed;
database credentials and clients remain runtime concerns. Imports must not
connect to databases, load artifacts, submit CDS requests, or load `.env` as a
side effect. Preserve per-task differences in retry/timeout policy.

## 4. PostgreSQL design

Target application tables:

| Table | Responsibility |
| --- | --- |
| `climate_data.daily_climate` | Weather partition metadata |
| `climate_data.daily_climate_detail` | Daily observations |
| `climate_data.pipeline_progress` | Operational batch progress |
| `climate_data.pipeline_progress_detail` | Partition progress and evidence |
| `climate_data.cds_submission_pacing` | Shared submission clock |

One schema means one application schema; PostgreSQL system schemas are unaffected.
Database name and credentials continue to come from the existing environment.

All operational DDL, queries, reports, and reconciliation must derive their schema
from the authoritative setting. Remove hard-coded qualified table names in task
SQL as well as duplicate schema constants. Reuse canonical weather table settings
from their existing owner rather than redefining them.

Separate DDL/bootstrap from repository operations where useful, without collecting
all SQL into one oversized module. Keep transaction boundaries, constraints,
foreign keys, uniqueness, row locks, and database-clock behavior unchanged.

Do not blindly replace `create_engine()` with `create_database_engine()`:
the latter can create a missing database. Preserve normal task connection behavior;
make reset/recreation an explicit maintenance operation. Do not create a shared
live engine at module import time.

No compatibility views, dual-schema writes, or migration of discarded data are
required. Audit legacy upgrade DDL; remove it only when redundant with the verified
fresh schema and preserve idempotent initialization.

## 5. Responsibility boundaries

Proposed destinations, subject to the caller audit:

| Module | Responsibility |
| --- | --- |
| `orchestration/requests.py` | Monthly request specifications and historical eligibility, delegating to pipeline request functions |
| `orchestration/intervals.py` | Pure Airflow-interval to inclusive-local-date conversion |
| `orchestration/monitoring.py` | Disk warnings and process peak-memory logging |
| `orchestration/tasks/build_monthly_requests.py` | Manual/scheduled job preparation |
| `orchestration/tasks/extract.py` | Extraction runtime delegation and extraction progress coordination |
| `orchestration/tasks/transform.py` | Transformation delegation and progress coordination |
| `orchestration/tasks/load.py` | Load delegation and verification coordination |
| `orchestration/tasks/cds_extract_operator.py` | Airflow deferral, wakeup, first-pass handling, and pass-through |
| `orchestration/repositories/` | Operational SQL, claims, persisted evidence, and reconciliation |
| `orchestration/database.py` | Engine lifecycle and bootstrap coordination |
| `orchestration/dags/cds_era5_land_etl.py` | Stable DAG declarations and dependency wiring |

Extract common progress recording and campaign repair into clearly named helpers
as needed. Move SQL such as `_persisted_row_count()` out of task wrappers.
Both `tasks/era5_land.py` and `tasks/progress.py` must be considered; splitting only
the former leaves the main responsibility mixing unresolved.

Campaign request construction should not import from a task wrapper. Avoid
generic utility collections, extra Airflow tasks, and unnecessary class layers.
Python module movement must not rename Airflow task IDs.

## 6. Behavior baseline to preserve

Verify these against the implementation baseline before editing:

- One DAG: `cds_era5_land_etl`; preserve task IDs and mapped dependencies.
- Current graph: `build_monthly_requests`, mapped `first_extract_attempt`,
  `prepare_retry_pass`, mapped `extract_or_resume`, mapped
  `transform_or_resume`, and mapped `load`.
- All first-pass extraction results reach the retry barrier before retry-pass work.
  Successful results pass through; failed partitions receive the existing bounded
  attempts. Preserve the failure-control payload and exception handling.
- Scheduled monthly lower bounds are +0/+1/+2 hours from the selection-time
  anchor (`slot_anchor` or current UTC time). Do not substitute `logical_date`.
  All maps still belong to one DagRun. Manual jobs have no scheduled offset.
- Preserve task retries, delays/backoff, timeouts, pool, `catchup=False`,
  `max_active_runs=1`, and `max_active_tasks=2`.
- Schedule is disabled by default; enabled default cadence is `2h`.
  Accepted cadence values remain `30m`, `1h`, and `2h`.
- CDS submission interval defaults to 1,800 seconds independently of cadence.
  Preserve row locking, PostgreSQL clock, and deferral/recheck behavior.
  One-hour slots and 30-minute pacing are project policy, not provider quotas.
- Campaign remains 2016–2025, selecting the earliest eligible incomplete quarter.
  Preserve claim ownership, failed-quarter blocking, and validated manual repair.
- Preserve six-day availability lag and 5 GiB warning threshold.
- Preserve small XCom metadata, request/artifact identity, validated resume,
  idempotent load, provenance, missing values, UTC+7 daily semantics, and units.
- Preserve the active ERA5-Land hourly source, variable set, and Aceh bbox.
  Do not introduce ERA5 or the daily-statistics product as a replacement.
- Preserve logging content and monitoring semantics unless separately discussed.

Apparent bugs or documentation/code disagreements found during this work must be
logged separately. Do not silently fix behavior in a structural refactor.

## 7. Ordered checklist

### P0 — Planning

- [x] Record the requested configuration split and database recreation decision.
- [x] Explicitly retain SQLite for Airflow metadata.
- [x] Record scope boundaries, preservation rules, and the document update rule.
- [x] Record the reviewed `dev` baseline and proposed schema target.

### P1 — Audit and baseline verification

- [ ] Refresh `dev` HEAD and applicable instructions; record implementation baseline SHA.
- [ ] Audit configuration, schema literals, imports, request-factory callers, and reports.
- [ ] Record exact files needed outside orchestration and notify the user before editing.
- [ ] Run relevant existing offline tests; record actual results and pre-existing failures.
- [ ] Capture graph, mapping, retry/defer, slot-anchor, XCom, and progress contracts.
- [ ] Finalize module destinations and check that the proposed imports cannot cycle.

### P2 — Configuration separation

- [ ] Introduce root shared configuration and orchestration-specific configuration.
- [ ] Set the single application schema source to `climate_data`.
- [ ] Reuse the existing database credential configuration.
- [ ] Relocate request construction/shared settings and update audited callers.
- [ ] Eliminate duplicate setting definitions and schema literals in affected runtime code.
- [ ] Add module purpose docstrings and comments above global variables.
- [ ] Verify defaults, environment validation, import safety, and standalone request behavior.

### P3 — Database organization and fresh-schema support

- [ ] Separate operational bootstrap, engine lifecycle, and repository responsibilities.
- [ ] Point all five application tables and their consumers to the shared schema.
- [ ] Move persisted-load verification SQL out of task wrappers.
- [ ] Preserve constraints, transaction boundaries, atomic claims, and pacing locks.
- [ ] Verify fresh bootstrap and repeated bootstrap using isolated PostgreSQL resources.
- [ ] Verify pacing, reconciliation, idempotency, and rollback with the new schema.
- [ ] Prepare a maintenance procedure showing the exact target database and recreation steps.

### P4 — Task responsibility refactor

- [ ] Separate request planning, interval conversion, and monitoring from ETL wrappers.
- [ ] Split extraction, transformation, load, and job preparation into named modules.
- [ ] Separate shared progress recording and campaign-repair coordination as needed.
- [ ] Update campaign, operator, DAG, repository exports, and audited callers.
- [ ] Preserve task IDs, graph, XCom fields, exceptions, attempts, and deferral behavior.
- [ ] Verify moved disk-path logic still resolves the repository data directory correctly.
- [ ] Verify logging and process-memory measurements retain their original meaning.

### P5 — Regression and documentation

- [ ] Run the offline unit and orchestration suite against the completed refactor.
- [ ] Run relevant PostgreSQL integration tests on isolated resources; report skips honestly.
- [ ] Verify scheduled selection, first-pass barrier, pass-through, offsets, and manual mode.
- [ ] Verify core pipeline remains Airflow-independent and scientific behavior is unchanged.
- [ ] Search affected callers/docs/tests for stale imports and old operational schema references.
- [ ] Update orchestration README and affected documentation after the required scope notice.
- [ ] Review the diff for unrelated changes and record any deviations from this plan.
- [ ] Confirm all implementation changes are on `dev`.

### P6 — Development PostgreSQL recreation

This is authorized in principle, but deferred until implementation and testing
are ready. Use the actual host configuration; never guess a database name.

- [ ] Identify the exact host/database and verify it is the disposable project database.
- [ ] Keep scheduling disabled and inspect active, queued, and deferred task instances.
- [ ] Ensure no old worker or DagRun can resume against the replacement database.
- [ ] Resolve old pending execution state deliberately while preserving SQLite and history.
- [ ] Record artifact retention and the consequences of resetting progress/pacing.
- [ ] Delete and recreate only the verified target PostgreSQL database.
- [ ] Bootstrap the five application tables in the single application schema.
- [ ] Verify schema, constraints, connection settings, and fresh progress state.
- [ ] Confirm SQLite metadata, artifact files, credentials, and JWT key were not deleted.

Old XCom may refer to progress IDs from the discarded database. Do not clear and
resume old task instances against the new database; use new runs that initialize
their own progress. Old Airflow success states are historical evidence, not proof
that the replacement PostgreSQL database contains data. A reset pacing table also
loses the last-submission time; account for the last actual CDS submission before
any new live request.

### P7 — Controlled runtime verification

- [ ] Agree the bounded live pilot scope before initiating new CDS work.
- [ ] Run a new manual pilot; verify checkpoints, load evidence, and progress reconciliation.
- [ ] Rerun the same partition and verify validated resume and unchanged-load idempotency.
- [ ] Run one clean scheduled pilot under the completed code version.
- [ ] Verify monthly slots, pacing, first-pass/retry ordering, and database evidence.
- [ ] Leave unattended campaign execution disabled pending explicit rollout review.

## 8. Completion and update discipline

Work is complete only when the required checks have evidence, the database setup
matches the documented target, and controlled runtime verification is recorded.
Distinguish code-complete from deployment-complete and pilot-verified.

For every subsequent fix:

1. Read this document and identify the checklist IDs being addressed.
2. State any newly required changes outside orchestration before editing.
3. Update relevant checkboxes, current status, changed paths, and validation results.
4. Record failures, skipped checks, blockers, and the next concrete action.
5. Add a dated entry below. Do not reuse old test counts or old pilot success as
   evidence for new code. Record implementation commit IDs when available.
6. Keep pending work unchecked; a prepared procedure is not an executed reset.

## 9. Work log

| Date (Asia/Jakarta) | Step | Change / evidence | Remaining work |
| --- | --- | --- | --- |
| 2026-09-29 | P0 | Planning document created from the user decisions and reviewed dev code; documentation-only change. No tests, reset, code refactor, or live requests executed. | Start P1; notify exact external paths before implementation. |

Current next action: P1 dependency/configuration audit and baseline verification.
No implementation is started by the creation of this document.
