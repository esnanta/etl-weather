# Orchestration Refactor Plan and Checklist

Last updated: 2026-10-05 (Asia/Jakarta)  
Branch: `dev` only  
Status: P1 through P7 and structural phases S1 through S5 are complete. The latest post-S5 verification has 283 unit/orchestration tests and 6 isolated PostgreSQL integration tests passing. Live CDS requests and schedule enablement remain explicitly deferred.  
Planning baseline: `65e70c31ece2bf90ba159e36021e25a7d184f457`  
P1 audited baseline: `2f8a93e509ac85ea8ac4ac4a7efffae18e32fc69`

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
| `climate_data.submission_pacing` | Shared submission clock |

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

The task modules are orchestration adapters only. They may coordinate progress,
monitoring, pacing, Airflow control metadata, and calls into the pipeline, but
must not reimplement extraction, transformation, loading, request identity,
checkpoint, validation, or scientific logic already owned by `pipeline/`.
Changes inside `pipeline/` require a separate, concrete technical reason and
must be reported before editing.

Campaign request construction should not import from a task wrapper. Avoid
generic utility collections, extra Airflow tasks, and unnecessary class layers.
Python module movement must not rename Airflow task IDs.

## 6. Behavior baseline to preserve

Verify these against the implementation baseline before editing:

- One DAG: `cds_era5_land_etl`; keep the DAG identity stable.
- Current graph: `build_monthly_requests`, mapped `attempt_raw_checkpoint`,
  mapped `resolve_raw_checkpoint`, mapped `ensure_transformed_checkpoint`,
  and mapped `load`. Monthly mapped instances use a `YYYY-MM` display label
  for Airflow UI observability.
- Successful first-pass RAW results continue immediately through
  `resolve_raw_checkpoint` as pass-through results. Only failed first-pass
  results wait for the durable extraction barrier before bounded retry work.
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

- [x] Refresh `dev` HEAD and applicable instructions; record implementation baseline SHA.
- [x] Audit configuration, schema literals, imports, request-factory callers, and reports.
- [x] Record exact files needed outside orchestration and notify the user before editing.
- [x] Run relevant existing offline tests locally; record actual results and pre-existing failures. User reported 208 passed in 6.15s on 2026-10-03; no failures or skips.
- [x] Verify the local PostgreSQL integration baseline. User reported 5 passed in 1.30s on 2026-10-03; no failures or skips (see section 10.1).
- [x] Verify the existing full-suite CI run for the exact audited baseline: 208 passed, 5 skipped; no failed tests. This is historical CI evidence, not a new local pytest execution.
- [x] Run available dependency-free checks locally: 35 direct behavior assertions; syntax parsing for 80 Python files; core import-boundary check for 26 pipeline modules.
- [x] Capture graph, mapping, retry/defer, slot-anchor, XCom, and progress contracts.
- [x] Finalize module destinations and check that the proposed imports cannot cycle.

### P2 — Configuration separation

- [x] Introduce root shared configuration and orchestration-specific configuration.
- [x] Set the single application schema source to `climate_data`.
- [x] Reuse the existing database credential configuration.
- [x] Relocate request construction/shared settings and update audited callers.
- [x] Eliminate duplicate setting definitions and schema literals in affected runtime code.
- [x] Add module purpose docstrings and comments above global variables.
- [x] Verify defaults, environment validation, import safety, and standalone request behavior.

### P3 — Database organization and fresh-schema support

- [x] Separate operational bootstrap, engine lifecycle, and repository responsibilities.
- [x] Point all five application tables and their consumers to the shared schema.
- [x] Move persisted-load verification SQL out of task wrappers.
- [x] Preserve constraints, transaction boundaries, atomic claims, and pacing locks.
- [x] Verify fresh bootstrap and repeated bootstrap using isolated PostgreSQL resources.
- [x] Verify pacing, reconciliation, idempotency, and rollback with the new schema.
- [x] Prepare a maintenance procedure showing the exact target database and recreation steps.

### P4 — Task responsibility refactor

- [x] Separate request planning, interval conversion, and monitoring from ETL wrappers.
- [x] Split extraction, transformation, load, and job preparation into named modules.
- [x] Separate shared progress recording and campaign-repair coordination as needed.
- [x] Update campaign, operator, DAG, repository exports, and audited callers.
- [x] Preserve task IDs, graph, XCom fields, exceptions, attempts, and deferral behavior.
- [x] Verify moved disk-path logic still resolves the repository data directory correctly.
- [x] Verify logging and process-memory measurements retain their original meaning.

### P5 — Regression and documentation

- [x] Run the offline unit and orchestration suite against the completed refactor.
- [x] Run relevant PostgreSQL integration tests on isolated resources; report skips honestly.
- [x] Verify scheduled selection, first-pass barrier, pass-through, offsets, and manual mode.
- [x] Verify core pipeline remains Airflow-independent and scientific behavior is unchanged.
- [x] Search affected callers/docs/tests for stale imports and old operational schema references.
- [x] Update orchestration README and affected documentation after the required scope notice.
- [x] Review the diff for unrelated changes and record any deviations from this plan.
- [x] Confirm all implementation changes are on `dev`.

### P6 — Guarded development PostgreSQL recreation

The current development database is intentionally allowed to be recreated once
because the user has confirmed that its present contents are disposable. This
is a **temporary development-state exception**, not an application property.
The database must be treated as non-disposable by default now and in future
environments.

Normal runtime code must never drop or recreate the configured database.
Destructive database maintenance belongs in a dedicated maintenance entrypoint,
separate from DAGs, pipeline runtime, bootstrap helpers, and ordinary scripts.
The maintenance entrypoint must be gated by the root `APP_ENV` setting.
`APP_ENV=prod` disables database recreation completely: the command must stop
before presenting any deletion prompt or reaching any DROP/CREATE path.
`APP_ENV=dev` may expose guarded recreation after displaying host, port,
database, user, and schema without printing passwords. Immediately before
deletion, it must ask an interactive yes/no question naming the exact target;
`yes` or `y` proceeds, while `no` or `n` aborts cleanly. Missing, blank, or
unsupported `APP_ENV` values log an actionable warning and abort; no mode is assumed.

Use the actual active configuration; never infer or guess a database name.
Future production or retained-development databases must not be made eligible
for reset merely because they use the same schema or application code.

- [x] Resolve and display the exact configured host, port, database, user, schema, and validated `APP_ENV`.
- [x] Confirm that the current **development state** is explicitly authorized for this one-time recreation; do not label the database itself permanently disposable.
- [x] Define/verify a dedicated maintenance-only reset entrypoint that is unreachable from normal DAG/pipeline execution.
- [x] Enforce `APP_ENV=prod` as a hard deny: no reset option, no deletion prompt, and no DROP/CREATE path may execute.
- [x] Allow guarded recreation only when `APP_ENV=dev`; missing, blank, or unsupported `APP_ENV` values must abort before application startup.
- [x] In `dev`, display the exact target and ask an interactive confirmation before deletion; accept only `yes`/`y` to continue and `no`/`n` to abort.
- [x] Reject PostgreSQL system databases and never use a database-name pattern as the sole safety check.
- [x] Keep scheduling disabled and inspect active, queued, running, and deferred task instances.
- [x] Ensure no old worker or DagRun can resume against the replacement database.
- [x] Resolve old pending execution state deliberately by replacing the development SQLite metadata with a fresh Airflow metadata database; historical runtime state was intentionally discarded as part of reset-all.
- [x] Record artifact retention and the consequences of resetting progress/pacing.
- [x] Recreate only the explicitly confirmed development target after all guards pass.
- [x] Bootstrap the five application tables in the single application schema.
- [x] Verify schema, constraints, connection settings, and fresh progress state.
- [x] Confirm SQLite metadata reset scope and verify retained config, credentials, JWT key, and artifact directories were preserved as intended.

Old XCom may refer to progress IDs from the discarded database. Do not clear and
resume old task instances against the new database; use new runs that initialize
their own progress. Old Airflow success states are historical evidence, not proof
that the replacement PostgreSQL database contains data. A reset pacing table also
loses the last-submission time; account for the last actual CDS submission before
any new live request.

### P7 — Controlled runtime verification (offline)

P7 is an offline verification gate for the completed refactor. By explicit user
decision, this phase must not submit live CDS requests and must not enable the
campaign schedule. Live manual/scheduled pilots are deferred to a later rollout
decision and are not required to close P7.

- [x] Run the complete unit and Airflow-orchestration regression with
      `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false`.
- [x] Run all isolated PostgreSQL integration tests, including idempotency,
      reconciliation, pacing serialization, and repeated five-table bootstrap.
- [x] Verify DAG parsing, five-task graph, retries/timeouts, pool assignment,
      concurrency limits, and schedule=None under the disabled schedule.
- [x] Verify first-pass barrier, bounded retry/pass-through, pacing deferral,
      +0/+1/+2-hour scheduled slot metadata, failed-quarter blocking, and terminal
      no-op entirely through mocks/offline state.
- [x] Verify manual-mode preparation, progress/load evidence, campaign repair,
      resume/idempotency contracts, and database rollback without provider I/O.
- [x] Verify maintenance/reset guard tests remain green after P6 and that no
      ordinary DAG/pipeline path exposes database recreation.
- [x] Verify the test suite used for P7 contains no live CDS/provider dependency;
      isolated PostgreSQL is the only external runtime dependency allowed.
- [x] Review warnings/failures/skips explicitly; do not treat skipped PostgreSQL
      integration tests as passing evidence.
- [x] Leave `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` and do not initiate live CDS
      work after P7 completion.

Deferred rollout verification (outside P7): a bounded manual live pilot,
same-partition resume/idempotency pilot, and one clean scheduled pilot may be
performed only after separate explicit approval.

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
| 2026-10-03 | P1 | Audited 96 source/config/test/document files at `2f8a93e`; verified baseline CI (208 passed, 5 skipped), 35 local direct assertions, 80 Python syntax parses, and 26 core modules without Airflow/orchestration imports. Recorded dependency map, external scope, behavior contracts, and gaps below. | Local full pytest unavailable; P2 not started; PostgreSQL integration and live-host state not verified. |
| 2026-10-03 | P1 closure | User supplied local quiet-test results: unit/orchestration **208 passed in 6.15s**; PostgreSQL integration **5 passed in 1.30s**. No failures or skips. Local checkout SHA was not supplied. Marked P1 complete; earlier assistant dependency limitation does not apply to the user's successful environment. | Ready for P2; no application code or database changes in this documentation update. |
| 2026-10-03 | P2 implementation | Added root `config.py`, `request_factory.py`, and `orchestration/config.py`; moved shared dataset/bbox/variable/schema settings and orchestration campaign/schedule/pacing/slot/availability/disk policy to their owners. Updated audited runtime callers and schema references while retaining `DatabaseConfig` credentials and compatibility exports. Static reference audit found no remaining old `climate.*` table literals or old request-factory imports in the changed runtime files. | Post-change local unit/orchestration and PostgreSQL integration pytest evidence is still required. P3 and database reset remain untouched. |
| 2026-10-03 | P2 verification fix | User ran post-change tests: unit/orchestration **206 passed, 2 failed**; PostgreSQL integration **4 passed, 1 failed**. All three failures were stale test references to the former `climate` progress schema, while runtime correctly used `climate_data`. Updated `test_pipeline_progress.py` and `test_pipeline_progress_reconciliation.py` to derive progress schema/table names from `orchestration.config`; no runtime behavior changed. | Rerun both quiet test groups. P2 verification remains open until they pass. |
| 2026-10-03 | P2 closure | User reran the post-fix verification locally: unit/orchestration **208 passed in 5.64s**; PostgreSQL integration **5 passed in 1.28s**. No failures or skips were reported. P2 configuration separation is complete. | P3 is the next planned stage. Database recreation remains deferred to P6. |
| 2026-10-03 | P3 implementation | Added `orchestration/database.py` for runtime engine lifecycle and bootstrap coordination, plus `repositories/load_evidence.py` for persisted-load verification. Progress preparation and CDS pacing now use the shared lifecycle while preserving their original bootstrap timing. Removed redundant fresh-schema `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` upgrade DDL; the columns remain in the canonical CREATE TABLE definition. Added isolated fresh/repeated bootstrap coverage and guarded P6 recreation procedure. | Run post-change unit/orchestration tests and six PostgreSQL integration tests, including fresh bootstrap. Do not start P4 or P6 until reviewed. |
| 2026-10-04 | P3 closure | User ran post-change verification locally: unit/orchestration **211 passed in 6.32s** and PostgreSQL integration **6 passed in 1.46s**, including fresh/repeated bootstrap. No failures or skips were reported. P3 is complete. | Start P4 task responsibility refactor; database recreation remains deferred to P6. |
| 2026-10-04 | P4 implementation | Split request planning, interval conversion, monitoring, job preparation, extraction, transformation, load, progress recording, and campaign repair into responsibility-specific modules. Updated campaign, DAG, CDS operator, and orchestration tests to the new owners. Kept `tasks/era5_land.py` and `tasks/progress.py` only as thin compatibility re-export facades; runtime callers no longer depend on them. Preserved historical logger names and added a focused repository-data-path assertion. | Run post-change unit/orchestration and PostgreSQL integration tests. Keep P4 behavioral verification items open until results are reviewed. |
| 2026-10-04 | P4 closure and pipeline-boundary audit | User ran post-change verification locally: unit/orchestration **211 passed in 5.76s** and PostgreSQL integration **6 passed in 1.34s**. Audited the new task modules against `pipeline/runtime.py`, `pipeline/request.py`, loader and CDS extraction owners: extraction, transformation, and loading remain delegated to existing pipeline functions; job preparation remains orchestration-only. No `pipeline/` files were changed for P4. | P4 complete. Start P5 regression/documentation; any future `pipeline/` change requires a separately stated technical reason. |
| 2026-10-04 | P5 closure | Reused the immediately post-P4 regression evidence because it was run against the completed code refactor: **211 unit/orchestration passed in 5.76s** and **6 isolated PostgreSQL integration tests passed in 1.34s**, with no failures or skips reported. Direct HEAD audit of 46 affected runtime/test Python files found no active imports from the compatibility facades and no old `climate.*` operational SQL/schema references. Updated `orchestration/README.md`, `tests/AGENTS.md`, and the current Stage 8 notes in `IMPLEMENTATION_PLAN.md`; retained explicitly historical Stage 3 baseline text. Diff review from the audited baseline found only planned configuration/orchestration/scripts/tests/docs changes plus the previously approved `pipeline/database/config.py` shared-schema import; no scientific pipeline algorithms changed. | P5 complete. Proceed to P6 only with explicit target-database verification and scheduling/process preflight; no database reset has occurred yet. |
| 2026-10-04 | P6 safety clarification | Clarified that only the **current development contents** are authorized for one-time replacement; the application database is non-disposable by default. Revised P6 to require a dedicated maintenance-only reset path, dry-run default, explicit execute flag, ephemeral destructive-reset opt-in, exact configured database-name confirmation, system-database rejection, target display, and Airflow/process preflight. Normal DAG/pipeline/bootstrap code must never DROP or recreate the database. | P6 execution remains deferred; next work is to review/implement the guarded maintenance entrypoint before any destructive action. |
| 2026-10-04 | P6 environment-mode guard | Added root `APP_ENV=dev|prod` policy. The mode is mandatory: missing, blank, or unsupported values log an actionable warning and abort; no mode is assumed. Production must not expose any database delete/recreate feature. Development may expose the maintenance reset only after showing the exact configured target and receiving an interactive confirmation: `yes`/`y` proceeds, `no`/`n` aborts. Startup validation/logging is wired into Airflow activation, DAG parsing, CLI entrypoints, and CI declares `APP_ENV=dev` explicitly. No reset has been executed. | Run regression after adding `APP_ENV` to the local `.env`, then implement/test the maintenance command before any database recreation. |
| 2026-10-04 | P6 APP_ENV regression fix | User verified Airflow activation logs `Application environment | APP_ENV=dev`. The focused config run reported **12 passed, 1 failed**, and the full unit/orchestration run reported **221 passed, 1 failed**. The only failure was a stale regex expecting the superseded error text `APP_ENV must be either`; runtime correctly emitted the new actionable instruction `Set APP_ENV=dev or APP_ENV=prod`. Updated only the test assertion; runtime behavior is unchanged. | Rerun the focused config test and full unit/orchestration suite. P6 execution remains deferred. |
| 2026-10-04 | P6 maintenance command implementation | Added `scripts/recreate_postgres_database.py` as a maintenance-only entrypoint. It hard-denies `APP_ENV=prod` before database configuration access; missing/invalid APP_ENV aborts; dev inspect mode displays the configured target and runs read-only preflight; system databases are rejected; campaign scheduling must be disabled; queued/running/deferred/scheduled/retry/reschedule/restarting task states, queued/running DagRuns, Airflow service processes, and other PostgreSQL sessions block execution. `--execute` reaches the interactive `y/yes` or `n/no` confirmation only after preflight. DROP/CREATE uses quoted PostgreSQL identifiers and then bootstraps the five application tables. Added focused unit tests; no real database recreation has been executed. | Run the new unit tests and inspect-only command on the development host. Record exact target and blockers before any `--execute` use. |
| 2026-10-04 | P6 inspect import fix | User ran the focused APP_ENV/maintenance tests: **32 passed in 0.64s**. The first inspect-only execution failed before preflight because direct execution from `scripts/` resolved `from config` to the compatibility module `scripts/config.py`, causing a circular import. Added project-root path anchoring to the maintenance command and the other affected CLI entrypoints (`report_campaign_progress.py`, `inspect_campaign_capacity.py`), plus a subprocess regression test that executes the maintenance script directly under `APP_ENV=prod` and verifies a safe hard deny without import shadowing. No database connection, DROP, CREATE, or bootstrap occurred in the failed inspect attempt. | Rerun the focused maintenance tests and inspect-only command; do not use `--execute` yet. |
| 2026-10-04 | P6 reset component implementation | User reran the guarded maintenance tests: **33 passed in 1.46s**. Inspect-only preflight then passed against `APP_ENV=dev`, target `localhost:5432/weather`, user `postgres`, schema `climate_data`; all blocking Airflow task/DagRun/process counts and PostgreSQL other-session count were zero. Added three independent guarded reset entrypoints: `reset_data.py` clears only children of `data/`; `reset_airflow_logs.py` clears only children of `orchestration/logs/`; `reset_airflow_runtime.py` removes generated SQLite sidecars, PID files, and generated auth state while explicitly preserving tracked `orchestration/airflow.cfg`, DAGs, docs, JWT key, and source. Added isolated tmp-path unit tests. `reset_all` is intentionally not implemented until these three components are locally verified. | Run the three focused reset-component test modules and inspect-only commands. Do not execute destructive cleanup or create `reset_all` yet. |
| 2026-10-04 | P6 reset component verification | User ran the three focused reset-component test modules with quiet Airflow activation: **11 passed in 0.03s**. Inspect-only execution of `reset_data.py`, `reset_airflow_logs.py`, and `reset_airflow_runtime.py` showed the expected repository paths, preserved `orchestration/airflow.cfg`, and confirmed that no files were deleted. User requested `source activate-airflow.sh -q` as the default activation form for subsequent commands. | Individual reset components are verified. Next: implement `reset_all` strictly as composition of the tested maintenance functions, then test it without executing real deletion. |
| 2026-10-04 | P6 reset-all implementation | Added `scripts/reset_all.py` as an inspect-only-by-default orchestrator over the already verified maintenance functions. It validates mandatory `APP_ENV`, hard-denies prod before database configuration access, displays all reset targets, reuses the PostgreSQL static/runtime preflight, and shows one final yes/no confirmation only with `--execute`. Execution order is PostgreSQL recreation first, then data, Airflow logs, and Airflow runtime cleanup; a database recreation failure therefore prevents local artifact/runtime deletion. Added focused composition/guard tests, including direct-file execution under prod and verification that no deletion logic is duplicated. | Run `tests/unit/test_reset_all.py` together with the component tests, then run `python scripts/reset_all.py` in inspect-only mode. Do not use `--execute` until reviewed. |
| 2026-10-04 | P6 reset-all verification | User ran the guarded maintenance/component/reset-all unit suite with quiet activation: **49 passed in 1.98s**. Inspect-only `reset_all.py` then verified `APP_ENV=dev`, PostgreSQL target `localhost:5432/weather` (user `postgres`, schema `climate_data`), data/log/runtime paths, preserved `orchestration/airflow.cfg`, zero blocking Airflow task states, zero queued/running DagRuns, zero blocking Airflow processes, and zero other PostgreSQL sessions. Inspect-only completed with no reset actions executed. | All preflight guards are clear. Actual P6 reset may proceed only via `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python scripts/reset_all.py --execute` and explicit interactive confirmation. |
| 2026-10-04 | P6 destructive reset execution | User executed `reset_all.py --execute` under `APP_ENV=dev` with campaign scheduling explicitly disabled. Preflight again reported zero blocking task states, zero queued/running DagRuns, zero blocking Airflow processes, and zero other PostgreSQL sessions. After one explicit `y` confirmation, PostgreSQL database `weather` was recreated, `climate_data` bootstrap logged ready status for canonical weather tables, then local cleanup removed **2** top-level data entries, **2** Airflow log entries, and **4** generated Airflow runtime files. Tracked `orchestration/airflow.cfg` was explicitly preserved. | Run post-reset verification before marking P6 complete: verify all five `climate_data` tables and empty fresh progress/pacing state; verify Airflow SQLite/runtime state is absent until reinitialized; verify retained config/credentials/JWT/source files remain present. |
| 2026-10-04 | P6 post-reset storage verification | User verified the recreated PostgreSQL target `localhost:5432/weather` and schema `climate_data`. All five application tables are present: `daily_climate`, `daily_climate_detail`, `pipeline_progress`, `pipeline_progress_detail`, and `submission_pacing`; every table has **0 rows**, confirming fresh progress/pacing/weather state. Filesystem verification shows `data/` and `orchestration/logs/` present and empty, `orchestration/airflow.db` absent as intended, and retained `orchestration/airflow.cfg`, `.env`, and `airflow-jwt-key.txt` present. | Reinitialize Airflow SQLite metadata and recreate/verify the `cds_api` pool, then verify the fresh Airflow state before closing P6. |
| 2026-10-04 | P6 Airflow metadata reinitialization | User ran `airflow db migrate`; Airflow created a fresh SQLite metadata database at `orchestration/airflow.db` and reported migration success. `airflow db check` returned connection successful. Pool `cds_api` was recreated with **1 slot** and the expected description. The new SQLite file is present (~724K). Airflow emitted a Starlette/httpx deprecation warning during migration, but migration completed successfully. The final DAG-list grep returned no matching output, so DAG parse/registration is not yet verified. | Diagnose DAG discovery/import state with `airflow dags list-import-errors` and an unfiltered DAG list before closing P6. |
| 2026-10-04 | P6 standalone runtime decision | User confirmed that local Airflow should be run with `airflow standalone` rather than separate scheduler/API-server/DAG-processor/triggerer terminals. Immediately after fresh metadata initialization, both `airflow dags list-import-errors` and `airflow dags list` returned `No data found`, consistent with a fresh metadata DB before a DAG processor has parsed the DAG. Updated the orchestration README so standalone is the canonical local runtime and documented this fresh-metadata behavior. | Start standalone with scheduling disabled, allow DAG parsing, then verify `cds_era5_land_etl` appears and no import errors are recorded. |
| 2026-10-04 | P6 closure | After starting the fresh local Airflow runtime, user verified `airflow dags list` contains `cds_era5_land_etl` from `orchestration/dags/cds_era5_land_etl.py`, and `airflow dags list-import-errors` returns `No data found`. This closes DAG discovery/import verification on the recreated SQLite metadata. P6 now has complete evidence for guarded reset, PostgreSQL recreation/bootstrap, filesystem cleanup, Airflow metadata recreation, pool recreation, retained configuration/credentials, and fresh DAG registration. | P6 complete. Next phase is P7 controlled runtime verification; do not begin live CDS work until explicitly requested. |
| 2026-10-04 | P7 offline scope | User explicitly prohibited live CDS execution and schedule enablement during P7, prioritizing refactor verification and correct tests. Audited current orchestration/integration coverage: DAG graph/policy, first-pass barrier, simulated pacing deferral/retry, offline scheduled selection with an assertion that unexpected CDS access fails, progress/load evidence, campaign repair, isolated PostgreSQL idempotency/reconciliation/pacing/bootstrap, and reset guards. Redefined P7 as an offline-only gate; live manual and scheduled pilots are deferred outside P7. | Run fresh full unit/orchestration regression and all four isolated PostgreSQL integration modules; review any warnings/skips before further changes. |
| 2026-10-04 | P7 offline regression closure | User ran the full offline unit/orchestration suite with scheduling disabled: **271 passed in 7.72s**. User then ran all four isolated PostgreSQL integration modules: **6 passed in 1.27s**. No failures or skips were reported. Source/test audit confirmed scheduled-selection tests explicitly fail on unexpected CDS access; CDS client/transport behavior in unit tests is mocked/faked; integration tests use only isolated `TEST_POSTGRES_*` resources. The passing suite covers DAG parsing/policy, five-task graph, retry/timeout/pool/concurrency, first-pass barrier, bounded retry/pass-through, pacing deferral, +0/+1/+2-hour slot metadata, failed-quarter blocking, terminal no-op, manual preparation/repair, load evidence, idempotency/rollback, bootstrap, pacing serialization, and reset guards. | P7 offline gate complete. Keep live CDS disabled and campaign scheduling off; live manual/scheduled pilots remain deferred to a separately approved rollout phase. |
| 2026-10-04 | Post-P7 mapped-instance observability | User approved keeping DAG ID `cds_era5_land_etl` and displaying mapped monthly instances as `YYYY-MM`. The initial observability change renamed the Airflow retry/pass-through task from the old `extract_or_resume` task ID to `resolve_raw_checkpoint` and added named mapping labels. This entry is superseded by the subsequent checkpoint naming alignment, which also renames the pipeline API and remaining Airflow task identities to postcondition-oriented checkpoint names. | Run focused DAG/operator tests, then the full offline unit/orchestration regression before resetting/restarting the development Airflow runtime. |
| 2026-10-04 | Checkpoint naming alignment | User approved postcondition-oriented pipeline API names `ensure_raw_checkpoint()` and `ensure_transformed_checkpoint()`, plus Airflow task identities `attempt_raw_checkpoint [YYYY-MM]`, `prepare_retry_pass`, `resolve_raw_checkpoint [YYYY-MM]`, `ensure_transformed_checkpoint [YYYY-MM]`, and `load [YYYY-MM]`. Runtime boundary metadata and orchestration helper names were aligned so code, tests, logs, and Airflow UI use the same checkpoint terminology. Historical references to old task IDs remain marked as historical where needed. | Run focused checkpoint/DAG tests and full offline regression before resetting/restarting Airflow. |
| 2026-10-04 | Campaign environment source of truth | Moved `CAMPAIGN_KEY`, `CAMPAIGN_START_YEAR`, and `CAMPAIGN_END_YEAR` from hard-coded orchestration constants to required environment-backed configuration. Added startup/config validation, CI values, campaign-key context in durable campaign `configuration_identity`, and matching selection/reporting/manual-repair identity construction. Updated current-state environment documentation and config-driven tests. | Run focused campaign/config/reporting tests and the full offline + PostgreSQL regression before restarting Airflow. |
| 2026-10-05 | Failure-only RAW retry barrier | The first clean scheduled pilot completed successfully and exposed unnecessary idle time: successful monthly RAW maps were held behind the global retry-preparation barrier. Rewired the DAG so each successful `attempt_raw_checkpoint` maps directly to `resolve_raw_checkpoint` pass-through, then transform/load. Failed first results alone defer on durable per-progress extraction state until all expected first attempts are terminal, then retry. Removed the Airflow `prepare_retry_pass` task and aligned DAG-local result/input variable names with final task identities. | Run focused DAG/operator/progress tests, full offline regression, and PostgreSQL integration regression before the next live pilot. |
| 2026-10-05 | S1-S5 structural refactor closure | User verified the final S5 state with **170 focused tests in 5.27s**, **283 unit/orchestration tests in 8.88s**, and **6 isolated PostgreSQL integration tests in 1.36s**. S1-S5 module moves are complete; legacy root/core modules were removed rather than retained as facades. Documentation reconciliation corrected current paths and the five-task DAG description. | Structural refactor complete. Keep scheduling disabled; live CDS/manual/scheduled pilots require separate approval. |

Current next action: structural refactor S1 through S5 is complete and offline-verified. Keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false`; perform documentation/dependency cleanup only unless a separately approved rollout phase authorizes live CDS work. Preserve `pipeline/` as the ETL/scientific implementation owner.


## 10. P1 audit evidence — 2026-10-03

### 10.1 Verification results and limits

All repository reads used `dev` or its exact commit
`2f8a93e509ac85ea8ac4ac4a7efffae18e32fc69`. No application code was edited.

| Check | Evidence | Result / limit |
| --- | --- | --- |
| Existing CI at the exact baseline | [Run 36558576961](https://github.com/esnanta/etl-weather/actions/runs/36558576961), job `109373628429`, push on 2026-09-29 | Python 3.12.14, pytest 7.4.4; 213 collected, **208 passed, 5 skipped in 6.31s** |
| CI suite breakdown | Retrieved job log | 97 orchestration tests and 111 unit tests passed |
| Skipped CI tests | Log shows pacing `s`, reconciliation `ss`, idempotency `ss` | All five PostgreSQL integration cases skipped; not evidence of database behavior |
| Initial assistant-environment pytest attempt | `python -m pytest tests/unit tests/airflow_orchestration -q` | Blocked before collection: `No module named pytest`; this is an environment failure, not a test failure |
| Initial assistant-environment installation attempt | `pytest==7.4.4` in a temporary virtual environment | Package access returned no matching distribution, including explicit PyPI index attempt; repository requirements were not changed |
| New direct behavior checks | Imported original pure helpers, no replacement dependencies | **35 assertions passed**; these are not pytest suite results |
| Python syntax | AST parsing of source and test files | **80 files parsed successfully** |
| Core dependency boundary | AST import scan of `pipeline/` | **26 modules** without Airflow/orchestration imports |
| Conservative internal dependency graph | Includes function-local and type-only imports | **47 runtime/script modules, 95 edges, no detected cycles**; not a proof of all dynamic imports |
| Assistant host/database access | No access to the user's host or configured `TEST_POSTGRES_*` values in the assistant environment | The assistant did not execute database queries, a reset, live DAGs, or CDS submissions |
| User local unit/orchestration run, 2026-10-03 | Terminal output supplied by the user after activating the project environment | **208 passed in 6.15s**; no failures or skips |
| User local PostgreSQL integration run, 2026-10-03 | Terminal output supplied by the user for the three integration modules | **5 passed in 1.30s**; no failures or skips |

The 35 direct assertions cover manual/scheduled input validation, schedule
defaults and accepted/invalid cadences, disabled-schedule behavior, Jakarta
interval conversion and invalid boundaries, 40-quarter bounds, and upstream
metadata validation. They do not exercise Airflow, PostgreSQL, extraction, or
scientific transformations.

No baseline regression was observed in the available evidence. The user completed
the local verification on 2026-10-03 and supplied both successful terminal outputs.
These are user-executed results, separate from the assistant's earlier limited
environment checks and the historical CI run. The user's local checkout SHA was
not included in the output; do not attribute the local runs to an independently
verified commit. P1's local verification gate is now complete.

The earlier dependency limitation applied to the assistant environment. It is not
evidence of a defect in `requirements.txt`; no dependency changes are required by
these results.

Commands executed by the user (quiet output):

```bash
source activate-airflow.sh
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit tests/airflow_orchestration -q
```

```bash
source activate-airflow.sh
python -m pytest \
  tests/integration/test_postgresql_idempotency.py \
  tests/integration/test_pipeline_progress_reconciliation.py \
  tests/integration/test_submission_pacing.py \
  -q
```

The integration run covers idempotency/rollback, progress reconciliation, and
submission pacing. Its five passes replace the earlier absence of local database
test evidence; they do not prove live scheduler behavior or completion of P6/P7.
Continue using the isolated `TEST_POSTGRES_*` configuration for future integration
runs. Historical pilot counts and old test results remain separate.

### 10.2 Current dependencies and relocation impact

| Current owner | Direct consumers / dependency | Required treatment |
| --- | --- | --- |
| `scripts/config.py` | `scripts/run_pipeline_cds.py`, `scripts/read_daily_climate.py`, `tasks/era5_land.py`, `tests/unit/test_config_requests.py` | Move shared values and request factory; update all four consumers |
| `tasks/era5_land.py` | `campaign.py`, `tasks/progress.py`; tests for tasks, progress, campaign, and progress wiring | Move request planning and availability setting out of task modules |
| `tasks/progress.py` | DAG, extraction operator, wiring tests, offline selection pilot | Separate job preparation, boundary wrappers, shared recording, and repair |
| `pipeline.database.DatabaseConfig` | Orchestration pacing/progress, report/capacity scripts, pipeline loader/read repository | Reuse credentials and connection behavior |
| `pipeline/database/config.py` | `pipeline/database/__init__.py`, schema helpers and their consumers | Import root schema setting while retaining existing exports/defaults |
| `orchestration/reporting.py` | Report entrypoint and reporting tests | Replace literal SQL schemas; derive report campaign labels/counts from campaign policy without changing output |
| `pipeline/model.py` | Request configuration and pipeline functions | Imports Pandas and xarray; do not pull this dependency into a lightweight root settings module |
| `pipeline/contracts/era5_land.py` | Scientific pipeline contracts | Keep scientific constants/algorithms under their existing owner; no changes planned in this audit |

The existing graph has no detected cycle, but ownership is inverted where
`campaign.py` imports task-level request planning and availability policy.
Moving those to `orchestration/requests.py` and `orchestration/config.py`
removes that dependency.

Finalized direction for implementation:

- Root `config.py` stores lightweight application settings, including schema,
  dataset selection, bbox coordinates, requested variable values, and standalone
  default dates. It imports neither pipeline infrastructure nor orchestration.
- Root `request_factory.py` owns `build_cds_request()` and typed request/bbox
  construction, consuming root settings and existing pipeline models. Preserve
  the current request defaults, copied variable list, and credential-check behavior.
- `scripts/config.py` is replaced after updating audited consumers; do not retain
  a second definition of the same settings.
- `pipeline/database/config.py` imports only the root schema value; preserve
  `SCHEMA_NAME` as the existing public name and retain `DatabaseConfig`.
- `orchestration/config.py` owns operational settings. Do not change scientific
  constants merely because some operational defaults have the same values.
- Use the task/monitoring/interval destinations in section 5. Shared recording
  belongs in `orchestration/progress_tracking.py`; whole-month campaign repair
  belongs in `orchestration/campaign_repair.py`.
- Put persisted-load verification in
  `orchestration/repositories/load_evidence.py`. Keep repositories below task
  coordination; they must not import tasks, campaign repair, or the database
  bootstrap coordinator.
- Preserve the independent, read-only behavior of reporting. Reports must not
  start creating schemas or reconciling state as a side effect.

The intended direction is settings -> request factory/database configuration ->
request and repository helpers -> progress/repair coordination -> tasks -> DAG.
Dependencies point toward lower-level helpers; root settings and repositories
never import their callers. Recheck actual imports after each implementation batch.

### 10.3 Schema, settings, and engine inventory

| Location | Existing value or literal |
| --- | --- |
| `pipeline/database/config.py` | `SCHEMA_NAME = "climate_data"`; canonical weather table names |
| `repositories/pipeline_progress.py` | `PIPELINE_PROGRESS_SCHEMA = "climate"`; two progress table names; hard-coded `climate_data.daily_climate*` joins |
| `repositories/submission_pacing.py` | `PACING_SCHEMA = "climate"`; pacing key/table, interval env name, 1,800-second default |
| `tasks/progress.py` | Hard-coded `climate_data.daily_climate*` load-evidence query; one-hour slot calculation |
| `orchestration/reporting.py` | Literal `climate.pipeline_progress*` queries; repeated 2016–2025 bounds and 40/4/3 display counts |
| `scripts/inspect_campaign_capacity.py` | Five fully qualified table names spanning both schemas |
| `schedule.py` | Enable/cadence env names, `2h` default, three-entry cron mapping |
| `campaign.py` | Campaign key, 2016/2025 bounds, operational timezone |
| `tasks/era5_land.py` | Operational timezone, six-day availability lag, 5 GiB warning threshold, GiB conversion |
| DAG and `progress.py` | Repeated DAG/pipeline identity; task execution policies |
| `scripts/config.py` | Aceh bbox, dataset, five variables, standalone dates 2022-01-01 through 2022-01-03 |

The standalone default dates are not the campaign range; do not unify their
values during configuration movement. Status vocabularies are internal contracts,
not user-tunable environment settings.

Pacing and progress each open a runtime engine and dispose it in `finally`.
The report script does likewise; capacity inspection adds a five-second connection
timeout. Preserve that timeout and read-only behavior if sharing helpers.
`create_database_engine()` additionally ensures the database exists; normal
orchestration currently does not do that.

`prepare_monthly_jobs()` currently initializes the weather schema/tables and
progress tables. Pacing creates its own table at submission-gate time.
Extracting a bootstrap helper must preserve invocation timing and transaction
boundaries; maintenance recreation remains explicit.

### 10.4 Exact external-file scope for later implementation

This records notice before implementation; this audit changes only this document.
Reannounce any additional path before editing it.

| Files outside orchestration | Planned reason |
| --- | --- |
| `config.py`, `request_factory.py` (new) | Lightweight shared settings and typed request factory |
| `scripts/config.py` | Relocate definitions/factory and retire the old owner |
| `scripts/run_pipeline_cds.py`, `scripts/read_daily_climate.py` | Update request/default imports; preserve command behavior and typed bbox |
| `scripts/inspect_campaign_capacity.py` | Derive the five table names from shared settings |
| `pipeline/database/config.py` | Replace only the duplicate schema definition with the shared import; retain the public interface |
| `tests/unit/test_config_requests.py` | Update factory imports/monkeypatch targets and protect credential behavior |
| `tests/airflow_orchestration/test_tasks.py`, `test_progress.py`, `test_campaign.py` | Update moved helper/constant references |
| `tests/airflow_orchestration/test_progress_wiring.py`, `test_scheduled_selection_pilot.py` | Update recording/setup/selection monkeypatch targets after splitting progress wrappers |
| `tests/airflow_orchestration/test_submission_pacing.py`, `test_submission_log.py`, `test_pipeline_progress.py`, `test_reporting.py`, `test_schedule.py` | Update settings/engine owners and schema expectations while preserving behavior assertions |
| `tests/integration/test_pipeline_progress_reconciliation.py`, `test_submission_pacing.py` | Update schema/configuration references and test isolation targets |
| `README.md`, `tests/AGENTS.md`, `docs/AIRFLOW_BEST_PRACTICE.md`, `docs/AIRFLOW_CODING_GUIDELINES.md` | Align configuration location, operational schema, current mapped DAG graph, and first-pass documentation |

No changes are currently required to `scripts/report_campaign_progress.py` if
its public reporting interface and independent engine lifecycle are retained.
Likewise `tests/unit/test_database.py`, canonical loader/repository tests, and
`test_postgresql_idempotency.py` should remain unchanged because canonical
weather table names remain `climate_data.*`; run them as regression checks.
The DAG and operator tests should continue using their stable public identities.

No edits are planned to scientific pipeline modules, `requirements.txt`,
Airflow SQLite configuration, activation/JWT scripts, or CI workflow. This is
a narrower pipeline change than relocating the entire database package.

### 10.5 Execution contract captured from source

| Task ID | Retries | Explicit timeout | Additional policy |
| --- | ---: | --- | --- |
| `build_monthly_requests` | 0 | 5 minutes | Resolve run mode and initialize/select progress |
| `attempt_raw_checkpoint` | 0 | 6 hours | Mapped, `cds_api`, `first_pass=True` |
| `resolve_raw_checkpoint` | 1 | 6 hours | Mapped, `cds_api`; immediate pass-through for successful first results, durable-barrier wait only for failed first results, then bounded retry with 10-minute delay, exponential backoff, maximum 1 hour |
| `ensure_transformed_checkpoint` | 0 | 2 hours | Mapped from resolved extraction |
| `load` | 1 | 2 hours | Mapped; 5-minute delay, exponential backoff, maximum 30 minutes |

Keep the five-task dependency order in section 6. The DAG start date is fixed at
2026-01-01 Asia/Jakarta. Earliest-start slots are lower bounds, not a guarantee
that maps execute or finish in index order. The retry barrier does not establish
a global chronological order among retry-pass maps.

Payload contracts:

| Boundary | Existing fields |
| --- | --- |
| Prepared monthly job | `request_spec`, `progress_id`, `run_mode`, `earliest_start_at` |
| Successful extraction/transformation | `boundary`, `request`, `request_key`, `artifact_path`, `artifact_status`, plus progress ID and mode |
| First-pass failure | `first_attempt_failed=True`, `request_spec`, `progress_id`, `run_mode`, `earliest_start_at` |
| Resolver mapping input | `request_spec`, `progress_id`, `run_mode`, `first_result` |
| Successful load | `boundary`, `request`, `request_key`, `artifact_path`, `daily_climate_id`, `rows_processed`, `load_status`, plus progress ID and mode |

Successful first-pass results are returned directly by the resolver. Resolver
mapping input is derived per mapped first-attempt result, so successful months do
not wait for other first-pass maps. Failed resolver maps poll durable extraction
state before retrying. Pacing deferral propagates without marking the
partition failed; wakeup calls execution again and rechecks the shared gate.

Manual configuration must contain exactly the two canonical dates. Scheduled
configuration must be empty; scheduled jobs select from campaign progress, not
the Airflow data interval. The interval helper is currently independent of that
selection path. A naive explicit `slot_anchor` is rejected even in manual mode;
do not change that validation while moving code.

Progress completion remains dependent on matching persisted request/source/
dataset/period, transformed path/checksum, positive observation count, and every
expected detail. A manual repair must match an exact whole campaign month.
Keep the order of record, reconcile, and claim-release calls.

### 10.7 Post-pilot orchestration follow-up agenda — 2026-10-05

Do not implement these items while the current scheduled DagRun is active.
The 2026-10-04/05 live pilot exposed behavior that is correct under the current
graph but unnecessarily delays successful monthly partitions.

- [x] **Make the retry barrier failure-only.** A monthly partition whose
      `attempt_raw_checkpoint [YYYY-MM]` first pass succeeds must continue
      immediately through `resolve_raw_checkpoint [YYYY-MM]` as a pass-through,
      then `ensure_transformed_checkpoint [YYYY-MM]`, then `load [YYYY-MM]`.
      It must not wait for other months to finish their first pass.
- [x] **Keep the global barrier only for failed first attempts.** A first-pass
      failure must remain bounded control metadata, allow the other first-pass
      months to continue, and become eligible for retry only after all first-pass
      attempts have reached a terminal first-pass result.
- [x] **Prevent duplicate downstream execution.** A successful month must have
      exactly one resolved RAW result and one transform/load path. A failed month
      must not enter transform/load until its retry resolver succeeds.
- [x] **Use CDS pacing wait time for local work.** With the operational
      `SUBMISSION_INTERVAL_SECONDS=300` pilot setting, successful RAW
      months should be free to transform and load while another monthly CDS
      submission is waiting for its provider slot.
- [x] **Align DAG-local Python names with Airflow task IDs.** Remove transitional
      names such as `first_attempts`, `retry_jobs`,
      `resolved_raw_checkpoint`, and generic `transformed`. The DAG now uses
      `attempt_raw_checkpoint_results`, `resolve_raw_checkpoint_inputs`,
      `resolve_raw_checkpoint_results`,
      `ensure_transformed_checkpoint_results`, and `load_results`.
- [x] **Audit the same naming drift outside the DAG before editing.** Search
      orchestration code, tests, and current-state documentation for local
      variables/helpers that still describe the old extraction/transform naming
      or no longer match the final task/boundary terminology. Historical records
      may retain old names when explicitly marked as historical.
- [x] Add focused DAG/operator/repository tests proving that one successful mapped month can reach
      transform/load while another first-pass month remains running/deferred,
      while a failed month still waits for the first-pass barrier.
- [x] Run the focused DAG/operator suite, full unit/orchestration regression, and
      PostgreSQL integration regression before the next live scheduled pilot.
      Verified locally after the failure-only barrier refactor: 40 focused
      orchestration tests passed, 283 unit/orchestration tests passed, and
      6 PostgreSQL integration tests passed.


### 10.8 Structural package and module naming agenda — 2026-10-05

This is a planning agenda only. Do not move modules or change imports before a
dedicated dependency/caller audit and a clean regression baseline. The user
requested that package roots become easier to scan and that module names state
their actual responsibility rather than only a broad provider/dataset label.

#### Structural principles

- [x] Use a deliberately small `orchestration/` root. The agreed target
      direction is:

      ```text
      orchestration/
      ├── __init__.py
      ├── airflow.cfg
      ├── config.py
      ├── README.md
      ├── dags/
      ├── operators/
      ├── tasks/
      ├── repositories/
      ├── campaign/
      ├── scheduling/
      ├── monitoring/
      └── docs/
      ```

      Minor adjustments remain allowed if the caller/dependency audit shows a
      clearer responsibility boundary.
- [ ] Group orchestration modules by responsibility rather than accumulating
      peer files at the package root. Candidate groups to evaluate include
      campaign policy/state, scheduling/request planning, monitoring/runtime
      support, repositories, operators, tasks, DAGs, and documentation.
- [ ] Do not create folders solely to reduce the root file count. Every new
      subpackage must have a coherent ownership boundary and must reduce import
      ambiguity rather than add indirection.
- [ ] Prefer names that describe behavior/postcondition. Avoid names that expose
      an incidental provider when the module implements a generic orchestration
      concern, and avoid generic names when the implementation is actually
      ERA5-Land-specific.
- [ ] Remove compatibility facades when all callers have migrated instead of
      preserving empty or misleading aliases indefinitely.

#### Orchestration naming findings

- [x] Treat `orchestration/tasks/era5_land.py` as a temporary compatibility
      facade to remove after caller migration; do not rename it into another
      long-lived facade.
- [x] Rename `orchestration/repositories/submission_pacing.py` to
      `orchestration/repositories/submission_pacing.py`. The module owns the durable provider-submission clock. Its table, functions, and identifiers are provider-neutral because orchestration currently uses one global submission clock.
- [x] Move the custom deferrable operator out of `tasks/` to
      `orchestration/operators/raw_checkpoint.py` and rename
      `RawCheckpointOperator` to `RawCheckpointOperator`. The implementation
      still requires an audit to isolate any provider-specific exception or
      pacing dependency cleanly.
- [ ] Audit root modules `campaign.py`, `campaign_repair.py`, `database.py`,
      `intervals.py`, `mode.py`, `monitoring.py`, `pacing.py`,
      `progress.py`, `progress_tracking.py`, `reporting.py`,
      `requests.py`, and `schedule.py` and propose coherent subpackages
      before moving any of them.

#### Pipeline package and naming findings

- [ ] Audit the root `pipeline/` modules (`interfaces.py`, `model.py`,
      `request.py`, `runtime.py`, and `logging.py`). Keep a root module only
      when it is genuinely package-wide; otherwise group it by responsibility.
- [ ] Audit `pipeline/extraction/cds.py` before renaming or splitting it. It
      currently combines credentials, CDS payload construction, ERA5-Land
      local-day context, monthly chunking, chunk-cache resume, download, and
      extractor behavior. Explicitly separate what belongs to provider transport
      from ERA5-Land retrieval policy and resumable chunk extraction before
      choosing the final module boundaries.
- [x] Keep `pipeline/contracts/era5_land.py` provider-independent. Its UTC+7
      daily semantics, accumulated/mean-variable classes, and output units are
      scientific ERA5-Land contracts. Separately, audit the current
      `pipeline/providers/cds.py` during extraction/provider restructuring;
      provider naming must follow its final responsibility rather than filename
      symmetry with the contract module.
- [x] Rename `pipeline/transformation/era5_land.py` to
      `pipeline/transformation/era5_land_daily.py`; the module represents
      ERA5-Land hourly-to-local-daily transformation.
- [x] Split `pipeline/validation/era5_land.py` by contract boundary into
      `pipeline/validation/era5_land_raw.py` and
      `pipeline/validation/era5_land_daily.py`.
- [x] Keep repository ownership responsibility-based rather than enforcing one
      file per physical table. `daily_climate` and `daily_climate_detail`
      form one canonical climate aggregate/read model.
- [x] Rename `pipeline/repositories/daily_climate.py` to
      `pipeline/repositories/daily_climate_reader.py` for its current read-only
      modelling-query responsibility.
- [x] Keep operational persistence for `pipeline_progress`,
      `pipeline_progress_detail`, and `submission_pacing` under
      `orchestration/repositories/`; do not mirror those tables into
      `pipeline/repositories/` merely for table symmetry.

#### Agreed naming decisions

The following names are now the preferred targets unless the dependency audit
uncovers a concrete conflict:

```text
orchestration/repositories/submission_pacing.py
orchestration/operators/raw_checkpoint.py
RawCheckpointOperator

pipeline/contracts/era5_land.py
pipeline/repositories/daily_climate_reader.py
pipeline/transformation/era5_land_daily.py
pipeline/validation/era5_land_raw.py
pipeline/validation/era5_land_daily.py
```

`pipeline/extraction/cds.py` remains intentionally unresolved until the
provider-transport versus ERA5-Land-retrieval-policy audit is complete.

#### Required implementation gate

- [ ] Produce a caller/import inventory before any move or rename.
- [ ] Define the proposed target tree and old -> new module mapping before edits.
- [ ] Preserve public runtime behavior, request/checkpoint identity, database
      schema/table ownership, and scientific calculations.
- [ ] Update imports, exports, tests, documentation, and any monkeypatch targets
      in the same refactor batch; do not leave long-lived compatibility aliases
      unless an external caller requires them.
- [ ] Run focused tests after each package group, then the full
      unit/orchestration suite and all isolated PostgreSQL integration tests
      before the next live pilot.

### 10.6 Findings to preserve or review separately

1. **Broad first-pass exception handling.** The operator catches all
   `Exception` values from extraction in first-pass mode and returns failure
   control metadata. Retry-pass exceptions propagate to Airflow. This does not
   implement the guideline's transient/permanent classification. Preserve it
   during structural movement; a policy fix needs its own decision and tests.
   A worker termination or Airflow failure outside that handler is not proven to
   follow the same barrier path.
2. **Resolved retry-preparation finding.** The former Airflow
   `prepare_retry_pass` task had no explicit timeout. The 2026-10-05
   failure-only barrier refactor removed that global task entirely, so this
   finding no longer applies to the current graph.
3. **Heavy transitive imports.** Root request configuration must not simply copy
   the existing import of `pipeline.model` into the lightweight settings layer.
   Model imports load Pandas/xarray. The factory boundary addresses this without
   changing the model.
4. **Monitoring path and logger identity.** Moving `era5_land.py` helpers changes
   `__file__` depth and potentially `logging.getLogger(__name__)`. Resolve the
   same repository `data/` directory and preserve logger identity/content where
   consumers or tests rely on it. RSS remains process high-water RSS, not
   per-task allocated memory.
5. **Tests depend on old patch locations.** Several tests monkeypatch
   `tasks.progress` or `tasks.era5_land`. Update patches at the new call sites;
   passing old tests with unused patches is not evidence of equivalent behavior.
6. **Coverage limits.** Existing tests cover graph/barrier, simulated retries,
   pacing wakeup, job offsets, and manual mode. They do not directly establish
   future `earliest_start_at` operator deferral, absence of external I/O during
   DAG import, every timeout/backoff field, or a real scheduled worker ordering.
   Add focused coverage during the relevant refactor phase, not blanket tests.
7. **Documentation and dependency drift.** Airflow guidance must distinguish
   the current five-task orchestration graph from the three pipeline runtime
   postcondition boundaries. `requirements.txt`
   contains Dask/Zarr/Google storage packages despite the documented ARCO removal.
   No active corresponding provider path was found in the runtime import audit.
   Dependency cleanup is separate; do not alter pins or claim these packages are
   removable without checking transitive requirements.
8. **Live state remains unknown.** This audit reads source and CI, not the user's
   current Airflow processes or PostgreSQL contents. P6 checks remain necessary
   before the already-planned development reset.


### 10.9 Dependency/import audit and proposed target tree — 2026-10-05

This section records the dependency audit performed against branch `dev` at
commit `672dafd3684dd4929d46fc47f00c889028552449`. It is a structural plan only;
no source module is moved by this section.

The latest verified regression baseline before structural moves is:

```text
focused orchestration     : 40 passed
unit + orchestration      : 283 passed
PostgreSQL integration    : 6 passed
```

#### Audit conclusions

1. The current `orchestration/` root mixes campaign policy, scheduling,
   progress definitions/tracking, database bootstrap, monitoring, reporting, and
   provider submission gating. These responsibilities should become subpackages.
2. Add a dedicated `orchestration/progress/` package to the previously agreed
   tree. Progress identity and progress tracking are used by both scheduled and
   manual flows, so placing them inside `campaign/` would create the wrong
   ownership boundary.
3. `orchestration/tasks/era5_land.py` and
   `orchestration/tasks/progress.py` are compatibility/facade modules rather
   than distinct runtime responsibilities. Remove them after all callers are
   migrated; do not preserve them as permanent aliases.
4. `orchestration/tasks/cds_extract_operator.py` is an Airflow operator, not a
   task helper. Move it to `operators/raw_checkpoint.py` and rename the class
   to `RawCheckpointOperator`.
5. The provider-defer exception should not remain owned by
   `pipeline/extraction/cds.py`. The pipeline only invokes a submission
   callback; orchestration owns the decision to defer. Move that control signal
   to the orchestration submission gate so `RawCheckpointOperator` can be
   provider-neutral.
6. `pipeline/extraction/cds.py` has multiple responsibilities and must be split,
   not merely renamed. The audit separates:
   - generic CDS transport/session/credential work;
   - CDS ERA5-Land payload/schema mapping;
   - ERA5-Land retrieval/context/chunk-cache policy.
7. `pipeline.model` and `pipeline.request` have the largest import blast
   radius. Move them only after the narrower orchestration/module renames are
   stable. Package-level exports may be used during one refactor batch, but
   avoid long-lived compatibility modules.
8. `request_factory.py` is part of typed weather-request construction and
   should move into `pipeline/requests/`; leaving it at repository root would
   preserve the same structural problem being removed elsewhere.

#### Proposed final `orchestration/` tree

```text
orchestration/
├── __init__.py
├── airflow.cfg
├── config.py
├── README.md
├── dags/
│   └── cds_era5_land_etl.py
├── operators/
│   ├── __init__.py
│   └── raw_checkpoint.py
├── tasks/
│   ├── __init__.py
│   ├── build_monthly_requests.py
│   ├── raw_checkpoint.py
│   ├── transformed_checkpoint.py
│   └── load.py
├── repositories/
│   ├── __init__.py
│   ├── database.py
│   ├── submission_pacing.py
│   ├── load_evidence.py
│   └── pipeline_progress.py
├── campaign/
│   ├── __init__.py
│   ├── policy.py
│   ├── repair.py
│   └── reporting.py
├── scheduling/
│   ├── __init__.py
│   ├── intervals.py
│   ├── requests.py
│   ├── run_mode.py
│   ├── schedule.py
│   └── submission_gate.py
├── progress/
│   ├── __init__.py
│   ├── definition.py
│   └── tracking.py
├── monitoring/
│   ├── __init__.py
│   └── resources.py
└── docs/
    ├── IMPLEMENTATION_PLAN.md
    ├── POSTGRES_RECREATION.md
    └── REFACTOR_PLAN.md
```

The three operational persistence concerns remain under
`orchestration/repositories/`: `pipeline_progress`,
`pipeline_progress_detail`, and `submission_pacing`. The first two remain
one `pipeline_progress.py` repository because parent/detail rows form one
durable progress aggregate.

#### Orchestration old -> new mapping

| Current module | Proposed module | Action / responsibility |
| --- | --- | --- |
| `orchestration/campaign.py` | `orchestration/campaign/policy.py` | Move campaign quarter model, campaign definitions, initialization/matching, and earliest-incomplete selection. |
| `orchestration/campaign_repair.py` | `orchestration/campaign/repair.py` | Move manual whole-month campaign reconciliation. |
| `orchestration/reporting.py` | `orchestration/campaign/reporting.py` | Move campaign progress read/format reporting. |
| `orchestration/schedule.py` | `orchestration/scheduling/schedule.py` | Move cadence enable/cron resolution. |
| `orchestration/mode.py` | `orchestration/scheduling/run_mode.py` | Move manual/scheduled DagRun mode validation. |
| `orchestration/intervals.py` | `orchestration/scheduling/intervals.py` | Move Airflow interval -> inclusive local-date conversion. |
| `orchestration/requests.py` | `orchestration/scheduling/requests.py` | Move historical range validation and monthly orchestration request-spec planning. |
| `orchestration/pacing.py` | `orchestration/scheduling/submission_gate.py` | Move provider-submission reservation/defer service; own the provider-neutral `SubmissionDeferred` control signal here. |
| `orchestration/progress.py` | `orchestration/progress/definition.py` | Move cadence-neutral progress identity/partition definitions. |
| `orchestration/progress_tracking.py` | `orchestration/progress/tracking.py` | Move durable task-boundary progress recording and artifact identity checks. |
| `orchestration/monitoring.py` | `orchestration/monitoring/resources.py` | Move disk and peak-memory monitoring. |
| `orchestration/database.py` | `orchestration/repositories/database.py` | Move orchestration DB engine lifecycle and application-storage bootstrap coordination. |
| `orchestration/repositories/submission_pacing.py` | `orchestration/repositories/submission_pacing.py` | Rename durable submission-clock repository; persisted table and API names are provider-neutral; no pacing key is retained. |
| `orchestration/repositories/load_evidence.py` | unchanged | Name already states its persistence evidence responsibility. |
| `orchestration/repositories/pipeline_progress.py` | unchanged | Keep parent/detail progress aggregate in one repository. |
| `orchestration/tasks/cds_extract_operator.py` | `orchestration/operators/raw_checkpoint.py` | Move custom deferrable operator and rename `RawCheckpointOperator` -> `RawCheckpointOperator`. |
| `orchestration/tasks/extract.py` | `orchestration/tasks/raw_checkpoint.py` | Rename task helpers around `ensure_raw_checkpoint`. |
| `orchestration/tasks/transform.py` | `orchestration/tasks/transformed_checkpoint.py` | Rename task helpers around `ensure_transformed_checkpoint`. |
| `orchestration/tasks/load.py` | unchanged | Already matches boundary responsibility. |
| `orchestration/tasks/build_monthly_requests.py` | unchanged | Already matches DAG task responsibility. |
| `orchestration/tasks/era5_land.py` | remove | Compatibility facade only; migrate callers then delete. |
| `orchestration/tasks/progress.py` | remove | Compatibility/convenience facade; migrate callers to responsibility modules then delete. |

#### Proposed final `pipeline/` tree

```text
pipeline/
├── __init__.py
├── runtime.py
├── contracts/
│   ├── __init__.py
│   └── era5_land.py
├── database/
│   ├── __init__.py
│   ├── config.py
│   └── schema.py
├── extraction/
│   ├── __init__.py
│   └── cds_era5_land.py
├── interfaces/
│   ├── __init__.py
│   └── etl.py
├── loaders/
│   ├── __init__.py
│   └── daily_climate_postgres.py
├── models/
│   ├── __init__.py
│   └── weather.py
├── observability/
│   ├── __init__.py
│   └── logging.py
├── providers/
│   ├── __init__.py
│   ├── catalog.py
│   ├── cds_client.py
│   └── cds_era5_land.py
├── repositories/
│   ├── __init__.py
│   └── daily_climate_reader.py
├── requests/
│   ├── __init__.py
│   ├── factory.py
│   └── weather.py
├── storage/
│   ├── __init__.py
│   ├── artifacts.py
│   └── local_checkpoints.py
├── transformation/
│   ├── __init__.py
│   └── era5_land_daily.py
└── validation/
    ├── __init__.py
    ├── era5_land_raw.py
    └── era5_land_daily.py
```

`runtime.py` remains at the pipeline root intentionally: it is the public
composition layer that wires extraction, checkpoint storage, transformation,
validation, and loading. The package root should otherwise avoid generic
implementation modules.

#### Pipeline old -> new mapping

| Current module | Proposed module | Action / responsibility |
| --- | --- | --- |
| `pipeline/contracts/era5_land.py` | unchanged | Keep provider-independent ERA5-Land scientific/time/output contract. |
| `pipeline/model.py` | `pipeline/models/weather.py` | Move weather request, bbox, source/layer enums, dataset/artifact dataclasses. |
| `pipeline/interfaces.py` | `pipeline/interfaces/etl.py` | Move extractor/storage/transformer/loader interfaces. |
| `pipeline/request.py` | `pipeline/requests/weather.py` | Move WeatherRequest validation, serialization, partition splitting, signatures, and request keys. |
| root `request_factory.py` | `pipeline/requests/factory.py` | Move configured Aceh/CDS WeatherRequest factory and API-key requirement. |
| `pipeline/logging.py` | `pipeline/observability/logging.py` | Move pipeline metric/section/dataframe logging helpers. |
| `pipeline/providers/cds.py` | split into `pipeline/providers/catalog.py` + `pipeline/providers/cds_era5_land.py` | Keep generic source/dataset profile lookup in catalog; move CDS ERA5-Land variables/profile/schema mapping into dataset-specific provider module. |
| part of `pipeline/extraction/cds.py` | `pipeline/providers/cds_client.py` | Move CDS credentials, session/POST callback integration, and generic retrieve transport. |
| part of `pipeline/extraction/cds.py` | `pipeline/providers/cds_era5_land.py` | Move ERA5-Land CDS payload building and provider schema normalization. |
| remainder of `pipeline/extraction/cds.py` | `pipeline/extraction/cds_era5_land.py` | Keep ERA5-Land local-day context, retrieval period/chunk purpose, chunk cache/resume, and extractor composition. Rename `CdsExtractor` -> `CdsEra5LandExtractor`. |
| `pipeline/transformation/era5_land.py` | `pipeline/transformation/era5_land_daily.py` | Agreed hourly -> local-daily transformation module. |
| RAW portion of `pipeline/validation/era5_land.py` | `pipeline/validation/era5_land_raw.py` | Validate raw xarray type, variables, hourly temporal continuity, and source grid. |
| daily portion of `pipeline/validation/era5_land.py` | `pipeline/validation/era5_land_daily.py` | Validate transformed local-day dataframe time, space, values, and daily-grid contract. |
| `pipeline/repositories/daily_climate.py` | `pipeline/repositories/daily_climate_reader.py` | Agreed read-only canonical daily-climate modelling query repository. |
| `pipeline/loaders/postgres.py` | `pipeline/loaders/daily_climate_postgres.py` | Rename because loader writes the canonical daily climate aggregate, not arbitrary PostgreSQL data. |
| `pipeline/storage/local.py` | `pipeline/storage/local_checkpoints.py` | Rename to state its RAW NetCDF / transformed Parquet checkpoint responsibility. |
| `pipeline/storage/artifacts.py` | unchanged | Deterministic artifact identity/path helpers are already clear. |
| `pipeline/database/config.py` | unchanged | Database configuration/engine concern is clear inside the package. |
| `pipeline/database/schema.py` | unchanged | Canonical daily-climate schema/table bootstrap concern is clear inside the package. |
| `pipeline/runtime.py` | unchanged | Keep stable public composition API at package root. |

#### `pipeline/extraction/cds.py` responsibility split

The current functions map to the target ownership as follows:

```text
providers/cds_client.py
  _resolve_cds_credentials
  provider HTTP/session POST callback mechanics
  generic CDS retrieve/download transport

providers/cds_era5_land.py
  CDS_API_VARIABLE_NAMES / reverse mappings / ERA5-Land profile
  _build_cds_request_payload
  _normalize_cds_dataset

extraction/cds_era5_land.py
  _build_era5_land_retrieval_request
  _retrieval_chunk_purpose
  _format_context_period
  _download_cds_period / chunk-cache orchestration
  CdsEra5LandExtractor

orchestration/scheduling/submission_gate.py
  SubmissionDeferred
  reserve_or_defer
```

The pipeline submission callback remains a plain callable. It may propagate an
orchestration-owned `SubmissionDeferred`, but the pipeline does not import or
interpret that exception.

#### Verified caller/import blast radius

Critical callers found during the audit include:

- `orchestration.repositories.submission_pacing`: `orchestration/database.py`,
  `orchestration/pacing.py`, and
  `tests/airflow_orchestration/test_submission_pacing.py`.
- `orchestration.tasks.cds_extract_operator`: DAG wiring and
  `test_paced_operator.py`.
- campaign/schedule/progress root modules: task preparation/load helpers,
  reporting scripts, campaign/progress tests, and database maintenance scripts.
- `pipeline.extraction.cds`: orchestration pacing/raw-checkpoint code plus
  extraction, normalization, pacing, progress-wiring, and scheduled-pilot tests.
- `pipeline.repositories.daily_climate`:
  `scripts/read_daily_climate.py` and
  `tests/unit/test_daily_climate_repository.py`.
- `pipeline.transformation.era5_land`: transformation package exports and
  `tests/unit/test_transform.py`.
- `pipeline.validation.era5_land`: validation package exports and validation
  tests through `pipeline.validation`.
- `pipeline.model`: request factory, provider/request/runtime/storage/
  extraction/loader/repository/transformation/validation modules and many unit
  and integration tests; treat this as a high-blast-radius move.
- `pipeline.request`: orchestration request/progress/task code, storage/runtime/
  extraction/loader modules, and request/campaign tests; also high blast radius.
- `pipeline.runtime`: CLI runner, Airflow task wrappers, coding docs, and
  runtime tests. It remains unchanged to preserve the public runtime boundary.

#### Recommended implementation order

Do not execute the entire tree move in one commit. Use dependency-ordered
structural phases:

1. **S1 — orchestration leaf moves:** create subpackages; move monitoring,
   scheduling primitives, campaign/reporting modules, and repositories; update
   imports/tests with no behavior changes.
2. **S2 — orchestration operator/task alignment:** move
   `RawCheckpointOperator`, raw/transformed task helpers, relocate
   `SubmissionDeferred`, then remove the two compatibility facades.
3. **S3 — pipeline low-blast-radius renames:** daily-climate reader,
   transformation, validation split, loader/storage filenames; preserve package
   exports where useful within the same batch.
4. **S4 — provider/extraction split:** split CDS transport, CDS ERA5-Land provider
   metadata/payload logic, and ERA5-Land chunk extraction; rename extractor.
5. **S5 — high-blast-radius pipeline core moves:** move weather models,
   ETL interfaces, request utilities/factory, and observability logging after all
   narrower modules are stable.
6. After every structural phase run focused tests for affected imports. After S5,
   run the full `tests/unit tests/airflow_orchestration` regression and isolated
   PostgreSQL integration suite before any live scheduled pilot.

All structural phases must preserve request keys, checkpoint paths, database
table/schema names, scientific calculations, Airflow task IDs, mapped
`YYYY-MM` labels, and durable progress identity.


### Structural refactor phase evidence

- S1 orchestration leaf moves: implemented on `dev`; user verification reported 106 focused orchestration tests passed in 4.15s and 4 PostgreSQL integration tests passed in 0.82s on 2026-10-05.
- S2 operator/task alignment: verified by the user on 2026-10-05 with 51 focused tests passed in 4.48s and 283 unit/orchestration regression tests passed in 6.73s.

- S3 pipeline low-blast-radius renames: verified by the user on 2026-10-05 with 71 focused tests passed in 2.28s and 283 unit/orchestration regression tests passed in 7.24s.

- S4 provider/extraction split: verified by the user on 2026-10-05 with 61 focused tests passed in 1.02s and 283 unit/orchestration regression tests passed in 7.34s.

- S5 high-blast-radius pipeline core moves: verified by the user on 2026-10-05 with 170 focused tests passed in 5.27s, 283 unit/orchestration regression tests passed in 8.88s, and 6 isolated PostgreSQL integration tests passed in 1.36s.


### 10.10 Post-refactor cleanup audit — 2026-10-05

This audit was performed after S1-S5 verification at `dev`
`53d1fa95274ac8e86f4f983b8998b67610f81ed9`. It is an audit only:
no Python/runtime behavior, dependency pins, database state, Airflow state, or
schedule setting was changed.

#### Clean findings

- The final source tree matches the agreed S1-S5 responsibility layout. Legacy
  root modules such as `pipeline/model.py`, `pipeline/request.py`,
  `pipeline/interfaces.py`, `pipeline/logging.py`, root
  `request_factory.py`, and the pre-split CDS modules are absent from the
  current tree rather than retained as compatibility facades.
- Current-state root/orchestration documentation was reconciled to the new
  module paths and the current five-task Airflow graph.
- No active `pipeline/` source import of Airflow or `orchestration` was found
  in the cleanup audit. The core ETL boundary therefore remains Airflow-neutral.
- Public package exports used by `pipeline/runtime.py` for storage,
  transformation, validation, and loading remain coherent. No cleanup is
  required merely to reduce `__all__` lists.

#### Cleanup candidates

1. **Unused compatibility wrapper: `scripts/config.py`.**
   The file describes itself as compatibility imports, but repository code
   search found no active Python caller importing `scripts.config`. Current
   entrypoints import root `config.py` and `pipeline.requests.factory`
   directly. Before deletion, run a final exact caller search and the full
   offline regression. Documentation references should be updated in the same
   change.
2. **Provider catalog back-edge.**
   `pipeline.providers.catalog` imports ERA5-Land profiles from
   `cds_era5_land`, while `build_cds_request_payload()` performs a
   function-local import back to the catalog. This avoids an import-time cycle
   today, but it is an avoidable two-way dependency. Any cleanup should preserve
   request normalization/error behavior and receive focused provider/request
   tests; it is not urgent enough to change during this audit.
3. **Dependency-manifest drift.**
   `requirements.txt` still pins `dask`, `zarr`, `gcsfs`, and Google
   Cloud storage packages, while active source search found no Dask, Zarr,
   GCSFS, or Google Cloud Storage imports. This conflicts with the documented
   ARCO-removal policy. However, the manifest is a full pinned environment,
   so absence of direct imports alone is insufficient evidence that every pin
   may be deleted. Dependency cleanup must first establish direct-vs-transitive
   requirements in an isolated environment, then run `pip check`, the full
   283-test offline suite, and the six PostgreSQL integration tests.
4. **Tracked IDE metadata.**
   `.idea/` remains tracked. This is repository hygiene rather than a runtime
   or S1-S5 correctness issue. Remove it only as a separate housekeeping change
   if the project no longer intends to version shared IDE settings.

#### Items intentionally not treated as cleanup defects

- Historical old paths/counts inside the audit/mapping/work-log portions of this
  document and `IMPLEMENTATION_PLAN.md` are retained as historical evidence
  when clearly framed as past state.
- Package-level exports in `pipeline.storage`, `pipeline.validation`,
  `pipeline.transformation`, and `pipeline.loaders` are currently used as
  stable composition APIs and should not be flattened merely for stylistic
  consistency.
- Live CDS/manual/scheduled pilots are rollout verification, not cleanup work.
  Keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` during cleanup.

#### Recommended cleanup order

1. Remove `scripts/config.py` only after one final exact caller audit.
2. Simplify the provider catalog back-edge with focused request/extraction tests.
3. Audit and prune legacy dependency pins separately; do not combine dependency
   changes with code-structure cleanup.
4. Optionally handle `.idea/` as repository housekeeping.
5. Re-run the complete offline and isolated PostgreSQL gates after code or
   dependency cleanup before considering any live pilot.

### 10.11 Environment-backed pipeline and campaign dates — 2026-10-05

The date configuration was normalized before cleanup work continued. Standalone
pipeline defaults and scheduled campaign bounds now both come from `.env`, but
remain separate settings because they have different operational risk and
semantics:

- `CDS_START_DATE` / `CDS_END_DATE` are the default inclusive period for
  standalone pipeline/read scripts and may be a small arbitrary valid date range.
- `CAMPAIGN_START_DATE` / `CAMPAIGN_END_DATE` are the bounded scheduled
  campaign range. They use canonical `YYYY-MM-DD` values and must align to
  complete calendar-quarter boundaries because the current scheduler maps each
  selected campaign unit to exactly three whole months.

The former `CAMPAIGN_START_YEAR` / `CAMPAIGN_END_YEAR` environment variables
are removed from current configuration. This avoids deriving a full-year campaign
from an arbitrary date and avoids making the standalone pipeline default silently
expand to a multi-year campaign. Scheduled campaigns now require one or more complete calendar years: the start\nmust be January 1 and the end December 31. Shorter periods are manual pilots and\ndo not redefine campaign bounds. This keeps Q1 -> Q2 -> Q3 scheduler behavior\nmeaningful without changing durable progress schema or request identity.


### 10.12 Full-year scheduled campaign contract — 2026-10-05

Scheduled campaign bounds are intentionally stricter than standalone/manual
request ranges. A scheduled campaign must span one or more complete calendar
years: `CAMPAIGN_START_DATE=YYYY-01-01` and
`CAMPAIGN_END_DATE=YYYY-12-31`. The minimum scheduled campaign is therefore
one full year. Shorter validation or CDS pilots use manual DagRuns with explicit
request dates and do not alter campaign bounds.

This decision preserves meaningful Q1 -> Q2 -> Q3 progression in the scheduler,
keeps yearly reporting intuitive, and avoids coupling short-lived pilot dates to
durable campaign identity. `CAMPAIGN_KEY` may remain the long-term logical key
while temporary test bounds are adjusted before real campaign execution.
