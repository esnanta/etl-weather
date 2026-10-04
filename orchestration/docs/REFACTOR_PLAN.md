# Orchestration Refactor Plan and Checklist

Last updated: 2026-10-04 (Asia/Jakarta)  
Branch: `dev` only  
Status: P1 through P5 complete. Final pre-reset regression: 211 unit/orchestration tests passed and 6 PostgreSQL integration tests passed. P6 database recreation is next and has not started.  
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
- [ ] Resolve old pending execution state deliberately while preserving SQLite and history.
- [ ] Record artifact retention and the consequences of resetting progress/pacing.
- [ ] Recreate only the explicitly confirmed development target after all guards pass.
- [ ] Bootstrap the five application tables in the single application schema.
- [ ] Verify schema, constraints, connection settings, and fresh progress state.
- [ ] Confirm SQLite metadata, artifact files, credentials, JWT key, and retained artifacts were not deleted.

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
| 2026-10-04 | P6 reset-all verification | User ran the guarded maintenance/component/reset-all unit suite with quiet activation; the supplied output showed only passing progress dots and no failure/error lines, but no final pytest count was included, so no exact pass count is claimed. Inspect-only `reset_all.py` then verified `APP_ENV=dev`, PostgreSQL target `localhost:5432/weather` (user `postgres`, schema `climate_data`), data/log/runtime paths, preserved `orchestration/airflow.cfg`, zero blocking Airflow task states, zero queued/running DagRuns, zero blocking Airflow processes, and zero other PostgreSQL sessions. Inspect-only completed with no reset actions executed. | All preflight guards are clear. Actual P6 reset may proceed only via `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python scripts/reset_all.py --execute` and explicit interactive confirmation. |

Current next action: P6 reset-all orchestration and guarded development PostgreSQL recreation; destructive execution remains deferred. P1 through P5 are complete. Before any destructive command, resolve and display the exact configured PostgreSQL target, verify the one-time development reset authorization, require the planned maintenance-only safety guards, keep scheduling disabled, and inspect active/queued/running/deferred Airflow work. Treat the database as non-disposable by default; only the current development contents have been authorized for one-time replacement. Preserve `pipeline/` as the ETL/scientific implementation owner.
No database reset has been performed.


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
  tests/integration/test_cds_submission_pacing.py \
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
| `repositories/cds_pacing.py` | `PACING_SCHEMA = "climate"`; pacing key/table, interval env name, 1,800-second default |
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
| `tests/airflow_orchestration/test_cds_pacing.py`, `test_submission_log.py`, `test_pipeline_progress.py`, `test_reporting.py`, `test_schedule.py` | Update settings/engine owners and schema expectations while preserving behavior assertions |
| `tests/integration/test_pipeline_progress_reconciliation.py`, `test_cds_submission_pacing.py` | Update schema/configuration references and test isolation targets |
| `README.md`, `tests/AGENTS.md`, `docs/AIRFLOW_BEST_PRACTICE.md`, `docs/AIRFLOW_CODING_GUIDELINES.md` | Align configuration location, operational schema, six-task graph, and first-pass documentation |

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
| `first_extract_attempt` | 0 | 6 hours | Mapped, `cds_api`, `first_pass=True` |
| `prepare_retry_pass` | 0 | Not explicitly set | Consume all first-pass results |
| `extract_or_resume` | 1 | 6 hours | Mapped, `cds_api`; 10-minute delay, exponential backoff, maximum 1 hour |
| `transform_or_resume` | 0 | 2 hours | Mapped from resolved extraction |
| `load` | 1 | 2 hours | Mapped; 5-minute delay, exponential backoff, maximum 30 minutes |

Keep the six-task dependency order in section 6. The DAG start date is fixed at
2026-01-01 Asia/Jakarta. Earliest-start slots are lower bounds, not a guarantee
that maps execute or finish in index order. The retry barrier does not establish
a global chronological order among retry-pass maps.

Payload contracts:

| Boundary | Existing fields |
| --- | --- |
| Prepared monthly job | `request_spec`, `progress_id`, `run_mode`, `earliest_start_at` |
| Successful extraction/transformation | `boundary`, `request`, `request_key`, `artifact_path`, `artifact_status`, plus progress ID and mode |
| First-pass failure | `first_attempt_failed=True`, `request_spec`, `progress_id`, `run_mode`, `earliest_start_at` |
| Prepared retry job | `request_spec`, `progress_id`, `run_mode`, `first_result` |
| Successful load | `boundary`, `request`, `request_key`, `artifact_path`, `daily_climate_id`, `rows_processed`, `load_status`, plus progress ID and mode |

Successful first-pass results are returned directly by the retry operator.
Retry-job preparation does not copy `earliest_start_at` as an operator argument;
preserve this existing detail. Pacing deferral propagates without marking the
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

### 10.6 Findings to preserve or review separately

1. **Broad first-pass exception handling.** The operator catches all
   `Exception` values from extraction in first-pass mode and returns failure
   control metadata. Retry-pass exceptions propagate to Airflow. This does not
   implement the guideline's transient/permanent classification. Preserve it
   during structural movement; a policy fix needs its own decision and tests.
   A worker termination or Airflow failure outside that handler is not proven to
   follow the same barrier path.
2. **Missing explicit retry-preparation timeout.** `prepare_retry_pass` has no
   declared `execution_timeout`, although the guideline asks for finite task
   timeouts. Adding one is a behavior change and is not part of P1.
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
7. **Documentation and dependency drift.** Older Airflow guidance still describes
   three runtime tasks without the full six-task graph. `requirements.txt`
   contains Dask/Zarr/Google storage packages despite the documented ARCO removal.
   No active corresponding provider path was found in the runtime import audit.
   Dependency cleanup is separate; do not alter pins or claim these packages are
   removable without checking transitive requirements.
8. **Live state remains unknown.** This audit reads source and CI, not the user's
   current Airflow processes or PostgreSQL contents. P6 checks remain necessary
   before the already-planned development reset.
