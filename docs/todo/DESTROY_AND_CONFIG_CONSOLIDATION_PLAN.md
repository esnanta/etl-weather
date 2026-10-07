# Destroy Command and Configuration Consolidation Plan

Last updated: 2026-10-07 (Asia/Jakarta)  
Branch: `dev` only  
Status: planning approved; implementation not started

## 1. Purpose

This plan separates two maintenance contracts that currently overlap and
consolidates duplicated project configuration into the root `config.py`.

The approved contracts are:

```text
reset_all.py
    conservative
    preserve/recover
    refuse ambiguous active state

destroy_all.py
    dev-only
    intentionally destructive
    stop Airflow
    wipe runtime/data/database
    preserve project configuration
    return machine to clean local-project state
```

A second approved change is to remove `orchestration/config.py` and make the
root `config.py` the single project configuration module. Existing names and
behavior in root `config.py` take precedence when equivalent configuration
already exists there.

This work must not change ERA5-Land scientific behavior, request identity,
artifact layout, PostgreSQL climate persistence semantics, or campaign logic.

---

## 2. Authority and Safety Rules

Follow:

1. `/AGENTS.md`
2. `/tests/AGENTS.md`
3. `/docs/AIRFLOW_BEST_PRACTICE.md`
4. `/docs/AIRFLOW_CODING_GUIDELINES.md`

Only branch `dev` may be modified.

Hard constraints:

- [ ] Do not modify scientific transformation or validation logic.
- [ ] Do not change request-key or artifact identity.
- [ ] Do not change the Aceh bounding box, ERA5-Land variables, units, or
      local-day semantics.
- [ ] Do not change canonical PostgreSQL weather tables or idempotency.
- [ ] Do not import Airflow into `pipeline/`.
- [ ] Keep destructive operations unavailable when `APP_ENV != dev`.
- [ ] Preserve tracked project configuration and secrets during destroy.
- [ ] Keep `env-example` free of real credentials or secrets.

---

## 3. Current State Audit

### 3.1 Reset behavior

`scripts/reset_all.py` currently composes:

```text
PostgreSQL recreation
project data cleanup
Airflow log cleanup
Airflow runtime cleanup
```

Recent changes also allow selected Airflow residual states to pass reset
preflight. That behavior was added to support a clean-slate pilot, but it makes
the meaning of reset less conservative.

Target decision:

> Reset must return to a recovery/maintenance contract and refuse ambiguous or
> resumable Airflow state.

### 3.2 Airflow runtime cleanup

`scripts/reset_airflow_runtime.py` currently:

- preserves `orchestration/airflow.cfg`;
- deletes generated Airflow SQLite/auth/PID state;
- may stop residual LocalExecutor workers;
- refuses cleanup while scheduler, API server, triggerer, or DAG processor is
  still active.

This behavior remains appropriate for a conservative runtime reset.

### 3.3 Configuration duplication

Root `config.py` already owns shared lightweight application configuration:

```text
APP_ENV policy
APPLICATION_SCHEMA
APPLICATION_TIMEZONE_NAME
ACEH_BBOX_COORDINATES
CDS_DATASET
CDS_VARIABLES
CDS_START_DATE / CDS_END_DATE environment keys and resolvers
application-environment validation/logging
```

`orchestration/config.py` currently owns additional operational values:

```text
CAMPAIGN_KEY
CAMPAIGN_START_YEAR
CAMPAIGN_END_YEAR
CAMPAIGN_START_DATE
CAMPAIGN_END_DATE
CAMPAIGN_TIMEZONE
PIPELINE_TIMEZONE
ERA5_CAMPAIGN_SCHEDULE_ENABLED key
ERA5_CAMPAIGN_SCHEDULE_CADENCE key
DEFAULT_CADENCE
CADENCE_CRON
AVAILABILITY_LAG_DAYS
DISK_WARNING_GIB
GIB
MONTHLY_SLOT_SPACING_MINUTES
PIPELINE_PROGRESS_SCHEMA
PIPELINE_PROGRESS_TABLE
PIPELINE_PROGRESS_DETAIL_TABLE
PACING_SCHEMA
PACING_TABLE
SUBMISSION_INTERVAL_SECONDS key
DEFAULT_INTERVAL_SECONDS
```

The root configuration module is already documented as safe for pipeline,
scripts, and orchestration imports. Therefore it should become the single owner.

### 3.4 Known `orchestration.config` callers

The current audit found active imports in at least:

```text
orchestration/campaign/policy.py
orchestration/campaign/reporting.py
orchestration/scheduling/schedule.py
orchestration/scheduling/requests.py
orchestration/scheduling/intervals.py
orchestration/tasks/build_monthly_requests.py
orchestration/monitoring/resources.py
orchestration/repositories/submission_pacing.py
orchestration/repositories/pipeline_progress.py
scripts/inspect_campaign_capacity.py

tests/unit/orchestration/test_reporting.py
tests/unit/orchestration/test_progress_wiring.py
tests/unit/orchestration/test_pipeline_progress.py
tests/unit/orchestration/test_scheduled_selection_pilot.py
tests/integration/orchestration/test_orchestration_database_bootstrap.py
tests/integration/orchestration/test_pipeline_progress_reconciliation.py
```

Implementation must perform a final exact search immediately before deleting
`orchestration/config.py`; this list is an audit baseline, not permission to
assume no other caller exists.

---

## 4. Phase A — Restore Conservative Reset Semantics

### Goal

Make `reset_all.py` clearly mean safe maintenance/recovery.

### Target behavior

Reset should refuse when any state could still execute or resume:

```text
queued task           -> refuse
running task          -> refuse
deferred task         -> refuse
scheduled task        -> refuse
up_for_retry          -> refuse
up_for_reschedule     -> refuse
restarting            -> refuse
queued/running DagRun -> refuse
scheduler             -> refuse
api-server            -> refuse
triggerer             -> refuse
dag-processor         -> refuse
active LocalExecutor worker -> refuse or require explicit runtime cleanup
```

Recommended implementation:

- [x] Remove reset-specific exceptions that allow deferred task instances.
- [x] Remove reset-specific exceptions that allow a running DagRun merely
      because only deferred tasks remain.
- [x] Residual LocalExecutor workers are not auto-cleaned by `reset_all.py`;
      they are blockers under the conservative reset contract.
- [x] Keep `reset_airflow_runtime.py` as the narrower guarded runtime cleanup
      command.
- [x] Preserve inspect-only default behavior.
- [x] Preserve `APP_ENV=dev` hard requirement.
- [x] Preserve exact-target confirmation before destructive reset actions.
- [x] Update reset tests so active/deferred/resumable state is rejected.

Exit condition:

> `reset_all.py` never behaves like a clean-slate destroy operation.

---

## 5. Phase B — Introduce `destroy_all.py`

### Goal

Provide one explicit development command that intentionally removes all local
runtime/data state and returns the project to a clean local state.

### Command contract

```bash
python scripts/destroy_all.py
python scripts/destroy_all.py --execute
```

Inspect-only mode should display exactly what would be destroyed.

`--execute` must require an explicit destructive confirmation, preferably:

```text
Type DESTROY to delete all local development runtime and data state:
```

A plain yes/no prompt is not preferred for this command.

### Destroy targets

Destroy should remove/recreate:

- [ ] Airflow scheduler process.
- [ ] Airflow API server process.
- [ ] Airflow triggerer process.
- [ ] Airflow DAG processor process.
- [ ] LocalExecutor worker processes.
- [ ] Generated Airflow SQLite metadata files.
- [ ] Generated Airflow auth/PID files.
- [ ] Airflow logs.
- [ ] Project `data/` contents.
- [ ] Development PostgreSQL application database, recreated with canonical
      schema/tables.

Destroy must preserve:

- [ ] source code;
- [ ] `.env`;
- [ ] `env-example`;
- [ ] `orchestration/airflow.cfg`;
- [ ] `airflow-jwt-key.txt`;
- [ ] tracked repository files;
- [ ] Python virtual environment;
- [ ] Git metadata.

### Process shutdown policy

Destroy may intentionally stop the full Airflow runtime.

Required order:

```text
enumerate Airflow processes
        ↓
SIGTERM all recognized Airflow services/workers
        ↓
grace period
        ↓
re-inspect
        ↓
SIGKILL only recognized Airflow processes still alive
        ↓
verify Airflow process count = 0
```

Safety requirements:

- [ ] Match only known Airflow command lines; never broad-kill arbitrary Python
      processes.
- [ ] Exclude the current destroy process from process matching.
- [ ] Refuse to continue if recognized Airflow processes remain after escalation.
- [ ] Log PID and command for every process selected for termination.
- [ ] Unit-test process matching using fake `/proc` fixtures and injected
      signal functions.

### PostgreSQL policy

Destroy is intentionally destructive, but database targeting must remain exact.

- [ ] Hard-deny PostgreSQL system databases.
- [ ] Display host, port, database, user, and schema before execution.
- [ ] Stop Airflow before checking remaining PostgreSQL sessions.
- [ ] Distinguish remaining external sessions from Airflow-owned sessions where
      practical.
- [ ] Do not silently terminate unrelated PostgreSQL clients by default.
- [ ] If unexplained sessions remain, fail with actionable PID/application/user
      details.
- [ ] Recreate only the configured development database.

### Failure-safe execution order

Use:

```text
validate APP_ENV=dev
        ↓
display destroy targets/current state
        ↓
require DESTROY confirmation
        ↓
stop all recognized Airflow runtime processes
        ↓
verify Airflow process count = 0
        ↓
verify PostgreSQL target/session safety
        ↓
recreate PostgreSQL
        ↓
clear project data
        ↓
clear Airflow logs
        ↓
clear Airflow generated runtime/metadata
        ↓
final clean-state verification
```

If Airflow cannot be stopped, PostgreSQL/data destruction must not begin.

### Final verification

Destroy should report:

```text
airflow_processes_remaining = 0
airflow_runtime_files_remaining = 0
data_entries_remaining = 0
airflow_log_entries_remaining = 0
database = recreated
schema = ready
canonical tables = ready
```

A final non-zero safety result must make the command fail.

---

## 6. Phase C — Share Process Helpers Without Blurring Contracts

Current process logic is split between
`reset_airflow_runtime.py` and `recreate_postgres_database.py`.

Before implementing destroy:

- [ ] Extract only genuinely shared process-discovery/signal helpers if this
      reduces duplication.
- [ ] Keep reset policy and destroy policy in their respective command modules.
- [ ] Do not create one helper whose boolean flags make reset and destroy
      semantics difficult to understand.
- [ ] Prefer explicit functions such as:

```text
list_airflow_processes()
list_airflow_services()
list_localexecutor_workers()
terminate_airflow_processes(...)
```

- [ ] Keep process detection free of Airflow imports.
- [ ] Keep helpers unit-testable through injected `proc_root`, signal, and
      sleep functions.

---

## 7. Phase D — Consolidate `orchestration/config.py` into Root `config.py`

### Ownership decision

Root `config.py` becomes the single lightweight configuration source for:

```text
application
pipeline
scripts
orchestration
campaign
scheduler
pacing
monitoring constants
```

Do not create another config wrapper or compatibility module after migration.

### Precedence rule

When a concept already exists in root `config.py`:

> Keep the root name, type, validation behavior, and documented ownership unless
> a concrete bug requires a separately reviewed change.

Examples:

- [ ] Keep `APPLICATION_SCHEMA = "climate_data"` as the schema source.
- [ ] Keep `APPLICATION_TIMEZONE_NAME = "Asia/Jakarta"` as the timezone-name
      source.
- [ ] Derived orchestration aliases must not duplicate these values merely for
      convenience unless a distinct type is required.

### Values to migrate

Move required operational constants/resolvers from `orchestration/config.py`
into root `config.py`, grouped by responsibility rather than copied verbatim.

Candidate groups:

#### Campaign

- [ ] `CAMPAIGN_KEY`
- [ ] `CAMPAIGN_START_YEAR`
- [ ] `CAMPAIGN_END_YEAR`
- [ ] `CAMPAIGN_START_DATE`
- [ ] `CAMPAIGN_END_DATE`
- [ ] campaign year/environment validation

#### Timezone

- [ ] derive `CAMPAIGN_TIMEZONE` from `APPLICATION_TIMEZONE_NAME`;
- [ ] review whether `PIPELINE_TIMEZONE` is still needed as an alias;
- [ ] remove redundant aliases when callers can use one canonical value without
      reducing readability.

#### Scheduler

- [ ] `ENABLED_ENV`
- [ ] `CADENCE_ENV`
- [ ] `DEFAULT_CADENCE`
- [ ] `CADENCE_CRON`
- [ ] `MONTHLY_SLOT_SPACING_MINUTES`
- [ ] `AVAILABILITY_LAG_DAYS`

#### Monitoring

- [ ] `DISK_WARNING_GIB`
- [ ] `GIB`

#### Progress/pacing storage

- [ ] reuse `APPLICATION_SCHEMA` rather than defining competing schema values;
- [ ] `PIPELINE_PROGRESS_TABLE`
- [ ] `PIPELINE_PROGRESS_DETAIL_TABLE`
- [ ] `PACING_TABLE`
- [ ] evaluate whether `PIPELINE_PROGRESS_SCHEMA` and `PACING_SCHEMA` should
      remain aliases to `APPLICATION_SCHEMA` or callers should import
      `APPLICATION_SCHEMA` directly.

#### Provider pacing

- [ ] `INTERVAL_ENV`
- [ ] `DEFAULT_INTERVAL_SECONDS`

### Environment resolver policy

Root `config.py` already has an `_environment(...)` helper that loads the
project `.env` when no explicit mapping is supplied.

During consolidation:

- [ ] Prefer this existing root mechanism over direct `os.getenv` where
      behavior can remain equivalent.
- [ ] Preserve required-value validation and actionable error messages.
- [ ] Decide explicitly whether missing campaign settings raise `ValueError`
      or `RuntimeError`; prefer consistency with root config unless callers
      materially depend on the old exception type.
- [ ] Do not silently introduce defaults for currently required campaign
      identity/year values.
- [ ] Keep scheduler enabled/cadence resolution testable with supplied mappings.

### Caller migration

- [ ] Replace every active `from orchestration.config import ...` with imports
      from root `config.py`.
- [ ] Update scripts and unit/integration tests in the same change set.
- [ ] Run exact repository search after migration.
- [ ] Delete `orchestration/config.py` only when active imports are zero.
- [ ] Do not leave a compatibility re-export file.

### Import safety

After consolidation:

- [ ] root `config.py` must remain lightweight;
- [ ] no Airflow imports;
- [ ] no SQLAlchemy imports;
- [ ] no PostgreSQL connection;
- [ ] no filesystem scanning;
- [ ] no CDS/API call;
- [ ] only standard-library, dotenv, constants, and pure resolver behavior.

---

## 8. Phase E — Update `env-example`

The repository uses root `env-example` (not `.env-example`).

Update it after config consolidation.

Required review:

- [ ] Keep `APP_ENV=dev` explanation and clarify that `destroy_all.py` is
      available only in development.
- [ ] Keep CDS credentials as placeholders only.
- [ ] Keep PostgreSQL and integration-test database examples.
- [ ] Keep `CDS_START_DATE` / `CDS_END_DATE` as standalone/manual defaults.
- [ ] Keep `CAMPAIGN_KEY`.
- [ ] Keep `CAMPAIGN_START_YEAR` / `CAMPAIGN_END_YEAR`.
- [ ] Keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED`.
- [ ] Keep allowed cadence values.
- [ ] Set/comment examples to match the currently intended development pilot
      policy without silently changing runtime defaults.
- [ ] Keep `SUBMISSION_INTERVAL_SECONDS` documented independently from
      scheduler cadence.
- [ ] Document monthly scheduled slot spacing if it remains a code constant
      rather than an environment value; do not add an environment variable
      merely because the constant moved to root config.
- [ ] Keep JWT secret outside `.env` and point to existing setup documentation.
- [ ] Do not add passwords, keys, host-specific absolute paths, or user-specific
      values.

---

## 9. Phase F — Tests

### Reset tests

- [ ] reset remains inspect-only by default;
- [ ] prod hard-denied;
- [ ] deferred task blocks reset;
- [ ] running/queued task blocks reset;
- [ ] running/queued DagRun blocks reset;
- [ ] active Airflow service blocks reset;
- [ ] active worker policy matches the final conservative decision;
- [ ] no confirmation means no mutation.

### Destroy tests

Create `tests/unit/operations/test_destroy_all.py`.

Cover at minimum:

- [ ] inspect mode never mutates;
- [ ] prod hard-denied before target mutation;
- [ ] confirmation requires exact `DESTROY`;
- [ ] Airflow services/workers are enumerated;
- [ ] SIGTERM is sent before SIGKILL;
- [ ] SIGKILL applies only to remaining recognized Airflow processes;
- [ ] inability to stop Airflow aborts before database recreation;
- [ ] PostgreSQL system database is rejected;
- [ ] external PostgreSQL session blocks destructive database recreation;
- [ ] execution order is Airflow stop -> DB -> data -> logs -> runtime;
- [ ] preserved files are untouched;
- [ ] final verification failure returns non-zero.

### Configuration tests

- [ ] root config resolves campaign values from an explicit mapping;
- [ ] required campaign values reject blank/missing input;
- [ ] campaign start/end year validation preserved;
- [ ] campaign date derivation preserved;
- [ ] schedule enabled/cadence behavior unchanged;
- [ ] `SUBMISSION_INTERVAL_SECONDS` behavior unchanged;
- [ ] `MONTHLY_SLOT_SPACING_MINUTES == 15`;
- [ ] schema/table constants unchanged;
- [ ] timezone behavior unchanged;
- [ ] no import of `orchestration.config` remains.

### Regression gates

After each phase run the narrowest affected tests.

Final minimum gate:

```bash
source activate-airflow.sh -q

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

Then isolated PostgreSQL integration tests:

```bash
python -m pytest \
  tests/integration/pipeline/test_postgresql_idempotency.py \
  tests/integration/orchestration/test_pipeline_progress_reconciliation.py \
  tests/integration/orchestration/test_submission_pacing.py \
  tests/integration/orchestration/test_orchestration_database_bootstrap.py \
  -q
```

Do not run a live CDS pilot until these gates pass.

---

## 10. Phase G — Documentation

After behavior is verified:

- [ ] Update `orchestration/README.md` with the reset/destroy distinction.
- [ ] Add explicit examples:

```bash
# safe maintenance/recovery
python scripts/reset_all.py
python scripts/reset_all.py --execute

# intentional clean slate
python scripts/destroy_all.py
python scripts/destroy_all.py --execute
```

- [ ] Explain preserved files/configuration for destroy.
- [ ] Update any maintenance documentation that still describes reset as a
      complete clean-install operation.
- [ ] Update configuration documentation to state root `config.py` is the
      single project config module.
- [ ] Remove current-state documentation references that instruct runtime code
      to import `orchestration.config`.
- [ ] Keep historical audit entries unchanged when clearly historical.

---

## 11. Recommended Implementation Order

Use this order so each change has one clear purpose.

### D1 — Freeze and audit

- [x] Approve reset vs destroy contracts.
- [x] Approve root config as the single configuration module.
- [x] Audit current root/orchestration config content.
- [x] Audit known `orchestration.config` callers.
- [x] Create this implementation checklist before code changes.
- [ ] Freeze live scheduled pilot work until maintenance/config refactor is
      verified.

### D2 — Restore reset semantics

- [x] Revert reset-only allowances for deferred/running-DagRun clean-slate use.
- [x] Decide final residual-worker behavior for conservative reset: any active
      LocalExecutor worker blocks `reset_all.py`.
- [x] Update/reset maintenance tests.
- [x] User verification: 54 focused operations tests passed in 2.15s and the
      full unit suite passed 291 tests in 6.84s on 2026-10-07.

### D3 — Shared Airflow process primitives

- [x] Add `scripts/airflow_processes.py` as the shared process-table mechanism.
- [x] Centralize recognized Airflow service/worker markers and process discovery.
- [x] Centralize explicit-PID SIGTERM -> recheck -> SIGKILL mechanics.
- [x] Move guarded runtime reset and PostgreSQL reset preflight onto the shared
      discovery primitives without changing their policies.
- [x] Keep reset policy local: `reset_all.py` still kills no processes, while
      `reset_airflow_runtime.py` may stop only residual LocalExecutor workers
      after main services are absent.
- [x] Add dedicated unit coverage for process discovery and termination.
- [x] User verification on 2026-10-07: 61 focused operations tests passed in
      2.05s and the full unit suite passed 298 tests in 6.65s.

### D4 — Implement destroy

- [x] Add `scripts/destroy_all.py`.
- [x] Add exact `DESTROY` confirmation.
- [x] Add full Airflow stop/escalation through the shared D3 process primitives.
- [x] Add database/session safety; external PostgreSQL sessions block recreation
      and are reported with PID/user/application/client/state context.
- [x] Reuse tested data/log/runtime/database primitives where appropriate.
- [x] Add final clean-state verification for Airflow processes/runtime files,
      data/log directories, and canonical application database tables.
- [x] Add `test_destroy_all.py`.
- [x] User verification on 2026-10-07: 81 focused operations tests passed in
      3.21s and the full unit suite passed 318 tests in 8.20s.

### D5 — Consolidate config

- [x] Move orchestration-owned configuration into root `config.py`.
- [x] Preserve existing root config definitions when concepts overlap:
      `APPLICATION_SCHEMA` and `APPLICATION_TIMEZONE_NAME` remain canonical.
- [x] Keep campaign environment settings lazy in root config so standalone
      pipeline imports do not require `CAMPAIGN_*` unless orchestration uses them.
- [x] Update all audited runtime callers to import from root `config.py`.
- [x] Update unit/integration test imports.
- [x] Audit the current `dev` tree rather than relying on stale default-branch
      code-search results.
- [x] Delete `orchestration/config.py` with no compatibility wrapper.
- [x] Root config remains Airflow-neutral/lightweight: standard library,
      `python-dotenv`, constants, and pure configuration resolvers only.
- [x] Add coverage that root config can import without campaign environment keys.
- [ ] Run focused config/orchestration tests and full unit regression.

### D6 — Update `env-example`

- [ ] Reconcile environment documentation with the single root config.
- [ ] Add destroy policy explanation under `APP_ENV`.
- [ ] Preserve placeholder-only secrets.
- [ ] Confirm every required environment key used by root config is represented.
- [ ] Do not add unnecessary environment knobs.

### D7 — Full verification and docs

- [ ] Run full unit regression.
- [ ] Run isolated PostgreSQL integration gate.
- [ ] Update operational documentation.
- [ ] Review diff for accidental `pipeline/` or scientific changes.
- [ ] Commit/freeze one DAG version before the next clean scheduled pilot.

### D8 — Real destroy validation

Only after automated tests pass:

- [ ] Start a disposable local Airflow runtime with scheduler/triggerer/workers.
- [ ] Create representative Airflow metadata, including deferred/running state.
- [ ] Run `python scripts/destroy_all.py` inspect-only and verify targets.
- [ ] Run `python scripts/destroy_all.py --execute`.
- [ ] Confirm all recognized Airflow processes are gone.
- [ ] Confirm Airflow metadata/log/data runtime state is empty.
- [ ] Confirm PostgreSQL is freshly recreated with canonical schema/tables.
- [ ] Confirm `.env`, `env-example`, `airflow.cfg`, JWT key, source code,
      and Git state are preserved.
- [ ] Bootstrap Airflow from the destroyed state without manual cleanup.
- [ ] Only then resume the clean scheduled-quarter pilot.

---

## 12. Definition of Done

This work is complete only when:

- [ ] `reset_all.py` has a conservative recovery contract.
- [ ] `destroy_all.py` provides an explicit dev-only clean-slate contract.
- [ ] Destroy can stop the complete recognized Airflow runtime safely.
- [ ] Destroy wipes local runtime/data/database state while preserving project
      configuration.
- [ ] `orchestration/config.py` no longer exists.
- [ ] Root `config.py` is the single project configuration source.
- [ ] `env-example` accurately documents all required environment keys and
      destroy availability.
- [ ] No active code imports `orchestration.config`.
- [ ] Unit and PostgreSQL integration gates pass.
- [ ] Documentation clearly distinguishes reset from destroy.
- [ ] A real local destroy/bootstrap cycle succeeds without manual `pkill`,
      metadata deletion, or database cleanup.
