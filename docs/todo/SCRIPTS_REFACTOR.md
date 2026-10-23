# Scripts Directory Refactor — Design and Checklist

Branch: `dev`  
Created: 2026-10-23  
Status: design proposal — **awaiting agreement; no files moved**

## Purpose

Reorganize `scripts/` by responsibility without changing operational semantics, scientific ETL, Airflow scheduling, idempotency, or destructive-operation safety. Maintain the four agreed stable daily ETL commands at the `scripts` root.

## Current inventory (audited on dev)

- Daily ETL: `run_pipeline.py`, `run_extract.py`, `run_transform.py`, `run_load.py`
- Experimental monthly: `run_monthly_climate.py`
- Environment: `setup_environment.sh`
- Maintenance: `reset_all.py`, `destroy_all.py`, `reset_data.py`, `reset_airflow_logs.py`, `reset_airflow_runtime.py`, `recreate_postgres_database.py`
- Monitoring: `inspect_campaign_capacity.py`, `report_campaign_progress.py`
- Inspection: `read_daily_climate.py`
- Shared utilities: `airflow_processes.py`, `helpers/reset_helpers.py`

## Proposed layout

```text
scripts/
├── __init__.py
├── run_pipeline.py
├── run_extract.py
├── run_transform.py
├── run_load.py
├── run_monthly_climate.py
├── setup_environment.sh
├── maintenance/
│   ├── __init__.py
│   ├── reset_all.py
│   ├── destroy_all.py
│   ├── reset_data.py
│   ├── reset_airflow_logs.py
│   ├── reset_airflow_runtime.py
│   └── recreate_postgres_database.py
├── monitoring/
│   ├── __init__.py
│   ├── inspect_campaign_capacity.py
│   └── report_campaign_progress.py
├── inspection/
│   ├── __init__.py
│   └── read_daily_climate.py
└── helpers/
    ├── __init__.py
    ├── airflow_processes.py
    └── reset_helpers.py
```

Retain `run_monthly_climate.py` rather than shorten to `run_monthly.py`: the explicit name distinguishes experimental *monthly climate aggregation* from monthly partitioning in the daily ETL runtime.

## Verified dependency hotspots

- `destroy_all.py` and `reset_all.py` currently import `scripts.recreate_postgres_database`, `scripts.reset_airflow_logs`, `scripts.reset_airflow_runtime`, `scripts.reset_data`.
- `destroy_all.py` and `recreate_postgres_database.py` import `scripts.airflow_processes`.
- `reset_data.py` imports `scripts.helpers.reset_helpers`.
- Existing unit tests in `tests/unit/operations/` target the old module names.
- Root `README.md` documents `scripts.reset_all`, `scripts.destroy_all`; `orchestration/README.md` documents both monitoring commands.
- Inspect additional import/call sites before finalizing moves; do not assume this inventory is exhaustive.

## Implementation checklist (not started)

- [ ] Agree on target directories and whether old CLI paths need transition aliases.
- [ ] Record clean `dev` baseline and verify no unrelated changes will be overwritten.
- [ ] Review all imports, module-level paths, `__file__`-derived repository roots, script invocations, README examples, tests, and operational references.
- [ ] Move modules only; avoid opportunistic behavioral rewrites.
- [ ] Update internal imports and module-relative file resolution after moves.
- [ ] Keep the daily ETL command paths and current stage idempotency unchanged.
- [ ] Preserve APP_ENV guards, inspect/dry-run mode, explicit confirmations, preflight checks, stop-on-failure behavior, and authorized database targeting for destructive CLIs.
- [ ] Decide and document whether to preserve temporary entrypoint aliases for operational command backward compatibility.
- [ ] Update `tests/unit/operations/` imports/mocks/CLI invocations, including subprocess invocation tests and failure cases.
- [ ] Add/import-smoke-test the relocated monitoring and inspection CLIs without contacting CDS or the application database.
- [ ] Update root `README.md`, `orchestration/README.md`, and other verified call sites with new module paths. Avoid duplicating full commands in AGENTS.
- [ ] Run offline regression: `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false python -m pytest tests/unit -q`.
- [ ] Run relevant isolated PostgreSQL integration suite as needed; never reset live `weather` for testing.
- [ ] Record test evidence and commit changes only on `dev`.

## Acceptance

- Unchanged root daily ETL CLIs work; no new CDS submissions are triggered by refactor/tests.
- All relocated module CLI invocations resolve, import, and preserve safety controls.
- Existing reset/destroy operations refuse inappropriate environments and do not mutate state during preview.
- Airflow task/DAG imports and CLI operational guidance remain valid.
- Unit/integration regression passes; any skipped live check remains clearly pending.
