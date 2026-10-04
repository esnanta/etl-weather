# Guarded PostgreSQL Development Database Recreation Procedure

This procedure prepares the explicit P6 database recreation. The current
development database contents are authorized for one-time replacement, but the
application database itself must be treated as non-disposable by default.

Airflow metadata remains SQLite and is outside this procedure. Normal DAG,
pipeline, bootstrap, reporting, and application code must never DROP or recreate
the configured PostgreSQL database.

## 1. Safety model

Database recreation is a maintenance-only operation.

The maintenance entrypoint implemented for P6 must:

1. resolve and validate `APP_ENV` using the shared root configuration;
2. abort with an actionable warning when `APP_ENV` is missing or blank;
3. reject unsupported values with an instruction to set `APP_ENV=dev` or
   `APP_ENV=prod`; no mode is assumed;
4. when `APP_ENV=prod`, terminate before exposing any reset option,
   interactive deletion prompt, or DROP/CREATE path;
5. when `APP_ENV=dev`, resolve host, port, database, user, and schema from
   `DatabaseConfig` and display them without printing the password;
6. reject well-known PostgreSQL system databases such as `postgres`,
   `template0`, and `template1`;
7. stop when Airflow/process preflight has not been completed;
8. immediately before deletion, ask an interactive yes/no question that names
   the exact configured database target;
9. accept `yes` or `y` case-insensitively to proceed, and `no` or `n`
   to abort cleanly; invalid answers must never count as consent;
10. never use a database-name pattern such as `dev` or `test` as the sole
    authorization for deletion.

Future production or retained-development databases are therefore protected by
default. Merely running the same application code or using the same
`climate_data` schema does not make a database eligible for reset.

## 2. Resolve and record the exact configured target

Activate the project environment:

```bash
source activate-airflow.sh
```

Inspect the active environment and runtime target without exposing the password:

```bash
python - <<'PY'
from config import resolve_app_environment
from pipeline.database import DatabaseConfig

config = DatabaseConfig.from_env()
print(f"app_env={resolve_app_environment()}")
print(f"host={config.host}")
print(f"port={config.port}")
print(f"database={config.database}")
print(f"user={config.user}")
print(f"schema={config.schema}")
PY
```

Record this output in the P6 work log. The database name must come from the
active `POSTGRES_DATABASE` configuration. Never substitute or infer another
database name.

This inspection does **not** authorize deletion. It only identifies the target
that a later guarded maintenance invocation would have to confirm exactly.

## 3. Pre-reset checks required in P6

Before any destructive maintenance:

1. Keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false`.
2. Inspect active, queued, running, and deferred Airflow task instances.
3. Ensure no old worker or DagRun can resume against the replacement database.
4. Record the last real CDS submission time because recreating the pacing table
   removes its durable submission timestamp.
5. Confirm RAW NetCDF, transformed Parquet, chunk cache, Airflow SQLite metadata,
   logs, credentials, and JWT key are outside the database reset scope.
6. Confirm explicitly in the P6 work log that the **current development
   contents**, not the database as a permanent resource, are authorized for
   one-time replacement.

If any of these checks cannot be established, stop. Do not attempt to compensate
with `dropdb --force` or by terminating unknown sessions automatically.

## 4. Guarded maintenance entrypoint

P6 should implement and test a dedicated maintenance script or command before
database recreation is performed. Its interface should make destructive intent
unambiguous. Conceptually, it should behave like:

```text
resolve APP_ENV
    |
    +-- missing / blank / invalid -> warn and abort startup
    |
    +-- prod -> reset feature unavailable; stop
    |
    +-- dev
          |
          v
    display exact configured target
          |
          v
    completed Airflow/process preflight
          |
          v
    prompt: delete and recreate this database? [y/n]
          |
          +-- no / n  -> abort cleanly
          |
          +-- yes / y -> recreate the displayed target
```

The exact CLI name should be finalized during P6 implementation. Application
startup must already have logged the selected environment. When `APP_ENV=prod`,
the reset feature must not be available at all. Do not expose
DROP/CREATE as a normal pipeline or orchestration helper.

The maintenance implementation may invoke PostgreSQL administrative operations
internally after all guards pass, but ordinary project documentation should
direct operators through the guarded entrypoint rather than encourage copying
raw `dropdb` / `createdb` commands.

## 5. Bootstrap the five application tables

After a successful guarded recreation, use the normal bootstrap coordinator:

```bash
python - <<'PY'
from orchestration.database import initialize_application_storage
from pipeline.database import DatabaseConfig
from sqlalchemy import create_engine

config = DatabaseConfig.from_env()
engine = create_engine(config.url, future=True)
try:
    initialize_application_storage(engine, config)
finally:
    engine.dispose()
PY
```

The expected application tables are:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
climate_data.pipeline_progress
climate_data.pipeline_progress_detail
climate_data.cds_submission_pacing
```

The bootstrap coordinator creates application schema/tables only. It is not
authorized to create, drop, or recreate the PostgreSQL database itself.

## 6. Post-reset verification

After recreation:

1. verify the configured database connection points to the exact target recorded
   before reset;
2. verify the `climate_data` schema and all five tables exist;
3. verify progress and pacing contain no unexpected historical state;
4. verify constraints and table ownership/permissions needed by runtime;
5. verify Airflow SQLite metadata is unchanged;
6. verify retained RAW/Parquet/chunk/log/credential/JWT files were not deleted.

Start new DagRuns after the reset. Do not clear and resume old task instances
whose XCom may contain progress IDs from the discarded development state.

## 7. Long-term rule

The one-time P6 development reset does not establish a general database-reset
workflow for normal operations. After P6, destructive maintenance must remain
explicitly gated and exceptional. Production and retained development databases
must be treated as persistent assets that require an environment-specific
backup, change, and recovery policy before any destructive operation.
