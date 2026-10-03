# PostgreSQL Development Database Recreation Procedure

This procedure prepares the explicit P6 database recreation. Do not execute the
DROP/CREATE section during P3. Airflow metadata remains SQLite and is not part of
this procedure.

## 1. Resolve and record the exact configured target

Activate the project environment first:

```bash
source activate-airflow.sh
```

Print the runtime target without exposing the password:

```bash
python - <<'PY'
from pipeline.database import DatabaseConfig

config = DatabaseConfig.from_env()
print(f"host={config.host}")
print(f"port={config.port}")
print(f"database={config.database}")
print(f"user={config.user}")
print(f"schema={config.schema}")
PY
```

Record this output in the P6 work log before destructive work. The database name
must come from the active `POSTGRES_DATABASE` configuration; never substitute a
guessed name.

## 2. Pre-reset checks required in P6

Before recreation:

1. Keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false`.
2. Inspect active, queued, running, and deferred Airflow task instances.
3. Ensure no old worker or DagRun can resume against the replacement database.
4. Record the last real CDS submission time because recreating the pacing table
   removes its durable submission timestamp.
5. Confirm RAW NetCDF, transformed Parquet, chunk cache, Airflow SQLite metadata,
   logs, credentials, and JWT key are outside the database reset scope.

## 3. Recreate only the verified project database

After the exact target from step 1 is reviewed, export it from the same runtime
configuration rather than typing a new value:

```bash
TARGET_DB="$(python - <<'PY'
from pipeline.database import DatabaseConfig
print(DatabaseConfig.from_env().database)
PY
)"
```

Display it immediately before destructive commands:

```bash
printf 'PostgreSQL database selected for recreation: %s\n' "$TARGET_DB"
```

Only after the P6 verification gate approves that exact value, recreate it with
the PostgreSQL administrative tools available on the host. The intended sequence
is:

```bash
dropdb --if-exists   --host "$POSTGRES_HOST"   --port "$POSTGRES_PORT"   --username "$POSTGRES_USER"   "$TARGET_DB"

createdb   --host "$POSTGRES_HOST"   --port "$POSTGRES_PORT"   --username "$POSTGRES_USER"   "$TARGET_DB"
```

Do not add `--force` automatically. Active sessions are a reason to stop and
investigate rather than silently terminate them.

## 4. Bootstrap the five application tables

After recreation, use the normal runtime credentials and bootstrap coordinator:

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

## 5. Post-reset verification

Verify that the schema and five tables exist, that progress and pacing contain no
unexpected historical state, and that Airflow SQLite metadata is unchanged.
Start new DagRuns after the reset; do not clear and resume old task instances whose
XCom may contain progress IDs from the discarded database.
