# Plan C — Isolated Pilot and Historical Rebuild Readiness

**Role:** Operator runbook for Phase 10. Scientific requirements belong to
[`docs/app_description.md`](../app_description.md); pipeline and Airflow behavior
belong to their domain READMEs. This runbook does not authorize an automatic
historical rebuild, scheduling, or cleanup.

## Current gate

Phases 0–9 have passed source review and the user-reported local regression
baseline of **454 unit tests + 23 integration tests**. No real nine-variable CDS
pilot is yet evidenced. Therefore **historical rebuild is NOT approved**.

The latest reported local configuration uses
`CAMPAIGN_KEY=era5_land_2016_2025`,
`CAMPAIGN_START_YEAR=2016`, and `CAMPAIGN_END_YEAR=2016`. A campaign key
does not override its year range. Do not change the year range or enable the
Airflow campaign during the pilot.

## Pilot identity and scope

Use one **manual, non-campaign month: 2024-05-01 through 2024-05-31**.
This is outside the currently configured 2016 campaign and avoids marking
2016 quarterly progress as complete. It is nevertheless a **real write** to
the configured `POSTGRES_*` application database when the load stage is run.
Use only the intended development database and inspect its identity first.
Existing five-variable partitions and artifacts must remain untouched.

Expected provider codes (9):
`tp,t2m,d2m,swvl1,swvl2,swvl3,ssrd,u10,v10`.

Derived daily columns (2): `wind_speed,wind_direction`.
Stable variable signature:
`d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10`.
The configured Aceh grid has 43 latitudes × 35 longitudes = 1,505 points
at 0.1°. May has 31 local days; at full coverage this is 46,655
daily grid observations. The 24-hour scientific completeness and spatial
validation rules remain authoritative rather than accepting a row count alone.

## Gate A — Read-only preflight

From the repository root:

```bash
git status --short --branch
git log -1 --format='%h %s'
source activate-airflow.sh -q

# Read-only resource snapshot; inspect disk, memory, PostgreSQL and Airflow.
python -m scripts.monitoring.inspect_campaign_capacity

# Climate DDL preview only; does not change the database.
python -m database.climate
```

Resolve the exact pilot request without printing credentials:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python - <<'PY'
from pipeline.requests.factory import build_cds_request
from pipeline.requests.weather import build_request_key, build_variable_signature
from pipeline.storage import raw_artifact_for_request, transformed_artifact_for_request
from database.config import DatabaseConfig

request = build_cds_request(require_api_key=False)
config = DatabaseConfig.from_env()
assert config.database != "postgres"
assert request.start_date == "2024-05-01"
assert request.end_date == "2024-05-31"
assert len(request.variables) == 9
assert build_variable_signature(request.variables) == (
    "d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10"
)
print("database:", config.database)
print("request_key:", build_request_key(request))
print("variables:", ",".join(request.variables))
for artifact in (raw_artifact_for_request(request), transformed_artifact_for_request(request)):
    print(f"{artifact.layer.value}_path:", artifact.path)
    print(f"{artifact.layer.value}_exists:", artifact.path.exists())
PY
```

Inspect the generated database name, generated artifact paths, disk headroom,
CDS quota/account readiness, and whether any Airflow tasks are actively
running. `source activate-airflow.sh -q` can report the .env default
`ERA5_CAMPAIGN_SCHEDULE_ENABLED=true`; the explicit per-command override
above and below keeps the **new process** unscheduled. It does not pause or
stop an already-running Airflow scheduler.

Before a write, verify the target database is the intended development
database and confirm there is no conflicting active ETL run. Do **not** run
`database.setup --execute`, reset/destroy scripts, or a broad campaign
trigger as pilot preparation.

## Gate B — Additive schema preparation (explicit write)

Only after reviewing Gate A, initialize **climate schema only** in the
existing configured application database:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m database.climate --execute
```

This performs idempotent additive climate/operational DDL, including six
nullable columns. It does not download data or recreate the database. A
failure due to an incompatible existing column is **NO-GO**, not permission
to drop or recreate the table.

## Gate C — One manual pilot, stage by stage (real CDS + artifact + DB writes)

Execute only after Gate A/B acceptance, with the scheduling override on
**every command**:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python -m scripts.run_extract

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python -m scripts.run_transform

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python -m scripts.run_load
```

The first command submits CDS retrieval if no valid nine-variable RAW
checkpoint exists; it can incur substantial CDS wait time and storage use.
The second requires existing validated RAW. The third writes canonical
PostgreSQL observations. The commands run the existing pipeline directly;
there is **no new DAG** and no scheduler activation.

## Gate D — Read-only artifact and scientific verification

Run after all three stages have completed:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python - <<'PY'
from pipeline.requests.factory import build_cds_request
from pipeline.storage import (
    raw_artifact_for_request, transformed_artifact_for_request,
    load_raw_netcdf, load_transformed_parquet, calculate_file_sha256,
    read_parquet_provenance,
)
from pipeline.validation import validate_raw_dataset, validate_transformed_dataset

request = build_cds_request(require_api_key=False)
raw = raw_artifact_for_request(request)
parquet = transformed_artifact_for_request(request)
raw_dataset = load_raw_netcdf(raw, request)
validate_raw_dataset(raw_dataset)
transformed = load_transformed_parquet(parquet, request, raw)
validate_transformed_dataset(transformed)

print("RAW_SHA256:", calculate_file_sha256(raw.path))
print("PARQUET_SHA256:", calculate_file_sha256(parquet.path))
print("RAW_dims:", dict(raw_dataset.raw_data.sizes))
print("RAW_variables:", sorted(raw_dataset.raw_data.data_vars))
print("Parquet_columns:", list(transformed.dataset.columns))
print("Parquet_rows:", len(transformed.dataset))
print("Local_days:", transformed.dataset.valid_time_local.nunique())
print("Parquet_provenance:", read_parquet_provenance(parquet))
assert set(request.variables).issubset(raw_dataset.raw_data.data_vars)
assert {"wind_speed", "wind_direction"}.issubset(transformed.dataset.columns)
assert transformed.dataset.valid_time_local.nunique() == 31
PY
```

Review source time boundaries (including context day), 43×35 grid coverage,
all nine source variables, 15 transformed columns, units, 24-hour local
coverage, wind speed/direction tests, SHA-256, and RAW→Parquet provenance.
Do not interpret a passing SHA alone as scientific validation.

## Gate E — PostgreSQL and idempotency proof

Inspect the pilot using the exact current request key and expected artifact
checksum, without querying or modifying old five-variable partitions:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python - <<'PY'
from sqlalchemy import create_engine, text
from database.config import DatabaseConfig
from pipeline.requests.factory import build_cds_request
from pipeline.requests.weather import build_request_key
from pipeline.storage import (
    transformed_artifact_for_request, calculate_file_sha256,
    artifact_path_for_metadata,
)

config = DatabaseConfig.from_env()
request = build_cds_request(require_api_key=False)
key = build_request_key(request)
artifact = transformed_artifact_for_request(request)
with create_engine(config.url, future=True).connect() as conn:
    row = conn.execute(text(f"""
        SELECT p.id, p.variable_signature, p.transformed_sha256,
               p.transformed_artifact_path, COUNT(d.id) AS row_count,
               COUNT(d.wind_direction_daily_deg) AS valid_direction_count
        FROM {config.schema}.{config.daily_climate_table} AS p
        JOIN {config.schema}.{config.daily_climate_detail_table} AS d
          ON d.daily_climate_id = p.id
        WHERE p.request_key = :key
        GROUP BY p.id
    """), {"key": key}).mappings().one()
assert row["row_count"] == 31 * 43 * 35
assert row["transformed_sha256"] == calculate_file_sha256(artifact.path)
assert row["transformed_artifact_path"] == artifact_path_for_metadata(artifact.path)
print(dict(row))
PY
```

Then repeat **only** the load stage:

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
PIPELINE_START_DATE=2024-05-01 PIPELINE_END_DATE=2024-05-31 \
python -m scripts.run_load
```

The repeated load should report **0 rows processed / skipped** with the same
metadata ID and observation count. Repeat extract and transform only after
confirming their immutable checkpoints are valid; successful reruns must
resume existing artifacts, not re-submit CDS or overwrite files. Use the
read-only checks again to compare checksum and row counts. If the first
load is not fully validated, stop rather than treating a second run as proof.

## Gate F — Rebuild decision

**GO** only if all of the following evidence exists:

- Gate A proves acceptable disk/memory/database capacity and correct target.
- Gate B completes additive DDL without dropping any existing table.
- Gate C performs one successful nine-variable real CDS month.
- Gate D passes RAW/Parquet scientific validation, provenance and coverage.
- Gate E verifies 46,655 observations (or explains and validates an approved
  alternative coverage contract), correct PostgreSQL values/NULL handling,
  controlled read, and idempotent resume/load.
- Historical five-variable artifacts/records remain unchanged.
- Scheduled/manual campaign controls, pacing and monitoring are reviewed.
- Before full 2016–2025 processing, deliberately update
  `CAMPAIGN_END_YEAR=2025`, verify all 40 target quarters, and obtain a
  **separate explicit authorization** to enable scheduling.

**NO-GO** if any evidence is missing, any table type is incompatible,
provenance changes unexpectedly, a checkpoint is mismatched, or resources
are insufficient. Do not reset or delete old data to bypass a failure.

The full historical campaign, broad scheduling, and cleanup remain outside
this runbook's execution steps.
