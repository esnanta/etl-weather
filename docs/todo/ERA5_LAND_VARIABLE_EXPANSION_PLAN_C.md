# ERA5-Land Variable Expansion — Plan C Work Plan and Checklist

**Document role:** Track the implementation of **Plan C**: rebuild the configured historical ERA5-Land campaign using one expanded variable set, preserving the existing one-request → one-RAW → one-Parquet → one-database-partition architecture. Product/scientific requirements remain owned by [app_description.md](../app_description.md); current operator-facing behavior remains owned by the root [README.md](../../README.md) and domain READMEs.

**Branch:** `dev`  
**Created:** 2026-11-04  
**Status:** Planned; implementation not started  
**Primary owners:** `pipeline/` owns request, transformation, artifact, validation, and loading behavior; `database/` owns schema convergence; `orchestration/` coordinates the unchanged ETL stages without owning scientific logic.

## 1. Approved direction

Plan C intentionally replaces the current five-variable active ERA5-Land request with one expanded nine-variable request and re-downloads the historical campaign.

Current provider variables:

```text
tp
t2m
d2m
swvl1
ssrd
```

Target provider variables:

```text
tp
t2m
d2m
swvl1
swvl2
swvl3
ssrd
u10
v10
```

The expanded request remains one logical monthly request partition. The expected architecture remains:

```text
ERA5-Land
    |
    v
one monthly RAW NetCDF
    |
    v
UTC+7 daily transformation
    |
    v
one monthly transformed Parquet
    |
    v
one canonical PostgreSQL daily-climate partition
```

The existing request/artifact identity model remains authoritative:

```text
one request
→ one request_key
→ one RAW artifact
→ one transformed artifact
→ one database partition
```

Because the variable set is part of request identity, the expanded nine-variable request intentionally produces new request keys and new deterministic artifact names. Existing five-variable RAW/Parquet artifacts are not valid checkpoints for the new request.

## 2. Scientific transformation contract

### Existing variables

Preserve the accepted daily rules:

- `tp` → UTC+7 daily accumulation, output `mm/day`.
- `ssrd` → UTC+7 daily accumulation, output `J/m2/day`.
- `t2m` → mean of 24 valid hourly samples for the UTC+7 local day, output `degC`.
- `d2m` → mean of 24 valid hourly samples for the UTC+7 local day, output `degC`.
- `swvl1` → mean of 24 valid hourly samples for the UTC+7 local day, output `m3/m3`.

### Additional soil-water variables

- `swvl2` → mean of 24 valid hourly samples for the UTC+7 local day, output `m3/m3`.
- `swvl3` → mean of 24 valid hourly samples for the UTC+7 local day, output `m3/m3`.

Layer interpretation for documentation:

```text
swvl1 :   0–7 cm
swvl2 :   7–28 cm
swvl3 :  28–100 cm
```

Do not add `swvl4` in this workstream.

### Wind variables and derived daily statistics

Provider inputs remain `u10` and `v10`. The transformed daily dataset must preserve both daily mean components and add derived wind statistics.

For each hourly sample:

```text
hourly_wind_speed = sqrt(u10^2 + v10^2)
```

Daily outputs:

```text
u10_daily_mean
    = mean(hourly u10)

v10_daily_mean
    = mean(hourly v10)

wind_speed_daily_mean
    = mean(sqrt(hourly u10^2 + hourly v10^2))
```

Do **not** define daily mean wind speed as:

```text
sqrt(mean(u10)^2 + mean(v10)^2)
```

Daily wind direction is the direction of the daily resultant vector derived from the daily mean `u10` and `v10`, using meteorological **from-direction** convention:

```text
0°   = from North
90°  = from East
180° = from South
270° = from West
```

The implementation must use a tested `atan2`-based conversion consistent with ECMWF `u`/`v` sign conventions.

If the daily resultant vector magnitude is effectively zero, wind direction is undefined and must remain missing/null rather than being coerced to `0°`.

The transformed Parquet is expected to contain:

```text
valid_time
valid_time_local
latitude
longitude
tp
t2m
d2m
swvl1
swvl2
swvl3
ssrd
u10
v10
wind_speed
wind_direction
```

Derived wind fields belong to the transformed daily product, not the RAW provider artifact.

## 3. Database target

Continue using the existing canonical tables:

```text
climate_data.t_daily_climate
climate_data.t_daily_climate_detail
```

Do not create separate wind or soil-moisture tables.

Target additional detail columns:

```text
swvl2_daily_mean_m3_m3
swvl3_daily_mean_m3_m3
u10_daily_mean_m_s
v10_daily_mean_m_s
wind_speed_daily_mean_m_s
wind_direction_daily_deg
```

Schema initialization must converge safely:

```text
table missing
    → CREATE complete current schema

table exists
    → inspect required columns
        ├─ missing column → ALTER TABLE ADD COLUMN
        ├─ compatible existing column → keep unchanged
        └─ incompatible existing type/contract → fail explicitly
```

New observation columns must be nullable so existing rows can remain valid during migration. Do not silently fabricate values for historical rows.

The loader must continue using transactional UPSERT behavior and preserve existing observation identity:

```text
daily_climate_id + valid_time + latitude + longitude
```

## 4. Artifact and provenance policy

The expanded request must continue using the existing deterministic artifact layout.

A monthly RAW filename is expected to reflect all nine provider variables through the existing stable variable signature. The exact order is determined by the current deterministic signature implementation.

Conceptually:

```text
YYYY_MMDD_MMDD_d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10.nc
```

and:

```text
YYYY_MMDD_MMDD_d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10.parquet
```

Preserve the current one-RAW → one-Parquet provenance model:

```text
RAW path/checksum
    ↓
Parquet provenance
    ↓
transformed path/checksum
    ↓
PostgreSQL partition metadata
```

Existing five-variable artifacts are immutable historical inputs. This workstream does not delete or overwrite them automatically. Any later cleanup of obsolete generated artifacts must be a separate explicitly authorized destructive task.

## 5. Orchestration policy

Keep one active ERA5-Land DAG and the existing ETL stage flow.

No second DAG is required for Plan C.

Preserve:

```text
build_monthly_requests
    ↓
attempt_raw_checkpoint
    ↓
resolve_raw_checkpoint
    ↓
ensure_transformed_checkpoint
    ↓
load
```

Preserve current campaign-year configuration:

```env
CAMPAIGN_START_YEAR=2016
CAMPAIGN_END_YEAR=2025
```

Future expansion to a longer historical period should require changing the campaign years, not redefining the variable set.

Preserve CDS pool, submission pacing, retries, mapped monthly execution, progress tracking, and recovery behavior unless a measured incompatibility is found during implementation/testing.

## 6. Scope boundaries

### In scope

- Expand the active ERA5-Land provider variable set from five to nine variables.
- Add ERA5-Land provider-catalog support for `u10`, `v10`, `swvl2`, and `swvl3` if not already present.
- Extend RAW/source validation for the new provider variables.
- Extend daily scientific transformation rules.
- Add derived `wind_speed` and `wind_direction` to transformed output.
- Extend transformed-data validation and output-unit contracts.
- Extend PostgreSQL detail schema and loader mappings.
- Add safe additive schema convergence for an already-existing daily detail table.
- Update controlled daily-climate reads/views/query contracts that explicitly enumerate climate columns.
- Update relevant documentation and tests.
- Re-run the historical campaign using the expanded variable set after implementation and validation.

### Out of scope

- Adding `swvl4`.
- Adding another data provider or replacing ERA5-Land.
- ARCO.
- Changing Aceh bounding-box coordinates or grid resolution.
- Changing UTC+7 local-day semantics.
- Changing monthly request partitioning.
- Changing campaign scheduling policy without measured need.
- Creating separate wind/soil tables.
- Introducing a second DAG for the new variables.
- Adding ET0, VPD, root-zone indices, drought indices, or other analytical features to canonical observations.
- Automatically deleting old five-variable RAW/Parquet artifacts.
- Reworking monthly climate aggregation unless compatibility requires a narrowly scoped follow-up.

## 7. Checklist

### Phase 0 — Baseline and source audit

- [ ] Re-read `AGENTS.md`, `tests/AGENTS.md`, `docs/app_description.md`, `pipeline/README.md`, `orchestration/README.md`, and `docs/python_se_principle.md` immediately before implementation.
- [ ] Record the pre-implementation `dev` commit SHA and confirm the intended working-tree state.
- [ ] Audit all consumers of `CDS_VARIABLES`.
- [ ] Audit provider catalog mappings for `u10`, `v10`, `swvl2`, and `swvl3`.
- [ ] Audit RAW validation assumptions for exact variable sets, dimensions, timestamps, and local-day context.
- [ ] Audit transformed schema validation for hard-coded five-variable expectations.
- [ ] Audit database loaders, controlled-read repositories, geospatial views, operational SQL, monthly experimental code, and tests for explicit daily climate column lists.
- [ ] Audit artifact/path tests that assert the current five-variable signature.
- [ ] Audit orchestration progress/campaign tests that embed request keys or variable signatures.
- [ ] Record baseline quiet test results before production changes.

Suggested baseline:

```bash
source activate-airflow.sh

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q

python -m pytest tests/integration -q
```

### Phase 1 — Update owning scientific/application requirements

- [ ] Update `docs/app_description.md` first because it owns the WHAT/scientific contract.
- [ ] Change the active provider-variable contract from five to nine variables.
- [ ] Document `swvl2` and `swvl3` layer depths and daily-mean treatment.
- [ ] Document `u10`/`v10` source semantics and output units.
- [ ] Document the exact daily mean wind-speed formula.
- [ ] Document meteorological wind-direction convention and undefined near-zero resultant behavior.
- [ ] Update canonical transformed schema.
- [ ] Update PostgreSQL canonical column requirements.
- [ ] Update controlled-read requirements.
- [ ] Update acceptance criteria from five active provider variables to nine provider variables plus derived wind statistics.
- [ ] Preserve the rule that analytical products such as ET0/VPD remain outside canonical observations.

### Phase 2 — Expand request/provider contract

- [ ] Update `config.py::CDS_VARIABLES` to the approved nine provider variables in one stable explicit configuration.
- [ ] Preserve a single active variable configuration; do not introduce base/extension profiles for Plan C.
- [ ] Ensure provider catalog supports all nine requested names.
- [ ] Confirm `build_request()` accepts the new variables and still rejects unsupported/duplicate variables.
- [ ] Confirm `build_variable_signature()` remains deterministic.
- [ ] Confirm `build_request_key()` intentionally changes when the variable set changes.
- [ ] Update request/unit tests for the new canonical active variable set and deterministic artifact names.

### Phase 3 — Extend ERA5-Land daily transformation

- [ ] Add `swvl2` and `swvl3` to ERA5-Land daily-mean variables.
- [ ] Add `u10` and `v10` to the appropriate instantaneous/daily transformation contract.
- [ ] Calculate hourly scalar wind speed before daily aggregation.
- [ ] Calculate daily mean scalar wind speed from hourly scalar speeds.
- [ ] Calculate daily mean `u10` and `v10` from 24 valid local-day hourly samples.
- [ ] Calculate daily resultant wind direction from the daily mean components.
- [ ] Keep wind direction missing when the resultant vector is effectively zero.
- [ ] Preserve the requirement for complete hourly local-day coverage; do not silently substitute partial means.
- [ ] Add output units for all new and derived fields.
- [ ] Preserve existing `tp`, `ssrd`, `t2m`, `d2m`, and `swvl1` scientific behavior unchanged.
- [ ] Update transformed dataframe column ordering explicitly.
- [ ] Add focused tests for `swvl2`/`swvl3` daily means.
- [ ] Add focused wind-speed tests that prove mean(speed) differs from speed(mean vector) for reversing winds.
- [ ] Add cardinal-direction tests for North/East/South/West meteorological from-direction.
- [ ] Add a near-zero resultant-vector test returning missing direction.
- [ ] Add missing-hour tests proving incomplete local days do not produce false complete wind/soil daily values.
- [ ] Preserve existing accumulated-variable cross-UTC-day tests.

### Phase 4 — Extend validation and artifact handling

- [ ] Update RAW validation to require/accept all requested nine provider variables without weakening dimension/time/grid checks.
- [ ] Update transformed validation for nine provider variables plus derived wind fields.
- [ ] Verify deterministic RAW and Parquet paths include the expanded variable signature.
- [ ] Verify existing five-variable artifacts are not reused for nine-variable requests.
- [ ] Verify a nine-variable checkpoint is resumed only when its artifact/provenance validation passes.
- [ ] Preserve one-RAW → one-Parquet provenance metadata and checksum validation.
- [ ] Add/update checkpoint tests for conflicting old/new variable signatures.

### Phase 5 — Database schema convergence

- [ ] Add the six new daily-detail columns to the canonical complete table definition.
- [ ] Implement/readjust schema inspection for required columns in an existing `t_daily_climate_detail`.
- [ ] Add missing compatible columns through additive `ALTER TABLE`.
- [ ] Keep existing columns/data unchanged.
- [ ] Fail explicitly if an existing required column has an incompatible SQL type or incompatible structural contract.
- [ ] Keep all six new observation columns nullable.
- [ ] Do not drop/recreate the daily table as part of normal bootstrap.
- [ ] Preserve existing primary key, foreign key, unique index, and cascade behavior.
- [ ] Add unit tests for:
  - [ ] fresh database/table creation with the complete new schema;
  - [ ] existing old schema upgraded by adding only missing columns;
  - [ ] repeated initialization making no further incompatible changes;
  - [ ] incompatible existing column type causing explicit failure;
  - [ ] existing historical rows remaining valid/null for newly added fields.

### Phase 6 — Extend PostgreSQL loader and read paths

- [ ] Extend `ERA5_LAND_DATABASE_COLUMNS` with `swvl2`, `swvl3`, `u10`, `v10`, `wind_speed`, and `wind_direction`.
- [ ] Preserve generic transactional observation UPSERT behavior.
- [ ] Verify the unchanged-partition fast path still uses request key + transformed path/checksum + observation row count.
- [ ] Verify rollback still prevents partially committed metadata/details.
- [ ] Update controlled daily-climate read column lists.
- [ ] Update regional view/read projections if they intentionally expose all canonical climate fields.
- [ ] Update read-only audit SQL only where its explicit column contract requires the new fields.
- [ ] Preserve duplicate local-day/grid rejection and deterministic ordering in controlled reads.
- [ ] Add integration coverage for loading and reloading a nine-variable+derived-wind partition.

### Phase 7 — Orchestration compatibility

- [ ] Confirm the existing single DAG builds monthly requests with the expanded variable set.
- [ ] Confirm campaign progress identity naturally uses the new request keys and does not reuse five-variable completed partition evidence.
- [ ] Preserve one active DAG ID unless an unrelated requirement explicitly changes it.
- [ ] Preserve the `cds_api` pool and provider submission gate.
- [ ] Preserve monthly slot pacing, retries, deferral, progress recording, reconciliation, and manual-repair behavior.
- [ ] Update orchestration tests that assert variable signatures/request specs/request keys.
- [ ] Verify the scheduler does not treat historical five-variable campaign rows as successful evidence for the expanded nine-variable campaign.

### Phase 8 — Documentation reconciliation

- [ ] Update root `README.md` active variables and daily-output table.
- [ ] Update `pipeline/README.md` current request/transformation/schema behavior.
- [ ] Update `orchestration/README.md` only if operator-facing campaign behavior/commands materially change.
- [ ] Update database/schema documentation where current columns are enumerated.
- [ ] Update query/geospatial documentation only where exposed climate-column lists change.
- [ ] Keep implementation detail in domain docs and avoid duplicating the full scientific contract outside `docs/app_description.md`.

### Phase 9 — Regression verification

Run focused suites first, then wider suites.

- [ ] Run request/provider/storage unit tests.
- [ ] Run ERA5-Land transformation/validation unit tests.
- [ ] Run database/loader/read-path unit tests.
- [ ] Run orchestration unit tests.
- [ ] Run complete unit suite:

```bash
source activate-airflow.sh

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

- [ ] Run focused PostgreSQL integration tests for schema migration, load idempotency, rollback, controlled reads, and progress reconciliation.
- [ ] Run complete integration suite:

```bash
source activate-airflow.sh

python -m pytest tests/integration -q
```

- [ ] Compare results with the pre-implementation baseline; do not rely on historical pass counts as proof of this change.
- [ ] Inspect final diff for accidental changes to bbox, local-day accumulation formulas, request partitioning, idempotency, pacing, or unrelated domains.
- [ ] Confirm no ARCO/provider branch was introduced.

### Phase 10 — Historical rebuild readiness

Do not start destructive cleanup merely to force the new campaign.

- [ ] Confirm database schema upgrade succeeds against the intended development database.
- [ ] Confirm at least one isolated/manual month can download all nine provider variables.
- [ ] Validate RAW dimensions, variable presence, UTC time coverage, grid counts, and checksums for the pilot month.
- [ ] Validate transformed Parquet schema, units, local-day coverage, wind calculations, and provenance for the pilot month.
- [ ] Load the pilot month and verify row counts, null policy, database columns, idempotent rerun, and read-path output.
- [ ] Verify repeated extract/transform/load resumes or skips exactly as intended.
- [ ] Only after pilot acceptance, authorize the full configured historical campaign rebuild.
- [ ] Monitor campaign progress, provider pacing, disk capacity, RAW/Parquet growth, and failures during the historical rebuild.
- [ ] Keep existing five-variable artifacts untouched until the expanded campaign is fully validated and any cleanup is separately approved.

## 8. Acceptance criteria

Plan C is complete when all of the following are true:

- [ ] The active ERA5-Land request contains exactly the approved nine provider variables.
- [ ] One monthly request produces one immutable RAW NetCDF containing those variables.
- [ ] One monthly transformed Parquet contains the canonical UTC+7 daily observations plus `wind_speed` and `wind_direction`.
- [ ] `swvl2` and `swvl3` use complete local-day daily means.
- [ ] Daily mean wind speed is computed from hourly scalar speeds, not from the magnitude of daily mean components.
- [ ] Wind direction follows meteorological from-direction convention and remains missing for an effectively zero resultant.
- [ ] Existing precipitation/radiation accumulation and temperature/dewpoint/`swvl1` behavior is unchanged.
- [ ] Existing database installations are upgraded additively without dropping canonical climate tables.
- [ ] Fresh database installations create the complete current schema.
- [ ] PostgreSQL load remains transactional and idempotent.
- [ ] Controlled daily reads expose the expanded canonical observation without ambiguous duplicate partitions.
- [ ] Existing five-variable artifacts are not falsely resumed for expanded requests.
- [ ] The existing single DAG coordinates the expanded pipeline without duplicating scientific logic.
- [ ] Relevant unit and integration tests pass.
- [ ] One pilot month passes extract → transform → load → rerun verification before the full historical rebuild is authorized.
- [ ] Owning documentation reflects the implemented behavior.

## 9. Risks to watch

- **CDS request size:** nine variables increase monthly payload size. Keep monthly partitioning unless measured failures justify a separate decision.
- **Historical progress identity:** old five-variable request keys must not be interpreted as completion evidence for the new campaign.
- **Wind semantics:** confusing scalar mean wind speed with resultant-vector magnitude would create scientifically incorrect daily data.
- **Wind direction convention:** mathematical vector direction and meteorological from-direction differ; cardinal tests are mandatory.
- **Schema drift:** `CREATE TABLE IF NOT EXISTS` alone does not upgrade existing tables; explicit additive convergence is required.
- **Read/view drift:** any hard-coded five-variable projection can silently hide newly loaded columns.
- **Disk usage:** old and new RAW/Parquet generations may coexist during the rebuild.
- **Destructive cleanup:** old artifacts must not be removed as an implicit part of this implementation.

## 10. Execution log

| Date | Step | Evidence | Status |
| --- | --- | --- | --- |
| 2026-11-04 | Plan C selected | Rebuild historical campaign using one expanded nine-variable ERA5-Land request | Complete |
| 2026-11-04 | Work plan created | Checklist recorded in `docs/todo`; no production code changed | Complete |
| — | Baseline verification | Awaiting local/unit/integration baseline | Pending |
| — | Requirement update | Awaiting implementation | Pending |
| — | Pipeline/database implementation | Awaiting implementation | Pending |
| — | Pilot month | Awaiting implementation and verification | Pending |
| — | Historical rebuild | Requires explicit pilot acceptance | Pending |
| — | Final regression/closure | Awaiting tests and review | Pending |
