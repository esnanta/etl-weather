# README Refactor Plan

Branch: `dev`  
Created: 2026-10-19  
Status: planning only — README content has not been refactored yet.

## Goal

Refactor project README files so documentation has a clear hierarchy:

- `/README.md` is the project entry point.
- Root README contains Prerequisites, installation, setup (example env-example), and CLI commands for all domains.
- Root README summarizes each domain and links to its own README for deeper documentation.
- Root README keeps ## Active Dataset, Variables and Daily Outputs, Local-Day Time Semantics, Project Structure and ## Architecture Overview sections.
- Root README gives a concise overview of every major domain.
- Root README keeps the important CLI commands users need for setup, execution, verification, testing, and operations.
- Domain README files explain only their own domain, including important domain-specific decisions and operational details.
- Root README links to each domain README for deeper documentation.
- Avoid long narrative sections and repeated explanations across README files.

## README inventory

The `dev` branch currently contains five README files:

- [x] `/README.md`
- [x] `/orchestration/README.md`
- [x] `/geospatial/README.md`
- [x] `/tests/README.md`
- [x] `/query/README.md` — discovered during repository-wide README inventory and added to this refactor scope.

No `/pipeline/README.md` currently exists. The pipeline must still be summarized in the root README. Creating a new pipeline README is outside the current refactor unless a later audit shows that pipeline-specific material cannot remain concise in the root documentation.

## Documentation ownership

| File | Responsibility |
| --- | --- |
| `/README.md` | Whole-project overview, architecture, prerequisites, quick setup, domain summaries, all important CLI entry points, and links to domain documentation. |
| `/orchestration/README.md` | Airflow-only architecture, scheduling/manual modes, pacing, retries/deferrals, progress, recovery, Airflow runtime setup, and orchestration decisions. |
| `/geospatial/README.md` | PostGIS/geospatial-only setup, administrative boundaries, import, grid-to-region mapping, spatial views, verification, and geospatial decisions. |
| `/tests/README.md` | Test structure, test levels/domains, isolation rules, commands, and definition of done for testing. |
| `/query/README.md` | Read-only operational SQL catalog, execution order, interpretation, result limits, and SQL-specific documentation rules. |

## Root README target structure

Keep sections short and prefer tables, bullets, numbered steps, and ASCII diagrams.

1. Project purpose
2. Active dataset and outputs
3. Architecture overview
4. Project/domain map with links
   - Pipeline
   - Database
   - Orchestration → `orchestration/README.md`
   - Geospatial → `geospatial/README.md`
   - Query/audit SQL → `query/README.md`
   - Tests → `tests/README.md`
5. Prerequisites
6. Environment setup
7. Database/PostGIS quick setup
8. CLI command reference
9. Data/artifact layout
10. Scientific processing summary
    - variables
    - UTC+7 local-day rule
    - idempotency/checkpoints
11. Testing quick start
12. Documentation links / further reading

Detailed Airflow scheduling, geospatial internals, test policy, and SQL audit interpretation must move to their domain README instead of being duplicated in root.

## Root CLI requirement

The root README must remain the fastest place to find executable commands. During refactor, inventory actual entry points before rewriting this section.

CLI groups to retain in root:

- [ ] environment activation
- [ ] dependency installation
- [ ] database preview/bootstrap/setup
- [ ] standalone daily ETL
- [ ] monthly climate CLI, clearly marked experimental/unverified where applicable
- [ ] Airflow start/setup commands required for normal use
- [ ] Airflow DAG trigger/status commands
- [ ] geospatial import and grid mapping
- [ ] SQL/audit entry guidance
- [ ] unit tests
- [ ] integration tests
- [ ] campaign/reporting/inspection scripts
- [ ] destructive/reset commands with explicit safety labels

Root CLI descriptions should be brief. Domain-specific explanation belongs in the linked README.

### CLI compactness rule

CLI documentation should optimize for the fewest practical commands.

- Prefer one command that performs the complete common workflow over several granular commands.
- When a combined command exists, document it as the primary/default path.
- Example: prefer `python -m database.setup --execute` when the intended task is to prepare the configured development and test databases end-to-end, instead of presenting separate climate/geospatial/bootstrap commands as equal first-class steps.
- Prefer `python -m database.bootstrap --execute` when the target database already exists and the goal is to initialize the complete application schema in one step.
- Keep granular commands such as `database.climate` and `database.geospatial` as secondary references only for partial initialization, diagnostics, recovery, or domain-specific maintenance.
- The same rule applies to reset/maintenance commands: prefer `reset_all` or `destroy_all` for the complete intended workflow; list individual reset commands only when they provide a meaningful partial-recovery use case.
- Avoid documenting multiple equivalent invocation styles for the same task. Choose one canonical form.
- A compact CLI section should group related commands by user goal, not by Python module.
- Safety warnings for destructive commands must remain explicit even when the CLI section is compressed.

Root README should therefore answer “what single command should I normally run?” first, and only then point to domain documentation for lower-level alternatives.

## Refactor steps

### Phase 1 — Audit

- [x] Inventory every `README.md` on branch `dev`.
- [x] Add the discovered `query/README.md` to scope.
- [x] Read each README completely and classify each section as:
  - project-wide;
  - orchestration;
  - geospatial;
  - testing;
  - query/audit;
  - duplicated/outdated.
- [x] Inventory current executable CLI entry points from `scripts/`, `database/`, `geospatial/`, and Airflow commands referenced by the repository.
- [x] Check root links and README-to-README links for valid relative paths.
- [x] Check terminology against current architecture and configuration names.

### Phase 2 — Define content boundaries

- [ ] Build a section ownership matrix before editing.
- [ ] Identify duplicate explanations across root and domain READMEs.
- [ ] Keep one canonical detailed explanation in the relevant domain README.
- [ ] Replace duplicate root detail with a short summary plus link.
- [ ] Preserve important warnings and operational decisions; shortening must not remove safety constraints.
- [ ] Keep experimental/unverified features explicitly labelled.

### Phase 3 — Refactor `/README.md`

- [ ] Rewrite the opening as a concise project summary.
- [ ] Add a compact architecture/domain overview.
- [ ] Summarize `pipeline/`, `database/`, `orchestration/`, `geospatial/`, `query/`, and `tests/`.
- [ ] Add direct relative links to domain READMEs.
- [ ] Consolidate important CLI commands into a compact, goal-oriented command reference that prefers combined one-step workflows over granular module-by-module commands.
- [ ] Keep prerequisite/setup instructions concise.
- [ ] Keep only the scientific rules needed to understand the pipeline at project level.
- [ ] Remove detailed Airflow behavior now owned by `orchestration/README.md`.
- [ ] Remove detailed PostGIS/geospatial procedures now owned by `geospatial/README.md`.
- [ ] Remove detailed testing policy now owned by `tests/README.md`.
- [ ] Remove detailed SQL audit interpretation now owned by `query/README.md`.

### Phase 4 — Refactor domain READMEs

#### `/orchestration/README.md`
- [ ] Keep only Airflow/orchestration content.
- [ ] Preserve important decisions: manual vs scheduled behavior, monthly mapping, pacing, deferral/retry, progress/reconciliation, concurrency, recovery, and runtime state.
- [ ] Remove project-wide setup or pipeline scientific explanation unless required to explain an orchestration boundary.
- [ ] Link back to root for common setup/CLI overview.

#### `/geospatial/README.md`
- [ ] Keep only geospatial/PostGIS content.
- [ ] Preserve immutable boundary-source rules, PostGIS schema contract, import, mapping, views, validation, and regional-query interpretation.
- [ ] Remove generic project/environment material duplicated in root.
- [ ] Link back to root for common setup/CLI overview.

#### `/tests/README.md`
- [ ] Keep only testing organization, isolation, execution, and testing policy.
- [ ] Update the test tree to match the current repository.
- [ ] Avoid duplicating setup commands already documented in root except the minimal activation/test commands required for usability.
- [ ] Link to orchestration/geospatial READMEs only where domain-specific test prerequisites require it.

#### `/query/README.md`
- [ ] Keep only operational SQL audit documentation.
- [ ] Verify the query inventory against current files before preserving counts such as “seven checks”.
- [ ] Keep read-only safety, ordering, result-limit rules, and PASS/FAIL/INFO interpretation.
- [ ] Remove historical acceptance narrative if it no longer helps operate the query catalog.
- [ ] Link back to root for database setup.

### Phase 5 — Consistency review

- [ ] Search all README files for duplicated paragraphs after refactor.
- [ ] Verify command names against actual modules/scripts.
- [ ] Verify DAG ID and task terminology against current code.
- [ ] Verify table/view names and schema references against `database/config.py`.
- [ ] Verify current bounding box, variables, UTC+7 semantics, and artifact paths.
- [ ] Verify all relative documentation links.
- [ ] Ensure no README contradicts `AGENTS.md` or domain-specific `AGENTS.md`.
- [ ] Ensure root README remains readable without requiring users to open every domain README.

### Phase 6 — Validation

- [ ] Review the final Git diff for documentation-only changes.
- [ ] Confirm no Python, SQL, DAG, configuration, or pipeline behavior changed.
- [ ] Run a repository search for stale README references and obsolete names.
- [ ] Run lightweight command discovery/collection checks where useful; do not run destructive commands.
- [ ] Present the README changes and any unresolved documentation conflicts for operator review.

## Style rules for this refactor

- Prefer short paragraphs.
- Prefer tables for inventories/comparisons.
- Prefer bullets or numbering for procedures.
- Use ASCII diagrams only when they reduce explanation length.
- Keep commands copy-pasteable.
- Do not duplicate the same detailed explanation in root and domain README.
- Root README may repeat a command from a domain README because CLI discoverability is an explicit requirement.
- Do not preserve historical status text merely because it exists; keep it only when it remains operationally relevant.
- Do not change application behavior as part of this documentation refactor.


## Phase 1 audit results

Audit basis: branch `dev` on 2026-10-19. This section records findings only; no README content has been refactored yet.

### README inventory confirmed

Exactly five `README.md` files exist on the current `dev` tree:

| README | Current role | Phase 1 finding |
| --- | --- | --- |
| `/README.md` | project entry point | Contains the required project-wide material, but also contains detailed PostGIS/geospatial and operational material that should become concise summaries plus domain links. |
| `/orchestration/README.md` | Airflow/orchestration | Mostly correctly scoped to Airflow. Common activation/test commands are acceptable usability duplication. Historical verification/status sections should be reviewed for ongoing operational value during refactor. |
| `/geospatial/README.md` | PostGIS/geospatial | Correct domain content, but long verification/history sections can be shortened. It currently has no Markdown link back to root. |
| `/tests/README.md` | testing | Correct purpose, but its displayed test tree is stale relative to the repository. |
| `/query/README.md` | read-only SQL audit | Seven SQL audit files still exist. The schema inventory wording needs clarification because it calls five tables the complete “application tables” while the current bootstrap also provisions monthly and geospatial objects. |

No `/pipeline/README.md` exists.

### Section classification

#### `/README.md`

Keep as project-wide content:
- project purpose;
- prerequisites;
- Active Dataset;
- Environment Setup;
- How It Works;
- Variables and Daily Outputs;
- Local-Day Time Semantics;
- architecture overview;
- Project Structure;
- concise data layout;
- concise persistence/idempotency summary;
- consolidated CLI reference;
- concise test quick start.

Condense and delegate:
- detailed PostgreSQL/PostGIS lifecycle and spatial schema behavior → `geospatial/README.md` plus a short root setup summary;
- detailed development/test database behavior → root may retain commands and safety summary, while database/geospatial mechanics should not dominate the project overview;
- detailed Airflow scheduling/recovery → `orchestration/README.md`;
- detailed testing policy → `tests/README.md`;
- operational SQL interpretation → `query/README.md`.

Concrete stale item:
- root command comment `python -m database.bootstrap --execute  # climate + geospatial (8 tables)` is incorrect. The actual bootstrap reports **7 climate/operational + 3 spatial = 10 tables**, plus **2 regional views**.

Navigation gap:
- root currently links to `orchestration/README.md` and `tests/README.md`, but does not provide direct README links to `geospatial/README.md` or `query/README.md`.

#### `/orchestration/README.md`

Domain ownership is appropriate for:
- Airflow setup;
- DAG execution;
- scheduled/manual contract;
- timing/pacing;
- progress/reconciliation;
- mapped monthly start slots;
- retry/deferral;
- task flow;
- recovery;
- observability;
- maintenance/reset behavior.

Verified current terminology:
- DAG ID in code: `cds-era5-land`;
- DAG source file: `orchestration/dags/cds_era5_land_etl.py`;
- current README commands use `cds-era5-land`, matching code.

Refactor candidate:
- dated verification/status narrative such as `P3 Scheduling Contract Verification (2026-10-18)` should be retained only if it remains useful as an operational contract; otherwise move/remove historical evidence rather than treating it as permanent README content.

#### `/geospatial/README.md`

Domain ownership is appropriate for:
- immutable administrative source;
- PostGIS setup contract;
- district import;
- grid-to-region mapping;
- daily/monthly regional views;
- spatial verification;
- identifier behavior.

Refactor candidates:
- operator screenshots, dated pass counts, and historical acceptance narrative are not core operating instructions;
- common environment/setup material can point back to root;
- add a link back to `/README.md`.

#### `/tests/README.md`

The current documentation tree is stale.

Actual unit domains on `dev`:
- `tests/unit/database/`
- `tests/unit/geospatial/`
- `tests/unit/operations/`
- `tests/unit/orchestration/`
- `tests/unit/pipeline/`

Actual integration domains on `dev`:
- `tests/integration/geospatial/`
- `tests/integration/orchestration/`
- `tests/integration/pipeline/`

Current README tree omits:
- unit `database`;
- unit `geospatial`;
- integration `geospatial`.

Its command section already references the geospatial integration suite, so the prose/tree and command list are internally inconsistent.

#### `/query/README.md`

Confirmed SQL files:
- `00_connection__01_database_context.sql`
- `01_schema__01_inventory.sql`
- `02_daily_climate__01_partition_summary.sql`
- `03_daily_climate_detail__01_data_quality.sql`
- `04_pipeline_progress__01_campaign_status.sql`
- `05_pipeline_progress_detail__01_load_verification.sql`
- `06_p2__01_standalone_pilot_verification.sql`

Therefore “seven read-only checks” is still structurally correct.

Terminology issue to resolve:
- query 01 explicitly checks five daily/operational tables:
  `t_daily_climate`, `t_daily_climate_detail`, `t_pipeline_progress`, `t_pipeline_progress_detail`, and `t_submission_pacing`;
- current bootstrap additionally provisions two monthly tables and three geospatial tables, plus two regional views;
- during refactor, query documentation should call the five objects the **core daily/operational audit scope** rather than implying they are every application table, unless query 01 is intentionally expanded in a separate non-documentation change.

### Actual CLI inventory

The following are current executable/user-facing entry points found on `dev`.

#### Environment and Airflow bootstrap

| Command | Purpose | README status |
| --- | --- | --- |
| `source activate-airflow.sh -q` | load `.env`, activate `myvenv`, configure Airflow environment | documented |
| `./generate-airflow-jwt-secret.sh` | generate local Airflow JWT key file | orchestration README |
| `scripts/setup_environment.sh` | create/activate venv and install `requirements.txt` | exists but is not part of the current root setup path |
| `airflow db migrate` | initialize/migrate Airflow metadata DB | orchestration README |
| `airflow pools set cds_api 1 "Copernicus CDS API requests"` | configure CDS pool | orchestration README |
| `airflow standalone` | run local Airflow stack | orchestration README |

Note: root installation currently documents manual venv creation and the Airflow constrained installation sequence. `scripts/setup_environment.sh` only installs `requirements.txt` and does not reproduce that constrained Airflow install sequence, so it must not silently replace the documented installation procedure without a separate code/setup decision.

#### Database CLI

| Module | Default behavior | Mutating flag |
| --- | --- | --- |
| `python -m database.setup` | preview dev/test database targets | `--execute` |
| `python -m database.bootstrap` | preview combined climate + spatial initialization | `--execute` |
| `python -m database.climate` | preview 7 climate/operational tables | `--execute` |
| `python -m database.geospatial` | preview 3 spatial tables + regional view creation | `--execute` |

Current combined object contract from code:
- 7 climate/operational tables;
- 3 geospatial tables;
- 2 regional views;
- PostGIS required for the spatial path.

#### Pipeline and data-access CLI

| Module | Purpose | Options |
| --- | --- | --- |
| `python -m scripts.run_pipeline` | active daily ETL runner using configured request period | configuration-driven; no argparse flags |
| `python -m scripts.run_monthly_climate --month YYYY-MM` | separate experimental monthly aggregation preview | `--month` required; optional dataset/bbox overrides; `--execute` writes |
| `python -m scripts.read_daily_climate` | read canonical daily climate data for modelling/access inspection | configuration-driven |

The monthly CLI remains explicitly experimental in its source docstring and must remain labelled that way in documentation.

#### Geospatial CLI

| Module | Default behavior | Mutating flag |
| --- | --- | --- |
| `python -m geospatial.importers.districts` | validate immutable district GeoJSON | `--execute` imports/upserts |
| `python -m geospatial.mapping` | preview coordinate-to-region classification | `--execute` persists mapping |

#### Orchestration operation CLI

| Command | Purpose |
| --- | --- |
| `airflow dags trigger cds-era5-land --conf ...` | manual ETL DagRun |
| `airflow dags list-runs cds-era5-land` | inspect DagRuns |
| `python scripts/report_campaign_progress.py` | read-only campaign progress report |
| `python scripts/inspect_campaign_capacity.py` | read-only host/campaign capacity snapshot |

For consistency during the README refactor, decide whether Python project scripts should be presented uniformly as `python -m scripts.<name>` where module execution is supported. Existing orchestration documentation currently uses direct file execution for reporting/maintenance scripts.

#### Maintenance/destructive CLI

All of these are guarded development operations and are preview/inspect-only by default where an `--execute` flag exists:

- `python -m scripts.reset_data [--execute]`
- `python -m scripts.reset_airflow_logs [--execute]`
- `python -m scripts.reset_airflow_runtime [--execute]`
- `python -m scripts.recreate_postgres_database [--execute]`
- `python -m scripts.reset_all [--execute]`
- `python -m scripts.destroy_all [--execute]`

`destroy_all` is the stronger clean-slate operation: source code states it is restricted to `APP_ENV=dev`, stops recognized Airflow processes, recreates the configured PostgreSQL database, clears generated data/log/runtime state, and requires exact `DESTROY` confirmation for execution.

The root README currently exposes only `destroy_all`; orchestration README exposes `reset_all` and `destroy_all`. Phase 3 should decide which individual maintenance commands belong in the root CLI index while keeping safety labels explicit.

#### Test CLI

Actual current suites support:

```bash
python -m pytest tests/unit -q
python -m pytest tests/unit/database -q
python -m pytest tests/unit/geospatial -q
python -m pytest tests/unit/operations -q
python -m pytest tests/unit/orchestration -q
python -m pytest tests/unit/pipeline -q

python -m pytest tests/integration -q
python -m pytest tests/integration/geospatial -q
python -m pytest tests/integration/orchestration -q
python -m pytest tests/integration/pipeline -q
```

Integration tests remain tied to isolated `TEST_POSTGRES_*` configuration.

### Link audit

Existing relative Markdown links resolve to files on `dev`:

- root → `geospatial/sql/01_postgis_preflight.sql`: valid;
- root → `orchestration/README.md`: valid;
- root → `tests/README.md`: valid;
- orchestration → root: valid;
- orchestration → tests README: valid;
- tests → root: valid;
- tests → orchestration README: valid;
- tests → `tests/AGENTS.md`: valid.

Missing navigation rather than broken links:
- root has no direct link to `geospatial/README.md`;
- root has no direct link to `query/README.md`;
- geospatial README has no Markdown link back to root;
- query README has no Markdown link back to root.

### Terminology/configuration audit

Verified against current code:

| Item | Current value |
| --- | --- |
| shared application config | root `config.py` |
| database identifier config | `database/config.py` |
| application schema | `climate_data` |
| table prefix | `t_` |
| view prefix | `vw_` |
| active CDS variables | `tp, t2m, swvl1, ssrd, d2m` |
| active DAG ID | `cds-era5-land` |
| DAG source filename | `cds_era5_land_etl.py` |
| Airflow metadata | separate from application PostgreSQL; local Airflow setup remains under `orchestration/` |

The root README statement that there is no separate `orchestration/config.py` remains consistent with the current tree.

### Phase 1 conclusion

Phase 1 audit is complete. No application code, SQL, configuration, or README content was changed as part of the audit; only this planning/checklist file was updated.

Primary issues to carry into Phase 2:
1. root README is too detailed in PostGIS/geospatial operations;
2. root CLI index is incomplete despite the requirement that root be the command discovery point;
3. root bootstrap comment says 8 tables instead of the actual 10 tables (+2 views);
4. root lacks direct links to geospatial and query READMEs;
5. tests README structure is stale;
6. query README/query 01 uses “five application tables” wording that no longer describes the complete bootstrapped schema;
7. domain READMEs contain dated verification/history material that should be separated from durable operating documentation;
8. Python script invocation style should be made consistent where practical;
9. CLI documentation should prefer combined one-step workflows (for example `database.setup`, `database.bootstrap`, `reset_all`, or `destroy_all`) and demote granular commands to secondary/troubleshooting references.
