# README Refactor Checklist

This document tracks the documentation-only refactor of:

- `/README.md`
- `/orchestration/README.md`

The goal is to make the root README explain the ETL project globally and in business-oriented terms, while the orchestration README explains only the Airflow-specific operational layer.

## Scope and Constraints

- [ ] Documentation-only change.
- [ ] Do not modify `pipeline/`.
- [ ] Do not modify DAG implementation.
- [ ] Do not modify configuration implementation.
- [ ] Do not modify `.env`.
- [ ] Do not modify database schema.
- [ ] Do not modify scripts.
- [ ] Do not change ETL, scheduling, persistence, retry, or scientific behavior.
- [ ] Work only on branch `dev`.
- [ ] If documentation review exposes a code/configuration issue, record it separately instead of changing implementation during this refactor.

## Documentation Responsibility

### Root README

`/README.md` should answer:

- What the ETL program is for.
- What data it processes.
- What the business process is.
- What the main outputs are.
- What is required to install and run the project.
- How local-day observations are interpreted.
- How canonical persistence and modelling access work at a high level.
- Where to find Airflow-specific documentation.

### Orchestration README

`/orchestration/README.md` should answer:

- What Airflow controls after the project is installed.
- How the Airflow runtime is initialized and started.
- How scheduled and manual modes differ.
- How campaign cadence and CDS submission pacing differ.
- How progress, retry, recovery, and resume work.
- How operators inspect and safely operate the campaign.

### Documentation Boundaries

- [ ] Keep detailed scientific rationale in design-decision documentation.
- [ ] Keep detailed testing contracts in `tests/README.md`.
- [ ] Keep root installation instructions in `/README.md`.
- [ ] Avoid duplicating installation instructions in `/orchestration/README.md`.
- [ ] Keep historical pilot evidence only when it directly helps explain current operational behavior.
- [ ] Prefer current-state documentation over development chronology.

---

# Phase 1 — Audit Existing Content

- [x] Review both READMEs against the current `dev` implementation.
- [x] Classify existing content as:
  - global ETL/project information;
  - Airflow-specific information;
  - operational example;
  - historical/development evidence.
- [x] Identify duplicated explanations between the two READMEs.
- [x] Identify implementation details that are too low-level for the root README.
- [x] Identify historical rollout notes that can be removed, shortened, or converted into examples.
- [x] Confirm current names and terminology before editing:
  - DAG: `cds_era5_land_etl`;
  - task terminology;
  - campaign terminology;
  - current bounding box;
  - no ARCO terminology;
  - no deprecated `backfill` terminology where it no longer reflects the current design.

## Phase 1 Audit Findings

### Root README — Content Classification

**Global ETL/project information to keep in the root README**

- Project purpose and active ERA5-Land dataset.
- Input/output overview.
- Variables and daily outputs.
- Local-day UTC+7 semantics.
- High-level project structure.
- Environment and dependency installation.
- Standalone pipeline execution.
- Data layout.
- High-level persistence and modelling access.
- Concise test entry points.
- Link to Airflow orchestration documentation.

**Airflow-specific content currently misplaced in the root README**

- `airflow-jwt-key.txt` in the root project tree.
- `generate-airflow-jwt-secret.sh` operational explanation.
- `AIRFLOW__API_AUTH__JWT_SECRET_CMD` behavior.
- Airflow campaign-validation details inside the environment narrative.
- Airflow implementation details that belong in `orchestration/README.md`.

**Root content that is too implementation-oriented**

- The current `How It Works` flow is expressed in checkpoint/runtime terminology rather than business-process language.
- `Project Structure` exposes more orchestration internals than a root project overview needs.
- `Pipeline Architecture` is dominated by module and filename names such as `runtime.py`, `interfaces/etl.py`, provider modules, loaders, repositories, and individual runtime functions.
- `Persistence and Modelling Access` exposes low-level identities, checksum comparison, UPSERT fallback behavior, and database-row relationships that are unnecessary for a first-level project explanation.

**Root content requiring clarification rather than removal**

- `Local-Day Time Semantics` should remain, but `D` and `A(t)` are not currently defined.
- The local-day formula needs a concrete date example.
- The environment example currently omits the full PostgreSQL, test-PostgreSQL, cadence, and submission-pacing configuration even though those settings exist in `env-example`.

### Orchestration README — Content Classification

**Airflow-specific operational information to keep**

- Airflow's responsibility as orchestration rather than scientific processing.
- JWT-secret generation and lifecycle.
- `source activate-airflow.sh`.
- Airflow metadata initialization and CDS pool setup.
- Scheduled/manual mode behavior.
- Campaign cadence and submission pacing.
- Durable campaign progress.
- Retry, recovery, resume, and task-state inspection.
- Safe deployment rules while a DagRun is active.
- Airflow-specific test commands and local runtime cleanup.

**Generic project information duplicated from the root README**

- Python 3.12 prerequisite.
- PostgreSQL prerequisite.
- CDS API credential prerequisite.
- Shared `data/` filesystem prerequisite.
- Virtual-environment creation.
- Dependency installation from `requirements.txt`.
- General `.env` setup.

These should be owned by the root README and referenced rather than repeated.

**Historical/development evidence that currently dominates the document**

- Offline Scheduled Selection Pilot.
- Controlled Scheduled Pilot on the Airflow Host.
- Campaign Host Capacity and Post-Change CDS Pilot.
- Local Pilot Evidence.
- Verified Three-Month Rollout.
- Historical peak-memory measurements.
- Historical disk/capacity observations.
- Development-era deployment notes tied to specific pilot runs.

These sections should be shortened into current-behavior examples, or moved out of the primary orchestration README when the historical detail still has value.

**Operational sections that are currently too long**

- `Running the Orchestration`.
- `Scheduled and Manual Mode Contract`.
- `Scheduler Cadence vs CDS Submission Interval`.
- `Generic Progress Persistence Foundation`.
- Campaign selection/reconciliation explanation.
- Backup and restart recovery.
- Trigger/status examples.
- Retry-barrier explanation.
- Deployment-during-campaign explanation.

The underlying safety rules should remain, but the primary explanation should become example-driven.

### Confirmed Current Terminology and Configuration

- Active DAG ID: `cds_era5_land_etl`.
- Active task IDs:
  - `build_monthly_requests`;
  - `attempt_raw_checkpoint`;
  - `resolve_raw_checkpoint`;
  - `ensure_transformed_checkpoint`;
  - `load`.
- Scheduled campaign uses durable quarter selection.
- Active campaign environment variables:
  - `CAMPAIGN_KEY`;
  - `CAMPAIGN_START_YEAR`;
  - `CAMPAIGN_END_YEAR`;
  - `ERA5_CAMPAIGN_SCHEDULE_ENABLED`;
  - `ERA5_CAMPAIGN_SCHEDULE_CADENCE`;
  - `SUBMISSION_INTERVAL_SECONDS`.
- Supported cadence values in current orchestration config: `30m`, `1h`, and `2h`.
- Default orchestration cadence: `2h`.
- Default provider submission interval: `1800` seconds.
- Scheduled monthly slot spacing: one hour.
- Active application timezone: `Asia/Jakarta`.
- Active dataset: `reanalysis-era5-land`.
- Active variables: `tp`, `t2m`, `swvl1`, `ssrd`, `d2m`.
- Active Aceh bounding box:
  - north: `6.1`;
  - south: `1.9`;
  - east: `98.3`;
  - west: `94.9`.
- No ARCO workflow terminology was found in the two README files under review.
- No obsolete DAG name `cds_era5_land_historical_backfill` was found in the two README files under review.
- No obsolete task name `plan_monthly_requests` was found in the two README files under review.
- No deprecated Aceh bbox values were found in the two README files under review.

### Audit Decision

Phase 1 confirms the planned documentation split:

- `/README.md` will be rewritten as the global, business-oriented entry point for the ETL project.
- `/orchestration/README.md` will become a concise Airflow operations guide.
- Historical evidence will not be used as the primary structure of the orchestration README.
- Safety-critical retry, recovery, persistence, and deployment rules will be preserved even when surrounding explanations are shortened.
- No implementation change is required or authorized by this audit.

---

# Phase 2 — Refactor `/README.md`

## 2.1 Project Introduction

- [x] Keep the opening description global and concise.
- [x] Explain that the program transforms Copernicus CDS ERA5-Land hourly data into local daily climate observations.
- [x] Mention PostgreSQL and reproducible artifacts without introducing Airflow internals.
- [x] Keep Airflow as an optional orchestration layer and link to `orchestration/README.md`.

## 2.2 Prerequisites

Add a root-level `## Prerequisites` section containing:

- [x] Python 3.12.
- [x] PostgreSQL for canonical daily climate observations.
- [x] CDS API credentials.
- [x] A single host, or a shared `data/` filesystem visible to every task worker.

- [ ] Do not repeat these generic prerequisites in the orchestration README. *(Deferred to Phase 3.)*

## 2.3 How It Works

- [x] Replace the current technical pipeline diagram with a narrative explanation.
- [x] Explain the process from a business/process perspective rather than implementation boundaries.
- [x] Avoid unnecessary use of terms such as runtime boundary, provider transport, UPSERT, interfaces, or internal module names.
- [x] Explain that the system:
  - obtains hourly climate data;
  - validates the source;
  - forms local daily observations;
  - preserves reproducible artifacts;
  - stores canonical observations;
  - makes the stored data available for analysis/modelling.
- [x] Keep idempotency understandable in plain language.

## 2.4 Variables and Daily Outputs

- [x] Keep the variables/output table if it remains useful.
- [x] Verify that variable names, daily operation, and units match current behavior.
- [x] Avoid adding scientific detail already documented elsewhere.

## 2.5 Local-Day Time Semantics

- [x] Define `D` explicitly.
- [x] Define `A(t)` explicitly as the accumulated ERA5-Land value at UTC time `t`.
- [x] Explain why a UTC+7 local day crosses two UTC dates.
- [x] Add one concrete local-day date example.
- [x] Show the corresponding UTC start and UTC end boundaries.
- [x] Add one simple accumulated-variable example, preferably total precipitation.
- [x] Keep the existing formula only after all symbols are explained.
- [x] Explain that mean variables require the complete hourly samples for the local day.
- [x] Keep missing values as missing; do not imply zero filling.

## 2.6 Project Structure

- [x] Simplify the root project tree.
- [x] Show only major root directories/files.
- [x] Show only the useful high-level structure of `pipeline/`.
- [x] Do not list internal Airflow files in detail.
- [x] Remove `airflow-jwt-key.txt` from the root project-structure explanation.
- [x] Remove Airflow JWT generation detail from the root README.
- [x] Point readers to `orchestration/README.md` for Airflow-specific runtime files.

## 2.7 Pipeline Architecture

- [x] Remove the current architecture diagram based on source filenames/modules.
- [x] Replace it with a conceptual architecture that can be understood without reading code.
- [x] Prefer a flow similar to:

  ```text
  Copernicus CDS
        ↓
  Acquire hourly climate data
        ↓
  Validate source data
        ↓
  Build local daily observations
        ↓
  Preserve reproducible artifacts
        ↓
  Store canonical observations
        ↓
  Analysis / modelling
  ```

- [x] Avoid module/file names in the conceptual diagram.
- [x] If the diagram still makes the README harder to understand, replace it with a short narrative instead.

## 2.8 Environment Setup and `env-example`

- [ ] Keep installation instructions only in the root README. *(Root side completed; orchestration duplicate removal is deferred to Phase 3.)*
- [x] Keep the single `requirements.txt` installation model.
- [x] Expand the environment example to include relevant groups.

### Application and CDS

- [x] `APP_ENV`
- [x] `CDSAPI_URL`
- [x] `CDSAPI_KEY`

### Pipeline PostgreSQL

- [x] Current `POSTGRES_*` variables from `env-example`.

### Integration-Test PostgreSQL

- [x] Current `TEST_POSTGRES_*` variables from `env-example`.

### Campaign

- [x] `CAMPAIGN_KEY`
- [x] `CAMPAIGN_START_YEAR`
- [x] `CAMPAIGN_END_YEAR`

### Orchestration Scheduling

- [x] `ERA5_CAMPAIGN_SCHEDULE_ENABLED`
- [x] `ERA5_CAMPAIGN_SCHEDULE_CADENCE`
- [x] `SUBMISSION_INTERVAL_SECONDS`

- [x] Root README should state what these groups configure at a high level.
- [x] Detailed Airflow behavior for cadence and pacing should remain in `orchestration/README.md`.
- [x] Never place real credentials or secrets in documentation examples.

## 2.9 Running the Pipeline

- [x] Keep the standalone pipeline command concise.
- [x] Explain that configured periods are processed through the same ETL behavior.
- [x] Avoid duplicating Airflow commands here.
- [x] Link to the orchestration README for Airflow operation.

## 2.10 Data Layout

- [x] Keep one representative RAW and transformed artifact example.
- [x] Explain the role of dataset, bounding box, year, and deterministic filename briefly.
- [x] Avoid unnecessary internal storage implementation detail.

## 2.11 Persistence and Modelling Access

- [x] Shorten this section substantially.
- [x] Explain canonical PostgreSQL storage in plain language.
- [x] Explain idempotent rerun behavior in plain language.
- [x] Add one concrete example:
  - process January 2024;
  - rerun the same request;
  - the same logical partition is reused rather than duplicated.
- [x] Explain that modelling reads canonical data without modifying it.
- [x] Move or omit low-level details such as checksum comparison, `daily_climate_id`, fallback UPSERT logic, and overlapping-partition internals unless essential for understanding.

## 2.12 Tests

- [x] Keep root test commands concise.
- [x] Keep detailed test behavior in `tests/README.md`.
- [x] Ensure examples use quiet mode where appropriate.
- [x] Verify current integration-test filenames against the repository before publishing commands.

---


## Phase 2 Result

- Root README is now the global entry point for the ETL project.
- `How It Works` is expressed as business/process narrative instead of checkpoint/task implementation.
- Local-day semantics now define `D` and `A(t)` and include a concrete 2024-05-01 UTC+7 example.
- The source-file-oriented architecture diagram was replaced with a conceptual data flow.
- Root project structure no longer lists Airflow JWT runtime files.
- Airflow JWT generation/authentication details were removed from the root README.
- The environment example now includes standalone dates, application PostgreSQL, isolated test PostgreSQL, campaign identity, scheduler cadence, and CDS submission pacing.
- Persistence/modelling access is now described through canonical storage and an idempotent January 2024 rerun example.
- Root test examples use quiet mode and point detailed testing behavior to `tests/README.md`.
- Removal of duplicated prerequisites/install steps from `orchestration/README.md` remains intentionally deferred to Phase 3.

# Phase 3 — Refactor `/orchestration/README.md`

## 3.1 Opening and Scope

- [ ] Keep the introduction focused on Airflow as an orchestration layer over the existing pipeline.
- [ ] State that scientific transformation and canonical persistence remain owned by `pipeline/`.
- [ ] Keep the instruction to run commands from the repository root if still applicable.
- [ ] Remove generic project prerequisites already documented in `/README.md`.

## 3.2 Replace Setup and Installation

- [ ] Remove the duplicated generic `Setup and Installation` section.
- [ ] Replace it with a short section such as `Airflow Setup After Project Installation`.
- [ ] Assume the reader has already followed `/README.md`.

The Airflow-specific continuation should cover:

- [ ] Run `./generate-airflow-jwt-secret.sh`.
- [ ] Explain what `airflow-jwt-key.txt` is used for.
- [ ] Explain that it is generated locally and ignored by Git.
- [ ] Explain why the secret is not stored in `.env`.
- [ ] Explain that normal activation should reuse the same secret.
- [ ] Explain that regenerating it intentionally rotates the secret.
- [ ] Explain that Airflow components must be restarted after rotation.
- [ ] Run `source activate-airflow.sh`.
- [ ] Run `airflow db migrate`.
- [ ] Configure the one-slot CDS API pool if still required by the current implementation.
- [ ] Show the canonical Airflow startup command used by this project.

## 3.3 Running the Orchestration

- [ ] Make this section shorter.
- [ ] Explain only the essential operational flow:
  - activate Airflow environment;
  - start Airflow;
  - inspect the DAG;
  - choose scheduled or manual operation;
  - inspect run/task state.
- [ ] Avoid repeating installation instructions.
- [ ] Avoid repeating environment-variable descriptions already covered elsewhere.

## 3.4 Project Structure

- [ ] Show only root files directly relevant to Airflow.
- [ ] Show only the major `orchestration/` subdirectories needed to understand ownership.
- [ ] Do not repeat the detailed `pipeline/` tree.
- [ ] Include `airflow-jwt-key.txt` only here if it helps explain Airflow runtime state.
- [ ] Include `generate-airflow-jwt-secret.sh` only here if it helps explain Airflow setup.

## 3.5 Scheduled and Manual Mode Contract

- [ ] Reduce the explanation to a short contract.
- [ ] Explain scheduled mode with one example:
  - scheduled run selects the earliest eligible incomplete quarter;
  - its months are processed by the same ETL boundaries.
- [ ] Explain manual mode with one example:
  - an operator explicitly requests a date range such as one failed calendar month.
- [ ] Explain that manual recovery and scheduled work use the same DAG.
- [ ] Avoid long internal selection/reconciliation implementation detail in the primary explanation.

## 3.6 Scheduler Cadence

- [ ] Refocus this section on `ERA5_CAMPAIGN_SCHEDULE_CADENCE`.
- [ ] Explain that it controls how often Airflow creates an opportunity for scheduled campaign work.
- [ ] State clearly that it does not control CDS download speed.
- [ ] Contrast it briefly with `SUBMISSION_INTERVAL_SECONDS`.
- [ ] Add one simple example, such as:
  - `ERA5_CAMPAIGN_SCHEDULE_CADENCE=2h`;
  - `SUBMISSION_INTERVAL_SECONDS=1800`.
- [ ] Explain that the first represents scheduler cadence and the second represents CDS submission pacing.
- [ ] Avoid repeating detailed queue/concurrency internals unless required for safe operation.

## 3.7 Generic Progress Persistence Foundation

- [ ] Rename the section if a simpler title is clearer, for example `Campaign Progress`.
- [ ] Explain that progress is stored durably in PostgreSQL.
- [ ] Explain that progress survives Airflow process restarts and individual DagRuns.
- [ ] Use a simple quarter example:
  - January complete;
  - February incomplete/failed;
  - March complete;
  - quarter remains incomplete;
  - successful February repair allows quarter completion.
- [ ] Avoid schema-level explanation in the primary narrative.
- [ ] Mention table names only where operational troubleshooting genuinely benefits from them.

## 3.8 Scheduled Selection and Campaign Examples

- [ ] Replace long implementation narratives with short examples.
- [ ] Provide one normal scheduled-quarter example.
- [ ] Provide one failed-month example.
- [ ] Provide one manual-repair example.
- [ ] Provide one restart/resume example.
- [ ] Provide one campaign-progress inspection example.
- [ ] Keep examples aligned with actual current behavior.

## 3.9 Retry and Monthly Start Slots

- [ ] Keep the current behavior documented because it is operationally important.
- [ ] Explain scheduled month start offsets in plain language.
- [ ] Explain that successful months can continue even if another month fails.
- [ ] Explain that failed work is retried after the initial pass/barrier according to current behavior.
- [ ] Keep manual recovery free from scheduled offsets if that remains current behavior.
- [ ] Avoid implementation-level control-metadata terminology unless needed.

## 3.10 Triggering and Status

- [ ] Keep one canonical manual trigger example.
- [ ] Keep one canonical campaign-recovery trigger example.
- [ ] Explain that `airflow dags trigger` creates a new DagRun and returns before completion.
- [ ] Keep one concise command for checking DagRun status.
- [ ] Avoid excessive repeated trigger examples.

## 3.11 Historical Pilot Evidence

Review these current sections carefully:

- [ ] Offline Scheduled Selection Pilot.
- [ ] Controlled Scheduled Pilot on the Airflow Host.
- [ ] Campaign Host Capacity and Post-Change CDS Pilot.
- [ ] Local Pilot Evidence.
- [ ] Verified Three-Month Rollout.
- [ ] Peak Memory examples.
- [ ] Disk-space examples.

For each:

- [ ] Keep only evidence that explains current operational behavior.
- [ ] Convert useful evidence into a compact example where possible.
- [ ] Remove development chronology that no longer helps an operator.
- [ ] Do not present historical durations or storage numbers as service guarantees.
- [ ] If detailed evidence is still valuable, consider moving it to a separate operational/history document rather than keeping it in the main orchestration README.

## 3.12 Backup and Restart Recovery

- [ ] Preserve safety-critical recovery guidance.
- [ ] Shorten procedural detail where possible.
- [ ] Emphasize the state that must be preserved:
  - PostgreSQL application/campaign state;
  - shared artifacts;
  - Airflow runtime metadata where needed;
  - stable Airflow JWT secret.
- [ ] Explain that Airflow SQLite metadata alone is not campaign truth.
- [ ] Explain that a paused DAG does not stop an already running task.
- [ ] Keep manual recovery guidance concise and example-driven.
- [ ] Do not encourage deleting progress rows or pacing state by hand.

## 3.13 Task Flow

- [ ] Keep a simple task-flow diagram if it helps operational understanding.
- [ ] Avoid repeating pipeline-internal architecture.
- [ ] Keep task principles concise.
- [ ] Keep mapped month labels only if they materially help operators interpret the Airflow UI.
- [ ] Simplify or remove the task breakdown table if it duplicates the flow diagram.

## 3.14 Memory and Disk Observability

- [ ] Decide whether detailed historical memory measurements belong in the main README.
- [ ] Prefer documenting what operators should look for in logs.
- [ ] Keep the meaning of peak RSS concise if retained.
- [ ] Keep the disk-space warning behavior concise if retained.
- [ ] Remove historical measurements if they distract from current operation.
- [ ] Preserve the warning that local worker metrics do not represent total host/database capacity.

## 3.15 Safe Retry, Resume, and Deployment

- [ ] Preserve checkpoint-resume behavior.
- [ ] Preserve transactional/idempotent persistence statements.
- [ ] Preserve the rule not to alter/replace artifacts manually as a retry mechanism.
- [ ] Preserve the warning not to deploy a changed DAG graph into an active DagRun.
- [ ] Explain the safe sequence briefly:
  - disable/pause new scheduling;
  - inspect active/deferred work;
  - finish or diagnose existing runs;
  - deploy the new DAG version;
  - run a clean post-change pilot;
  - verify persisted evidence.
- [ ] Avoid long development-history explanation.

## 3.16 Tests

- [ ] Keep only current orchestration test commands.
- [ ] Use quiet mode.
- [ ] Verify current test filenames before finalizing commands.
- [ ] Avoid duplicating detailed testing contracts already documented in `tests/README.md`.

## 3.17 Cleanup and Local State

- [ ] Keep shutdown instructions concise.
- [ ] Explain which Airflow files are local runtime state.
- [ ] Preserve the warning not to remove `orchestration/airflow.db` while Airflow is running.
- [ ] Keep database reset clearly separated from normal retry/shutdown behavior.

---

# Phase 4 — Cross-Document Consistency

- [ ] Root README contains the generic prerequisites.
- [ ] Orchestration README does not repeat generic installation steps.
- [ ] Root README does not explain Airflow JWT internals.
- [ ] Orchestration README explains JWT generation and lifecycle.
- [ ] Root README contains global environment examples.
- [ ] Orchestration README explains Airflow-specific use of scheduling variables.
- [ ] Root README explains ETL behavior in business-oriented language.
- [ ] Orchestration README explains operational behavior with examples.
- [ ] Scientific behavior is not redefined differently between documents.
- [ ] Both documents use the same current variable names.
- [ ] Both documents use the same current DAG/task terminology.
- [ ] Both documents use the current bounding box where a concrete bbox is shown.
- [ ] No ARCO workflow remains documented.
- [ ] No obsolete DAG names remain.
- [ ] No obsolete `backfill` wording remains where the current system is campaign/ETL oriented.
- [ ] Links between root, orchestration, tests, and design-decision documentation remain valid.

---

# Phase 5 — Final Documentation Validation

- [ ] Read `/README.md` from the perspective of a new user who does not know the source code.
- [ ] Confirm the root README explains the business process before implementation details.
- [ ] Read `/orchestration/README.md` from the perspective of an operator who has already installed the project.
- [ ] Confirm the orchestration README can answer normal run, failure, retry, recovery, and scheduled-operation questions without reading development history.
- [ ] Check all code blocks for current commands and filenames.
- [ ] Check all environment-variable names against current `env-example`.
- [ ] Check all Airflow commands against the current DAG name.
- [ ] Check all test commands against current test filenames.
- [ ] Check all relative Markdown links.
- [ ] Check that no documentation-only edit changes or implies changed runtime behavior.
- [ ] Review the final diff and confirm only documentation files were modified.

## Completion Criteria

This refactor is complete when:

- [ ] `/README.md` is a concise global introduction and operating entry point for the ETL project.
- [ ] `/orchestration/README.md` is a concise Airflow operations guide rather than a development-history log.
- [ ] Installation and prerequisites are not unnecessarily duplicated.
- [ ] Local-day semantics are understandable through a concrete example.
- [ ] Architecture is explained conceptually rather than through source filenames.
- [ ] Persistence and progress behavior are explained with examples.
- [ ] Airflow JWT setup is documented only where Airflow-specific operation is explained.
- [ ] Safety-critical retry, recovery, and deployment rules remain documented.
- [ ] The documentation accurately describes the current `dev` behavior without any implementation change.
