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

- [x] Do not repeat these generic prerequisites in the orchestration README.

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

- [x] Keep installation instructions only in the root README.
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

- [x] Keep the introduction focused on Airflow as an orchestration layer over the existing pipeline.
- [x] State that scientific transformation and canonical persistence remain owned by `pipeline/`.
- [x] Keep the instruction to run commands from the repository root if still applicable.
- [x] Remove generic project prerequisites already documented in `/README.md`.

## 3.2 Replace Setup and Installation

- [x] Remove the duplicated generic `Setup and Installation` section.
- [x] Replace it with a short section such as `Airflow Setup After Project Installation`.
- [x] Assume the reader has already followed `/README.md`.

The Airflow-specific continuation should cover:

- [x] Run `./generate-airflow-jwt-secret.sh`.
- [x] Explain what `airflow-jwt-key.txt` is used for.
- [x] Explain that it is generated locally and ignored by Git.
- [x] Explain why the secret is not stored in `.env`.
- [x] Explain that normal activation should reuse the same secret.
- [x] Explain that regenerating it intentionally rotates the secret.
- [x] Explain that Airflow components must be restarted after rotation.
- [x] Run `source activate-airflow.sh`.
- [x] Run `airflow db migrate`.
- [x] Configure the one-slot CDS API pool if still required by the current implementation.
- [x] Show the canonical Airflow startup command used by this project.

## 3.3 Running the Orchestration

- [x] Make this section shorter.
- [x] Explain only the essential operational flow:
  - activate Airflow environment;
  - start Airflow;
  - inspect the DAG;
  - choose scheduled or manual operation;
  - inspect run/task state.
- [x] Avoid repeating installation instructions.
- [x] Avoid repeating environment-variable descriptions already covered elsewhere.

## 3.4 Project Structure

- [x] Show only root files directly relevant to Airflow.
- [x] Show only the major `orchestration/` subdirectories needed to understand ownership.
- [x] Do not repeat the detailed `pipeline/` tree.
- [x] Include `airflow-jwt-key.txt` only here if it helps explain Airflow runtime state.
- [x] Include `generate-airflow-jwt-secret.sh` only here if it helps explain Airflow setup.

## 3.5 Scheduled and Manual Mode Contract

- [x] Reduce the explanation to a short contract.
- [x] Explain scheduled mode with one example:
  - scheduled run selects the earliest eligible incomplete quarter;
  - its months are processed by the same ETL boundaries.
- [x] Explain manual mode with one example:
  - an operator explicitly requests a date range such as one failed calendar month.
- [x] Explain that manual recovery and scheduled work use the same DAG.
- [x] Avoid long internal selection/reconciliation implementation detail in the primary explanation.

## 3.6 Scheduler Cadence

- [x] Refocus this section on `ERA5_CAMPAIGN_SCHEDULE_CADENCE`.
- [x] Explain that it controls how often Airflow creates an opportunity for scheduled campaign work.
- [x] State clearly that it does not control CDS download speed.
- [x] Contrast it briefly with `SUBMISSION_INTERVAL_SECONDS`.
- [x] Add one simple example, such as:
  - `ERA5_CAMPAIGN_SCHEDULE_CADENCE=2h`;
  - `SUBMISSION_INTERVAL_SECONDS=1800`.
- [x] Explain that the first represents scheduler cadence and the second represents CDS submission pacing.
- [x] Avoid repeating detailed queue/concurrency internals unless required for safe operation.

## 3.7 Generic Progress Persistence Foundation

- [x] Rename the section if a simpler title is clearer, for example `Campaign Progress`.
- [x] Explain that progress is stored durably in PostgreSQL.
- [x] Explain that progress survives Airflow process restarts and individual DagRuns.
- [x] Use a simple quarter example:
  - January complete;
  - February incomplete/failed;
  - March complete;
  - quarter remains incomplete;
  - successful February repair allows quarter completion.
- [x] Avoid schema-level explanation in the primary narrative.
- [x] Mention table names only where operational troubleshooting genuinely benefits from them.

## 3.8 Scheduled Selection and Campaign Examples

- [x] Replace long implementation narratives with short examples.
- [x] Provide one normal scheduled-quarter example.
- [x] Provide one failed-month example.
- [x] Provide one manual-repair example.
- [x] Provide one restart/resume example.
- [x] Provide one campaign-progress inspection example.
- [x] Keep examples aligned with actual current behavior.

## 3.9 Retry and Monthly Start Slots

- [x] Keep the current behavior documented because it is operationally important.
- [x] Explain scheduled month start offsets in plain language.
- [x] Explain that successful months can continue even if another month fails.
- [x] Explain that failed work is retried after the initial pass/barrier according to current behavior.
- [x] Keep manual recovery free from scheduled offsets if that remains current behavior.
- [x] Avoid implementation-level control-metadata terminology unless needed.

## 3.10 Triggering and Status

- [x] Keep one canonical manual trigger example.
- [x] Keep one canonical campaign-recovery trigger example.
- [x] Explain that `airflow dags trigger` creates a new DagRun and returns before completion.
- [x] Keep one concise command for checking DagRun status.
- [x] Avoid excessive repeated trigger examples.

## 3.11 Historical Pilot Evidence

Review these current sections carefully:

- [x] Offline Scheduled Selection Pilot.
- [x] Controlled Scheduled Pilot on the Airflow Host.
- [x] Campaign Host Capacity and Post-Change CDS Pilot.
- [x] Local Pilot Evidence.
- [x] Verified Three-Month Rollout.
- [x] Peak Memory examples.
- [x] Disk-space examples.

For each:

- [x] Keep only evidence that explains current operational behavior.
- [x] Convert useful evidence into a compact example where possible.
- [x] Remove development chronology that no longer helps an operator.
- [x] Do not present historical durations or storage numbers as service guarantees.
- [x] If detailed evidence is still valuable, consider moving it to a separate operational/history document rather than keeping it in the main orchestration README.

## 3.12 Backup and Restart Recovery

- [x] Preserve safety-critical recovery guidance.
- [x] Shorten procedural detail where possible.
- [x] Emphasize the state that must be preserved:
  - PostgreSQL application/campaign state;
  - shared artifacts;
  - Airflow runtime metadata where needed;
  - stable Airflow JWT secret.
- [x] Explain that Airflow SQLite metadata alone is not campaign truth.
- [x] Explain that a paused DAG does not stop an already running task.
- [x] Keep manual recovery guidance concise and example-driven.
- [x] Do not encourage deleting progress rows or pacing state by hand.

## 3.13 Task Flow

- [x] Keep a simple task-flow diagram if it helps operational understanding.
- [x] Avoid repeating pipeline-internal architecture.
- [x] Keep task principles concise.
- [x] Keep mapped month labels only if they materially help operators interpret the Airflow UI.
- [x] Simplify or remove the task breakdown table if it duplicates the flow diagram.

## 3.14 Memory and Disk Observability

- [x] Decide whether detailed historical memory measurements belong in the main README.
- [x] Prefer documenting what operators should look for in logs.
- [x] Keep the meaning of peak RSS concise if retained.
- [x] Keep the disk-space warning behavior concise if retained.
- [x] Remove historical measurements if they distract from current operation.
- [x] Preserve the warning that local worker metrics do not represent total host/database capacity.

## 3.15 Safe Retry, Resume, and Deployment

- [x] Preserve checkpoint-resume behavior.
- [x] Preserve transactional/idempotent persistence statements.
- [x] Preserve the rule not to alter/replace artifacts manually as a retry mechanism.
- [x] Preserve the warning not to deploy a changed DAG graph into an active DagRun.
- [x] Explain the safe sequence briefly:
  - disable/pause new scheduling;
  - inspect active/deferred work;
  - finish or diagnose existing runs;
  - deploy the new DAG version;
  - run a clean post-change pilot;
  - verify persisted evidence.
- [x] Avoid long development-history explanation.

## 3.16 Tests

- [x] Keep only current orchestration test commands.
- [x] Use quiet mode.
- [x] Verify current test filenames before finalizing commands.
- [x] Avoid duplicating detailed testing contracts already documented in `tests/README.md`.

## 3.17 Cleanup and Local State

- [x] Keep shutdown instructions concise.
- [x] Explain which Airflow files are local runtime state.
- [x] Preserve the warning not to remove `orchestration/airflow.db` while Airflow is running.
- [x] Keep database reset clearly separated from normal retry/shutdown behavior.

---


## Phase 3 Result

- Generic Python/PostgreSQL/CDS/shared-filesystem prerequisites and dependency installation were removed from the orchestration README; the document now assumes `/README.md` has already been followed.
- Airflow-specific continuation now starts with JWT-secret generation, activation, metadata migration, CDS pool setup, and `airflow standalone`.
- `airflow-jwt-key.txt` and `generate-airflow-jwt-secret.sh` are documented only in the Airflow context, including initial creation, reuse, rotation, and component restart requirements.
- Running instructions were reduced to the essential operator flow: activate, start, inspect the DAG, run scheduled/manual work, inspect status, and inspect campaign progress.
- Project structure now shows only Airflow-related root/orchestration paths and relevant operational scripts.
- Scheduled/manual behavior is explained with concrete quarter and whole-month examples.
- Scheduler cadence is centered on `ERA5_CAMPAIGN_SCHEDULE_CADENCE` and explicitly separated from `SUBMISSION_INTERVAL_SECONDS`.
- Generic progress persistence was replaced by a concise `Campaign Progress` explanation using a failed-February quarter example.
- Historical rollout/pilot chronology was removed from the main README and replaced with current-behavior operational examples.
- One-hour scheduled month start slots and the first-pass/retry behavior remain documented in plain language.
- Backup/restart guidance was shortened while preserving PostgreSQL/artifact/JWT state, active-task, pacing, and claim safety rules.
- Task flow was kept as a simple operational diagram without repeating pipeline architecture.
- Historical memory and disk measurements were removed; current observability semantics remain documented.
- Safe retry/resume and safe DAG-deployment rules remain explicit.
- Test commands were verified against current repository filenames and use quiet mode.
- Cleanup now distinguishes Airflow local runtime state from canonical climate data.

# Phase 4 — Cross-Document Consistency

- [x] Root README contains the generic prerequisites.
- [x] Orchestration README does not repeat generic installation steps.
- [x] Root README does not explain Airflow JWT internals.
- [x] Orchestration README explains JWT generation and lifecycle.
- [x] Root README contains global environment examples.
- [x] Orchestration README explains Airflow-specific use of scheduling variables.
- [x] Root README explains ETL behavior in business-oriented language.
- [x] Orchestration README explains operational behavior with examples.
- [x] Scientific behavior is not redefined differently between documents.
- [x] Both documents use the same current variable names.
- [x] Both documents use the same current DAG/task terminology.
- [x] Both documents use the current bounding box where a concrete bbox is shown.
- [x] No ARCO workflow remains documented.
- [x] No obsolete DAG names remain.
- [x] No obsolete `backfill` wording remains where the current system is campaign/ETL oriented.
- [x] Links between root, orchestration, tests, and design-decision documentation remain valid.

---


## Phase 4 Result

- `/README.md` owns generic prerequisites, dependency installation, global environment examples, business-oriented ETL behavior, data layout, persistence, and modelling access.
- `/orchestration/README.md` owns Airflow JWT setup, runtime activation, DAG operation, scheduled/manual mode, cadence, CDS submission pacing, durable campaign progress, retry/recovery, observability, and DAG deployment safety.
- Generic installation steps are no longer duplicated in the orchestration README.
- Airflow JWT internals are absent from the root README and documented only in the Airflow operations guide.
- Root environment examples match the current `env-example` names for application, standalone dates, PostgreSQL, test PostgreSQL, campaign identity, schedule enable/cadence, and submission pacing.
- Scientific ownership is described consistently in both documents: Airflow coordinates execution while `pipeline/` retains transformation, validation, artifact, and canonical persistence behavior.
- Current DAG ID verified against implementation: `cds_era5_land_etl`.
- Current task IDs verified against implementation: `build_monthly_requests`, `attempt_raw_checkpoint`, `resolve_raw_checkpoint`, `ensure_transformed_checkpoint`, and `load`.
- Current Aceh bounding box verified against `config.py`: north `6.1`, south `1.9`, east `98.3`, west `94.9`.
- No ARCO terminology, obsolete DAG name, obsolete task name, deprecated `backfill` wording, or old bbox reference was found in either README.
- Existing relative links from the root README to `orchestration/README.md` and `tests/README.md`, and from the orchestration README back to `README.md` and `tests/README.md`, resolve to files present on branch `dev`.
- Neither README currently contains a direct design-decision-document link; no broken design-decision link was introduced by this refactor.
- No README edit was required during Phase 4 because the cross-document consistency checks passed as written.

# Phase 5 — Final Documentation Validation

- [x] Read `/README.md` from the perspective of a new user who does not know the source code.
- [x] Confirm the root README explains the business process before implementation details.
- [x] Read `/orchestration/README.md` from the perspective of an operator who has already installed the project.
- [x] Confirm the orchestration README can answer normal run, failure, retry, recovery, and scheduled-operation questions without reading development history.
- [x] Check all code blocks for current commands and filenames.
- [x] Check all environment-variable names against current `env-example`.
- [x] Check all Airflow commands against the current DAG name.
- [x] Check all test commands against current test filenames.
- [x] Check all relative Markdown links.
- [x] Check that no documentation-only edit changes or implies changed runtime behavior.
- [x] Review the final diff and confirm only documentation files were modified.

## Completion Criteria

This refactor is complete when:

- [x] `/README.md` is a concise global introduction and operating entry point for the ETL project.
- [x] `/orchestration/README.md` is a concise Airflow operations guide rather than a development-history log.
- [x] Installation and prerequisites are not unnecessarily duplicated.
- [x] Local-day semantics are understandable through a concrete example.
- [x] Architecture is explained conceptually rather than through source filenames.
- [x] Persistence and progress behavior are explained with examples.
- [x] Airflow JWT setup is documented only where Airflow-specific operation is explained.
- [x] Safety-critical retry, recovery, and deployment rules remain documented.
- [x] The documentation accurately describes the current `dev` behavior without any implementation change.

## Phase 5 Result

- `/README.md` was reread as a new-user entry point and now introduces purpose, prerequisites, business process, local-day semantics, conceptual architecture, environment setup, standalone execution, data layout, persistence, modelling access, and basic tests in that order.
- `/orchestration/README.md` was reread as an operator guide after installation and covers Airflow-specific setup, startup, scheduled/manual execution, status inspection, progress, retry, recovery, observability, safe deployment, tests, and cleanup without depending on historical rollout notes.
- Root local-day documentation was checked against `pipeline/transformation/era5_land_daily.py`; the documented accumulated-variable boundary formula matches the implemented calculation.
- `source activate-airflow.sh -q` was checked against `activate-airflow.sh`; quiet mode is supported.
- JWT setup was checked against `generate-airflow-jwt-secret.sh`; it generates `airflow-jwt-key.txt`, applies permission `600`, and instructs operators to restart Airflow components after rotation.
- DAG ID, task IDs, `max_active_runs=1`, and deferrable RAW retry/pacing behavior were checked against the active DAG/operator implementation.
- Scheduler enable/cadence behavior was checked against `orchestration/scheduling/schedule.py`.
- Environment-variable names in the documentation match current `env-example`.
- Test commands reference current files under `tests/unit`, `tests/airflow_orchestration`, and `tests/integration`.
- Relative links between root, orchestration, and testing documentation resolve to files present on branch `dev`.
- Final repository diff from the initial README-refactor checklist commit to current `dev` contains only:
  - `README.md`;
  - `orchestration/README.md`;
  - `orchestration/docs/refactor_readme.md`.
- No `pipeline/`, DAG, configuration implementation, `.env`, database schema, or script file was modified by this README refactor.
- No runtime behavior change is implied or required by the final documentation.

## Refactor Status

README refactor Phases 1 through 5 are complete on branch `dev`.



# Phase 6 — Refactor `/tests/README.md`

This phase extends the documentation-only refactor to the testing guide.

## 6.1 Scope

- [x] Documentation-only change.
- [x] Do not modify tests, pipeline code, orchestration code, configuration implementation, database schema, or scripts.
- [x] Keep `tests/README.md` focused on how testing is organized, what each test layer protects, and how to run the current suites.
- [x] Use `tests/AGENTS.md` for technical testing rules that developers/agents must follow when changing or adding tests.
- [x] Avoid repeating project overview from `/README.md` or Airflow operational guidance from `/orchestration/README.md`.
- [x] Do not maintain a catalog of every test filename; document test locations and responsibilities instead.

## 6.2 Audit Current Content

- [x] Review `tests/README.md` against the current `dev` test directories.
- [x] Compare testing documentation with `/README.md` and `/orchestration/README.md` for overlap.
- [x] Identify stale test filenames and historical baselines.
- [x] Identify sections that are too implementation-oriented for a testing README.
- [x] Identify language that can be simplified without weakening test contracts.

### Audit findings

- The current document still reflects an older pipeline-only testing state and underrepresents the present orchestration and database-progress coverage.
- The `Unit Test Responsibilities` filename list is incomplete compared with the current `tests/unit/` directory.
- A filename-by-filename catalog is not a sustainable documentation pattern as the suite grows. The README should refer to `tests/unit/`, `tests/airflow_orchestration/`, and `tests/integration/` and explain the responsibility of each location.
- Current unit coverage also includes pacing, logging, database recreation, and reset/runtime maintenance behavior.
- Current integration coverage includes:
  - `test_postgresql_idempotency.py`;
  - `test_pipeline_progress_reconciliation.py`;
  - `test_submission_pacing.py`;
  - `test_orchestration_database_bootstrap.py`.
- Current Airflow coverage is much broader than the short list in the README and includes campaign configuration/selection, schedule, progress/reconciliation wiring, reporting, RAW checkpoint deferral/retry, submission pacing/logging, and DAG/task policy.
- Historical baseline text such as `100 unit tests passed`, `119 unit/orchestration tests passed`, and `2 PostgreSQL integration tests passed` is stale and should be removed rather than maintained as permanent documentation.
- The current project baseline previously verified for this refactor context is `208` unit + Airflow orchestration tests and `5` PostgreSQL integration tests; however, the README should prefer reproducible commands over hard-coded pass counts that become stale.
- `Running Tests` should use quiet mode consistently.
- Running the entire `tests/integration` directory is less explicit than listing the intended isolated PostgreSQL integration files.
- The `Known NetCDF Runtime Warning` section should be removed entirely by user decision.
- Several sections repeat implementation detail already better owned by root/design documentation or by `tests/AGENTS.md`, especially:
  - full transformed schema and unit contract;
  - detailed PostgreSQL column/provenance mapping;
  - low-level UPSERT comparison logic;
  - detailed modelling-data-access definition.
- These details may remain in `tests/AGENTS.md` when they are developer-facing testing rules, but `tests/README.md` should only summarize the observable behavior the tests protect.
- `tests/AGENTS.md` already contains technical rules for locked scientific contracts, PostgreSQL identity/idempotency, repository behavior, checkpoint/retry hardening, progress persistence, provenance, and Airflow test boundaries. Do not duplicate these rules into the README.
- `tests/AGENTS.md` currently contains a long `Module Guidance` filename catalog. During this documentation refactor, simplify that section toward directory/responsibility guidance and retain individual filenames only when a specific canonical owner is materially useful.
- The NetCDF runtime warning policy already exists in `tests/AGENTS.md`. The user-requested removal applies to the README; the technical warning policy may remain in AGENTS unless a later decision removes it there as well.
- `Retry, Resume, and Provenance Verification` overlaps with root persistence semantics and can be folded into a simpler test-responsibility section.
- `Checkpoint and Idempotency Regression Rules` is useful but should be rewritten in plain language with a short example.
- `Test Style` and `Definition of Done` remain appropriate, but can be shortened.
- The document should clearly distinguish:
  - unit tests: fast/offline behavior;
  - Airflow orchestration tests: scheduling and orchestration policy without CDS work;
  - PostgreSQL integration tests: real isolated database behavior.

## 6.3 Proposed Structure

Refactor `tests/README.md` toward this structure:

- [x] `# Testing`
- [x] `## Testing Strategy` — concise purpose and three test layers.
- [x] `## What the Tests Protect` — business/scientific correctness in plain language.
- [x] `## Unit Tests` — responsibilities and `tests/unit/` location; do not enumerate every test file.
- [x] `## Airflow Orchestration Tests` — responsibilities and `tests/airflow_orchestration/` location; do not enumerate every test file.
- [x] `## PostgreSQL Integration Tests` — isolated database requirement and `tests/integration/` location; only name specific files inside runnable command examples when necessary.
- [x] `## Idempotency and Recovery Examples` — short examples instead of low-level UPSERT internals.
- [x] `## Running Tests` — canonical quiet commands.
- [x] `## Test Style` — concise development rules.
- [x] `## Definition of Done` — concise acceptance criteria.

## 6.4 Content Changes

- [x] Remove `Known NetCDF Runtime Warning` completely.
- [x] Remove historical test-count baselines.
- [x] Remove or greatly shorten the opening ETL path because the root README already explains the pipeline.
- [x] Reduce `Locked Scientific Contract` to the scientific behaviors tests must protect rather than duplicating the full project data contract; keep developer-level contract detail in `tests/AGENTS.md`.
- [x] Reduce `PostgreSQL Contract Under Test` to observable test expectations; keep low-level identity/UPSERT rules in `tests/AGENTS.md`.
- [x] Avoid detailed provenance-column lists in the README; keep technical provenance rules in `tests/AGENTS.md` where needed.
- [x] Avoid explaining modelling architecture; state only that repository tests protect read-only access and preservation of climate semantics.
- [x] Keep the rule that routine unit tests must not require live PostgreSQL or CDS.
- [x] Keep the rule that external systems should be isolated/mocked in unit tests.
- [x] Keep explicit negative-case testing for validation and persistence behavior.
- [x] Keep temporary-artifact isolation and the rule not to write tests into the real project `data/` directory.
- [x] Keep scientific correctness as part of software correctness, but explain it in plain language.

## 6.5 Canonical Test Commands

Use quiet commands aligned with the current repository:

- [x] Unit + Airflow orchestration regression:

  ```bash
  source activate-airflow.sh

  ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
  python -m pytest tests/unit tests/airflow_orchestration -q
  ```

- [x] Isolated PostgreSQL integration:

  ```bash
  source activate-airflow.sh

  python -m pytest \
    tests/integration/test_postgresql_idempotency.py \
    tests/integration/test_pipeline_progress_reconciliation.py \
    tests/integration/test_submission_pacing.py \
    tests/integration/test_orchestration_database_bootstrap.py \
    -q
  ```

- [x] Explain that integration tests use `TEST_POSTGRES_*` and must point to an isolated test database.
- [x] Do not embed a permanent numeric pass-count baseline in the README.

## 6.6 Cross-Document Boundaries

- [x] Root README remains responsible for installation, environment setup, ETL overview, and basic test entry points.
- [x] Orchestration README remains responsible for Airflow operation and may show its canonical orchestration test commands.
- [x] Tests README becomes the authoritative human-facing explanation of testing strategy, test-layer responsibilities, and canonical commands.
- [x] Tests AGENTS becomes the authoritative developer/agent-facing location for detailed testing rules and invariants.
- [x] Avoid duplicating local-day formulas, project architecture, Airflow JWT setup, campaign operating procedures, or full database schema documentation.
- [x] Avoid filename-by-filename inventories; prefer test directories and responsibility descriptions.
- [x] Link back to root/orchestration documentation where appropriate instead of repeating those explanations.

## 6.6A Tests AGENTS Review

- [x] Review current `tests/AGENTS.md` before moving technical content out of the README.
- [x] Confirm that most detailed testing contracts already exist there.
- [x] Keep technical rules that are still relevant and not duplicated elsewhere.
- [x] Simplify `Module Guidance` so it does not attempt to list every test module.
- [x] Prefer directory-level ownership:
  - `tests/unit/` for fast/offline pipeline and maintenance behavior;
  - `tests/airflow_orchestration/` for Airflow policy and orchestration behavior;
  - `tests/integration/` for isolated real-PostgreSQL behavior.
- [x] Retain individual filenames only for exceptional canonical ownership where naming the exact module materially improves maintenance.
- [x] Review stale terminology such as `backfill` and replace it with current ETL/campaign terminology where appropriate.
- [x] Review historical numeric coverage baselines in AGENTS and remove them if they are being used as permanent documentation rather than transient evidence.
- [x] Keep the NetCDF warning policy in AGENTS for now because it is a technical developer rule; remove only the README warning section as explicitly requested.

## 6.7 Final Validation

- [x] Verify every referenced test file exists on branch `dev`.
- [x] Verify all commands use current filenames and quiet mode.
- [x] Verify no stale numeric baseline remains.
- [x] Verify `Known NetCDF Runtime Warning` is absent.
- [x] Verify terminology matches current pipeline/orchestration documentation.
- [x] Verify only documentation files are changed.

## Phase 6 Result

- `tests/README.md` is now the human-facing testing guide.
- `tests/AGENTS.md` is now the developer/agent-facing location for detailed testing invariants.
- The README no longer duplicates the full ETL path, scientific schema, PostgreSQL column mapping, provenance field catalog, or modelling architecture.
- Test responsibilities are documented by location:
  - `tests/unit/`;
  - `tests/airflow_orchestration/`;
  - `tests/integration/`.
- The README does not maintain a filename-by-filename test inventory.
- Specific integration filenames appear only in the canonical runnable PostgreSQL command where exact selection matters.
- `Known NetCDF Runtime Warning` was removed from `tests/README.md`.
- The NetCDF warning policy remains in `tests/AGENTS.md` as a technical developer rule.
- Historical numeric pass-count baselines were removed from both testing documents.
- Stale `backfill` and historical-DAG terminology was removed from the testing guidance.
- Canonical test commands now use quiet mode.
- Integration documentation requires the isolated `TEST_POSTGRES_*` database.
- `tests/AGENTS.md` no longer contains a long `Module Guidance` catalog; ownership is described by test layer and responsibility.
- Final diff for this phase contains only `tests/README.md` and `tests/AGENTS.md`; no test implementation or runtime code was changed.

## Testing Documentation Status

Phase 6 is complete on branch `dev`.


# Phase 7 — Test Directory Restructure

This phase reorganizes tests by **test level first** and **domain second**.

Unlike Phases 1–6, this phase is not documentation-only because test files will move and path-sensitive test code may require updates. Production/runtime behavior must remain unchanged.

## 7.1 Target Structure

Use this conceptual structure:

```text
tests/
├── unit/
│   ├── pipeline/
│   ├── orchestration/
│   └── operations/
├── integration/
│   ├── pipeline/
│   ├── orchestration/
│   └── operations/
├── conftest.py
├── README.md
└── AGENTS.md
```

Physical directories should be created only when they contain tests. Do not add empty directories only for symmetry.

### Domain meaning

- `pipeline/`: scientific ETL, artifacts, canonical climate persistence, read-only climate access, and pipeline-specific helpers.
- `orchestration/`: Airflow scheduling, DAG/task policy, campaign selection, progress, pacing, retry/deferral, reporting, and orchestration database coordination.
- `operations/`: maintenance and administrative scripts/workflows such as reset, cleanup, recreation, and repository-level operational tooling.

## 7.2 Constraints

- [ ] Do not modify `pipeline/` implementation.
- [ ] Do not modify orchestration/DAG runtime behavior.
- [ ] Do not change scientific contracts.
- [ ] Do not add or remove intended test coverage.
- [ ] Allow only test relocation and the minimum test-path/import/fixture updates required by relocation.
- [ ] Update documentation and CI references to the new paths where required.
- [ ] Preserve `tests/conftest.py` as the shared top-level fixture/environment layer unless a concrete fixture needs narrower scope.
- [ ] Do not create new nested `conftest.py` files merely for symmetry.

## 7.3 Current-to-Target Mapping Audit

### Unit — Pipeline

Move the current pipeline-focused unit tests from `tests/unit/` into `tests/unit/pipeline/`:

- [ ] `test_cds_pacing_chunks.py`
- [ ] `test_config_requests.py`
- [ ] `test_daily_climate_repository.py`
- [ ] `test_database.py`
- [ ] `test_extract.py`
- [ ] `test_extract_cds_normalization.py`
- [ ] `test_load.py`
- [ ] `test_logging.py`
- [ ] `test_request.py`
- [ ] `test_runtime.py`
- [ ] `test_storage.py`
- [ ] `test_transform.py`
- [ ] `test_validation.py`

### Unit — Orchestration

Remove the top-level `tests/airflow_orchestration/` category and move its unit tests into `tests/unit/orchestration/`:

- [ ] `test_campaign.py`
- [ ] `test_campaign_config.py`
- [ ] `test_dag.py`
- [ ] `test_database.py`
- [ ] `test_mode.py`
- [ ] `test_pipeline_progress.py`
- [ ] `test_progress.py`
- [ ] `test_progress_wiring.py`
- [ ] `test_raw_checkpoint_operator.py`
- [ ] `test_reporting.py`
- [ ] `test_schedule.py`
- [ ] `test_scheduled_selection_pilot.py`
- [ ] `test_submission_log.py`
- [ ] `test_submission_pacing.py`
- [ ] `test_tasks.py`

### Unit — Operations

Move maintenance/administrative unit tests into `tests/unit/operations/`:

- [ ] `test_recreate_postgres_database.py`
- [ ] `test_reset_airflow_logs.py`
- [ ] `test_reset_airflow_runtime.py`
- [ ] `test_reset_all.py`
- [ ] `test_reset_data.py`

### Integration — Pipeline

Move pipeline persistence integration coverage into `tests/integration/pipeline/`:

- [ ] `test_postgresql_idempotency.py`

### Integration — Orchestration

Move orchestration persistence/coordination integration coverage into `tests/integration/orchestration/`:

- [ ] `test_pipeline_progress_reconciliation.py`
- [ ] `test_submission_pacing.py`
- [ ] `test_orchestration_database_bootstrap.py`

### Integration — Operations

- [ ] No current integration test clearly belongs to `operations/`.
- [ ] Do not create `tests/integration/operations/` until a real integration test requires that domain.

## 7.4 Package and Path-Sensitivity Audit

- [x] No direct `from tests...` or `import tests...` dependency was found in the current test suite search.
- [x] Shared deterministic environment setup currently lives in `tests/conftest.py` and can remain at the test root.
- [x] Several tests derive repository paths from `Path(__file__).resolve().parents[2]`; moving them one directory deeper will invalidate that assumption.
- [ ] Update path-sensitive tests to derive the repository root correctly after relocation.
- [ ] Prefer a robust repository-root helper/fixture if multiple moved tests need the same calculation, rather than scattering fragile parent indices.
- [ ] Verify DAG-folder resolution after moving orchestration tests.
- [ ] Verify direct-script subprocess tests after moving operations tests.
- [ ] Verify monitoring/data-path assertions after moving orchestration tests.
- [ ] Add `__init__.py` only where package semantics are useful; do not add files solely to mimic the old layout.

Known path-sensitive tests identified during audit include the current:

- `tests/airflow_orchestration/test_dag.py`
- `tests/airflow_orchestration/test_schedule.py`
- `tests/airflow_orchestration/test_tasks.py`
- `tests/unit/test_recreate_postgres_database.py`
- `tests/unit/test_reset_all.py`

## 7.5 Commands After Restructure

Canonical commands should become:

### All unit tests

```bash
source activate-airflow.sh

ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit -q
```

### Unit pipeline only

```bash
python -m pytest tests/unit/pipeline -q
```

### Unit orchestration only

```bash
ERA5_CAMPAIGN_SCHEDULE_ENABLED=false \
python -m pytest tests/unit/orchestration -q
```

### Unit operations only

```bash
python -m pytest tests/unit/operations -q
```

### All integration tests

```bash
python -m pytest tests/integration -q
```

### Integration pipeline only

```bash
python -m pytest tests/integration/pipeline -q
```

### Integration orchestration only

```bash
python -m pytest tests/integration/orchestration -q
```

- [ ] Do not document an `integration/operations` command until that directory exists.

## 7.6 Documentation and Reference Updates

After files move:

- [ ] Update `tests/README.md` to describe level-first/domain-second structure.
- [ ] Update `tests/AGENTS.md` with the new domain ownership.
- [ ] Update `/README.md` test command/path references.
- [ ] Update `/orchestration/README.md` test command/path references.
- [ ] Update root `AGENTS.md` references to specific old test paths.
- [ ] Search documentation for `tests/airflow_orchestration` and replace active references.
- [ ] Search documentation for old `tests/unit/test_*.py` and `tests/integration/test_*.py` references and update only current/authoritative documentation.
- [ ] Preserve historical evidence documents when a path is intentionally historical; do not silently rewrite historical records unless they are intended as current instructions.
- [ ] Update `orchestration/docs/refactor_readme.md` with the final mapping/results.

## 7.7 CI and Pytest Configuration

- [x] Current GitHub Actions workflow runs `python -m pytest` without hard-coded test subpaths, so directory movement should not require a workflow path change.
- [x] Current `pytest.ini` defines the integration marker and warning filter but no test-path-specific collection rules.
- [ ] Verify full GitHub Actions-equivalent collection after the move.
- [ ] Verify no duplicate module-name/import mismatch occurs after introducing same filenames such as `test_database.py` under different domain packages.
- [ ] Resolve duplicate module-name collection issues with package structure/import mode if necessary, without renaming tests unless needed.

## 7.8 Validation Sequence

Run tests in this order after relocation:

- [ ] `tests/unit/pipeline`
- [ ] `tests/unit/orchestration`
- [ ] `tests/unit/operations`
- [ ] all `tests/unit`
- [ ] `tests/integration/pipeline`
- [ ] `tests/integration/orchestration`
- [ ] all `tests/integration`
- [ ] full `python -m pytest -q`

For Airflow-related unit runs, keep `ERA5_CAMPAIGN_SCHEDULE_ENABLED=false` unless schedule behavior itself is under test.

## 7.9 Completion Criteria

Phase 7 is complete when:

- [ ] `tests/airflow_orchestration/` no longer exists.
- [ ] Unit tests are grouped under `pipeline/`, `orchestration/`, and `operations/`.
- [ ] Integration tests are grouped under the applicable domains.
- [ ] No empty domain directory is created only for appearance.
- [ ] Shared fixtures remain correctly scoped.
- [ ] Path-sensitive tests work from their new locations.
- [ ] Documentation references use the new structure.
- [ ] Full pytest collection and regression pass without changing runtime behavior.
