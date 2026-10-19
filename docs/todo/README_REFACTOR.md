# README Refactor Plan

Branch: `dev`  
Created: 2026-10-19  
Status: planning only — README content has not been refactored yet.

## Goal

Refactor project README files so documentation has a clear hierarchy:

- `/README.md` is the project entry point.
- Root README gives a concise overview of every major domain.
- Root README keeps the important CLI commands users need for setup, execution, verification, testing, and operations.
- Domain README files explain only their own domain, including important domain-specific decisions and operational details.
- Root README links to each domain README for deeper documentation.
- Avoid long narrative sections and repeated explanations across README files.

## README inventory

The `dev` branch currently contains five README files:

- [ ] `/README.md`
- [ ] `/orchestration/README.md`
- [ ] `/geospatial/README.md`
- [ ] `/tests/README.md`
- [ ] `/query/README.md` — discovered during repository-wide README inventory and added to this refactor scope.

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

## Refactor steps

### Phase 1 — Audit

- [x] Inventory every `README.md` on branch `dev`.
- [x] Add the discovered `query/README.md` to scope.
- [ ] Read each README completely and classify each section as:
  - project-wide;
  - orchestration;
  - geospatial;
  - testing;
  - query/audit;
  - duplicated/outdated.
- [ ] Inventory current executable CLI entry points from `scripts/`, `database/`, `geospatial/`, and Airflow commands referenced by the repository.
- [ ] Check root links and README-to-README links for valid relative paths.
- [ ] Check terminology against current architecture and configuration names.

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
- [ ] Consolidate all important CLI commands into one easy-to-scan command reference.
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
