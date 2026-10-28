# AGENTS.md → INSTRUCTIONS

**Document role: INSTRUCTIONS.** This file tells AI coding agents **how to work in this repository**: which documents to consult, how to plan changes, which boundaries to respect, what to test, and how to report completion. It does not redefine product requirements, operational Airflow policies, or code implementation guidelines.

## 1. Project context

This repository implements **ETL Weather**, a Copernicus CDS ERA5-Land data pipeline that converts hourly source data into validated UTC+7 daily climate observations, persists canonical results in PostgreSQL, and supports Airflow orchestration, geospatial region mapping, operational inspection, and downstream climate analysis.

The active provider path is Copernicus CDS ERA5-Land. Do not add or restore another provider, ARCO path, or incompatible scientific contract without an explicit product/architecture decision in the owning documentation.

## 2. Documentation navigation

Before changing code, read the documents that own the affected topic.

| Document | Role | Owns |
| --- | --- | --- |
| [docs/app_description.md](docs/app_description.md) | **WHAT** | Product scope, scientific and data requirements, architecture decisions, selected technologies, and acceptance criteria |
| [README.md](README.md) | **CURRENT BEHAVIOR / SETUP** | Repository overview, environment setup, operator-facing commands, current architecture, and data layout |
| [pipeline/README.md](pipeline/README.md) | **PIPELINE DOMAIN** | Current pipeline structure, responsibilities, and pipeline-facing usage |
| [orchestration/README.md](orchestration/README.md) | **AIRFLOW OPERATIONS** | Scheduling, pacing, retries, progress, recovery, and operator-facing Airflow behavior |
| [geospatial/README.md](geospatial/README.md) | **GEOSPATIAL DOMAIN** | PostGIS boundaries, imports, mapping, and regional views |
| [query/README.md](query/README.md) | **OPERATIONAL INSPECTION** | Read-only SQL audit queries |
| [docs/python_se_principle.md](docs/python_se_principle.md) | **PYTHON DESIGN** | SOLID principles guided by DRY, KISS, and YAGNI for Python code design and refactoring |
| [tests/AGENTS.md](tests/AGENTS.md) | **TEST INSTRUCTIONS** | Test-specific boundaries and verification rules |
| **AGENTS.md** (this file) | **INSTRUCTIONS** | Agent workflow, repository navigation, change discipline, and completion reporting |

The ERA5-Land source references stored under `docs/ERA5-Land hourly data from 1950 to present/` are upstream scientific evidence. Use them when a change depends on ERA5-Land semantics.

Do not duplicate entire rules across documents. When requirements or architecture change, update the owning WHAT document and then repair references elsewhere. When runtime behavior changes without changing requirements, update the relevant domain README.

## 3. Agent workflow

1. **Inspect first.** Read the affected code, tests, and owning documentation before editing. Distinguish implemented behavior from desired behavior.
2. **Identify ownership.** Decide whether the change belongs to product/scientific requirements, pipeline logic, database persistence, orchestration, geospatial code, operational tooling, or tests.
3. **Scope narrowly.** Prefer the smallest coherent change that satisfies the requested behavior. Do not perform unrelated refactors.
4. **Preserve contracts.** Treat scientific, temporal, artifact, provenance, persistence, and idempotency behavior as explicit contracts. Do not alter them implicitly.
5. **Implement at the owning layer.** Keep Airflow-specific coordination out of reusable ETL code and keep scientific/data processing out of DAG definitions and task wrappers.
6. **Validate.** Run the smallest meaningful test suite first, then broader tests when appropriate. Never claim checks passed if they were not run.
7. **Document.** Update the document that owns any changed requirement, architecture decision, operator command, or domain contract.
8. **Report completion.** Summarize changed files, behavior, tests run, limitations, and any remaining risks.

For exploratory or review tasks, recommendations may be given without modifying code.

## 4. Repository responsibility boundaries

Follow the existing repository layout before creating or moving modules.

- `pipeline/` owns Airflow-independent extraction, validation, transformation, artifact storage, loading, domain requests/models, and climate-data access.
- `orchestration/` owns Airflow scheduling, task dependency graphs, mapping, retries/deferral, pacing, progress, recovery, runtime coordination, and Airflow-specific configuration.
- `database/` owns PostgreSQL connection/bootstrap and canonical database schema creation.
- `geospatial/` owns administrative-boundary source data, validation/import, PostGIS persistence, and grid-to-region mapping. It must remain independently runnable from Airflow.
- `query/` contains read-only SQL inspection and audit queries. Do not turn it into an alternate mutation path.
- `scripts/` contains thin human/CI-facing entrypoints. Reusable business or scientific logic belongs in domain modules.
- `tests/` owns automated verification and fixtures; follow `tests/AGENTS.md` for local rules.
- `docs/` owns application requirements, scientific references, and design documentation.

Do not import Airflow into reusable pipeline or geospatial modules.

## 5. Change discipline

- Use English for code, identifiers, comments, documentation, and commit messages unless a task explicitly requires another language.
- Treat downloaded RAW scientific data and committed geospatial source files as immutable inputs.
- Keep provider, dataset, spatial extent, time semantics, units, and artifact identity explicit.
- Preserve deterministic artifact and request identity.
- Preserve safe rerun/resume behavior and idempotent persistence.
- Keep missing scientific observations as missing values; never silently coerce them to zero.
- Do not bypass validation merely because an artifact or database record already exists.
- Do not introduce compatibility layers for discarded development schemas unless explicitly requested.
- Do not reintroduce ARCO or another data-source runtime branch without an approved design.
- Do not place scientific transformation logic in Airflow DAGs or task-wrapper files.
- Do not send full scientific datasets through Airflow XCom.
- Do not commit credentials, API keys, database passwords, JWT secrets, or other sensitive values.

If an implementation request conflicts with `docs/app_description.md`, stop treating the old requirement as authoritative only after the WHAT document is explicitly changed.

## 6. Python and documentation quality

Before writing or refactoring Python code, read [Python Software Engineering Principles](docs/python_se_principle.md). Apply SOLID pragmatically, with DRY, KISS, and YAGNI as guardrails. Do not introduce abstractions or design patterns without a demonstrated need.

New or materially modified Python modules should begin with a concise module-level docstring that states purpose and important side effects or safety behavior. Keep CLI modules thin and clear about whether they are read-only or state-changing.

Prefer explicit, small interfaces and simple implementations. Apply DRY, KISS, and YAGNI pragmatically; avoid speculative abstractions.

Markdown documentation should start with a concise statement of its role and should link to the owning document rather than restating large contracts.

## 7. Testing requirements

Routine test commands use quiet pytest output:

```bash
python -m pytest tests/unit -q
python -m pytest tests/integration -q
```

Run focused tests first when only one domain changes. Integration tests must use the isolated `TEST_POSTGRES_*` database configuration and must never target the development database.

For changes affecting retry, resume, scientific transformation, persistence, geospatial mapping, destructive maintenance, or Airflow orchestration, verify the relevant failure and repeat-execution paths as well as the happy path.

Follow `tests/AGENTS.md` for detailed test ownership and isolation rules.

## 8. Operational and destructive changes

Inspect maintenance commands before changing them. Preserve environment guards, explicit execution flags, confirmation barriers, and protection of source code/configuration.

Never delete generated data, recreate databases, terminate Airflow processes, or perform other destructive actions unless the task explicitly requires it and the existing safety contract is preserved.

## 9. Definition of done

A task is complete when:

- the requested behavior is implemented in the correct architectural layer;
- the owning documentation remains consistent with the implementation;
- relevant tests and checks pass, or unrun/failed checks are explicitly reported;
- scientific correctness, provenance, data integrity, rerun safety, and security are not weakened;
- no unrelated behavior was changed;
- the completion report identifies changed files, verification performed, and remaining follow-ups.

**Boundary reminder:** `AGENTS.md = INSTRUCTIONS`; `docs/app_description.md = WHAT`; domain READMEs describe current operation and implementation boundaries.
