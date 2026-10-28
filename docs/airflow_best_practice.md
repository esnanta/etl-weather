# Apache Airflow Best Practices

## Documentation Role: POLICY

This document defines **which orchestration principles and operational rules must be followed**, and why. It is reusable across projects and does not prescribe project-specific features or code templates.

**Document owner:** Cross-project orchestration standards (**POLICY**).  
**Audience:** Data platform architects, Airflow operators, and workflow maintainers.  
**Scope:** Policies applicable to data engineering, analytics, and ML workflows; no project-specific schemas, providers, datasets, or business SLAs.

For Python implementation patterns, see [Airflow Coding Guidelines](airflow_coding_guidelines.md). Application-specific technology selection and data contracts belong in each application's architecture description.

## 1. Architecture principles

Apache Airflow is responsible for scheduling, workflow state, dependency management, execution governance, and operational visibility. Business transformations, extraction, data validation, storage, and inference are implemented in Airflow-independent libraries or services.

Airflow primarily orchestrates finite workflows; it is not a replacement for a message broker, streaming engine, persistent artifact store, or domain pipeline runtime. Tasks must have identifiable inputs and declared output contracts.

## 2. DAG design principles

- Design a DAG around a clear operational outcome, owner, and measurable completion contract.
- Split work along durable, independently retryable task boundaries, not arbitrary internal functions.
- Declare dependencies explicitly, and limit unnecessary cross-DAG coupling.
- Keep graphs readable and mapping/fan-out bounded.
- DAG parsing must remain predictable and free of external I/O or costly execution.

## 3. Scheduling and backfill policy

- Define schedules and timezone context explicitly; use fixed timezone-aware start dates.
- Use logical data intervals for processing identity, not the worker's wall-clock time. Respect half-open interval semantics and validate any conversion to an inclusive range.
- Decide `catchup` intentionally for each workflow.
- Historical replay must have an approved scope, capacity estimate, source-access constraints, and documented recovery plan.
- Repeated execution for the same logical input must be safe.

## 4. Task execution and state policy

- Make business record and artifact identity deterministic, independent of `run_id` and task attempt.
- Store durable data in databases or object stores; restrict XCom to small, serializable **control metadata and references**.
- A task must not report success before its output contract is met.
- Treat upstream checkpoints and downstream state as separately recoverable where the domain permits.
- Airflow's metadata database must not serve as the primary dataset store.

## 5. Retry and recovery policy

- Retry bounded transient failures; fail fast on deterministic validation, configuration, or authorization errors.
- Set timeouts and retry behavior per task based on expected failure modes; avoid indiscriminate global retries.
- Preserve original exceptions and expose failed states accurately.
- Use bounded, non-blocking waits (deferrable or rescheduling sensors where appropriate).
- Ensure partial failures, clearing, and reruns do not corrupt durable outputs.
- Provide item-level failure/replay paths if a workflow allows partial processing.

## 6. Resource management policy

- Protect limited source APIs, databases, and compute services with pools, concurrency limits, and backpressure.
- Bound active runs and dynamic task mapping.
- Plan capacity using observed throughput, delays, rate limits, and cost rather than assumed scalability.
- Delegate heavy or distributed compute to suitable workers/services instead of misusing the orchestration metadata layer.

## 7. Security and configuration policy

- Use approved secret backends and Airflow Connections; never embed credentials in source, DAG parameters, logs, or XCom.
- Keep deployment, business, and sensitive configurations distinct and validated.
- Apply least privilege, credential rotation, and protection of sensitive source content.
- Ensure external parameters and data are treated as untrusted inputs.

## 8. Observability and operations policy

- Correlate workflow identity (`dag_id`, `task_id`, `run_id`), logical period, business identifiers, attempts, outputs, and durations.
- Alert on actionable failures, freshness breaches, and abnormal backlog or retry behavior, without exposing secrets.
- Maintain documented incident response, rerun/backfill, escalation, and source outage procedures.
- Surface data quality outcomes without reimplementing business validation inside DAGs.

## 9. Testing strategy and quality gates

- **Unit-level:** validate deterministic scheduling/partition helpers and orchestration adapters.
- **DAG-level:** validate parsing, dependency graphs, schedule, timeout/retry/concurrency policy, and import safety.
- **Integration-level:** validate runtime boundary calls and controlled failures.
- **End-to-end:** verify representative input, replay/idempotency, partial failure, and recovery.
- CI must run appropriate static checks and DAG import tests against **pinned compatible Airflow and provider versions**.

The companion coding guide owns actual test structures, examples, naming, and code review steps. Avoid re-specifying code-level instructions here.

## 10. Policy compliance checklist

- [ ] Workflow ownership and output contracts are documented.
- [ ] Business processing remains Airflow-independent.
- [ ] Scheduling and backfill behavior are intentional and safe.
- [ ] Persistent artifacts and stable business identifiers are used.
- [ ] Retry and timeout behavior matches failure classes.
- [ ] Resource limits, security, monitoring, and recovery are defined.
- [ ] Tests exercise orchestration contracts and replay behavior.

## Documentation boundary

This document defines **policy and rationale**, not application requirements or copy-paste code. For implementation use [Airflow Coding Guidelines](airflow_coding_guidelines.md); for a particular application's mandatory tools, schemas, and features use that application's architecture document. Resolve discrepancies in the document responsible for that topic.
