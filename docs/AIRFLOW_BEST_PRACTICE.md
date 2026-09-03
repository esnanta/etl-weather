# AIRFLOW BEST PRACTICES

## 1. Keep DAGs Declarative, Small, and Human-Readable

A DAG should describe **workflow orchestration**, not contain the full
implementation of the workflow.

A reader should be able to understand the pipeline by looking at the DAG
structure without reading every implementation detail.

Prefer:

``` text
extract
   ↓
validate
   ↓
transform
   ↓
load
```

instead of:

``` text
run_everything()
```

Guidelines:

-   Keep DAG files small and focused.
-   Use clear and consistent DAG and task names.
-   Keep task responsibilities explicit.
-   Avoid deeply nested workflow logic.
-   Move reusable business logic into separate Python modules.
-   Avoid placing large functions directly inside DAG files.

The DAG should answer:

> What runs, in what order, under what conditions?

The implementation modules should answer:

> How is the work performed?

------------------------------------------------------------------------

## 2. Treat Airflow as an Orchestrator, Not a Processing Engine

Airflow should coordinate computation, not become the computation
itself.

Avoid putting heavy workloads directly inside Airflow orchestration
code, such as:

-   large-scale data aggregation;
-   multi-GB Pandas transformations;
-   machine-learning training;
-   CPU-intensive processing;
-   large file manipulation;
-   complex analytical workloads.

Instead, Airflow should invoke the appropriate processing component.

For example:

``` text
Airflow Task
     ↓
Python ETL module
```

or:

``` text
Airflow Task
     ↓
Spark / dbt / SQL / container / external service
```

Principle:

> Airflow should call the thing, not be the thing.

------------------------------------------------------------------------

## 3. Separate Orchestration Logic from Business Logic

Airflow-specific code should not own the core ETL logic.

Prefer:

``` text
project/
├── orchestration/
│   ├── dags/
│   └── tasks/
│
├── pipeline/
│   ├── extract.py
│   ├── validation.py
│   ├── transform.py
│   ├── load.py
│   └── storage.py
│
└── tests/
```

The `pipeline/` layer should ideally be executable and testable
independently from Airflow.

For example:

``` python
def transform_products(data):
    ...
```

should contain transformation logic.

The Airflow task should only orchestrate it:

``` python
def task_transform(input_path):
    data = load_raw_dataset(input_path)
    result = transform_products(data)
    return save_processed_dataset(result)
```

This produces a **thin orchestrator** architecture.

Benefits:

-   easier unit testing;
-   easier debugging;
-   reduced Airflow coupling;
-   reusable pipeline logic;
-   easier migration to another orchestrator if ever required.

------------------------------------------------------------------------

## 4. Design Tasks Around Clear Responsibilities

Each task should perform one meaningful responsibility.

Good:

``` text
extract
validate
transform
export_csv
load_database
```

Avoid combining unrelated responsibilities:

``` text
extract_validate_transform_upload_notify()
```

However, task granularity should remain pragmatic.

Do not split trivial operations into dozens of meaningless tasks.

Bad:

``` text
read_file
↓
rename_column
↓
drop_column
↓
convert_price
↓
convert_rating
↓
save_file
```

if all of those operations logically belong to a single transformation
step.

The goal is:

> Tasks should be independently understandable, retryable, and
> observable.

Not:

> Every Python function must become an Airflow task.

------------------------------------------------------------------------

## 5. Make Every Task Idempotent

A task should be safe to execute more than once.

Running a task twice should not corrupt the pipeline.

A retry should not:

-   create duplicate records;
-   duplicate files unnecessarily;
-   send the same external transaction twice;
-   corrupt intermediate data;
-   leave inconsistent partial state.

Prefer deterministic artifact naming or execution-specific paths such
as:

``` text
data/raw/2026-09-03/products.json
```

or:

``` text
data/raw/{dag_run_id}/products.json
```

Database writes should use appropriate mechanisms such as:

``` text
UPSERT
MERGE
ON CONFLICT
```

when applicable.

Principle:

> Retry should be a recovery mechanism, not a data corruption risk.

------------------------------------------------------------------------

## 6. Make Data Dependencies Explicit

Tasks should communicate through clearly defined artifacts or
identifiers.

Prefer:

``` text
extract
   ↓
raw artifact path
   ↓
validate
   ↓
validated artifact path
   ↓
transform
```

over relying on invisible shared state.

Avoid global mutable data or undocumented assumptions about files in
temporary directories.

A downstream task should know exactly what artifact it consumes.

This makes the pipeline:

-   reproducible;
-   observable;
-   testable;
-   easier to rerun;
-   easier to debug.

------------------------------------------------------------------------

## 7. Treat Data Artifacts as First-Class Objects

For data pipelines, passing around the location and metadata of data is
usually better than passing the data itself.

For example:

``` text
DatasetArtifact

dataset     = products
layer       = RAW
created_at  = ...
path        = ...
```

Then the workflow becomes:

``` text
EXTRACT
   ↓
RAW artifact
   ↓
VALIDATE
   ↓
TRANSFORM
   ↓
PROCESSED artifact
   ↓
LOAD / EXPORT
```

A useful conceptual separation is:

``` text
RAW
 ↓
PROCESSED
 ↓
EXPORT / SERVING
```

Do not silently overwrite earlier data layers unless that behavior is
explicitly intended.

------------------------------------------------------------------------

## 8. Use XCom Only for Control Metadata

XCom should transport **small orchestration messages**, not datasets.

Appropriate XCom values include:

-   artifact paths;
-   object IDs;
-   partition names;
-   status values;
-   row counts;
-   timestamps;
-   small configuration values.

A path such as:

``` text
/data/processed/products/20260903.parquet
```

is appropriate.

Avoid putting into XCom:

-   Pandas DataFrames;
-   large JSON responses;
-   HTML documents;
-   binary files;
-   full datasets;
-   thousands of API objects.

Prefer:

``` text
XCom
   ↓
"where is the data?"
```

rather than:

``` text
XCom
   ↓
"here is all the data"
```

------------------------------------------------------------------------

## 9. Prefer Immutable Pipeline Stages

When possible, each pipeline stage should create a new artifact instead
of mutating the previous one.

Prefer:

``` text
raw/products.json
        ↓
processed/products.parquet
        ↓
export/products.csv
```

instead of repeatedly modifying the same file in place.

Immutable stages improve:

-   debugging;
-   rollback;
-   lineage;
-   reproducibility;
-   auditing.

They also make failures easier to isolate.

------------------------------------------------------------------------

## 10. Validate Early and Fail Clearly

Bad input should fail as early as possible.

A typical pipeline should follow:

``` text
Extract
   ↓
Validate
   ↓
Transform
   ↓
Load
```

Validation may include:

-   required columns;
-   schema;
-   data types;
-   null constraints;
-   range checks;
-   duplicate detection;
-   record count;
-   business invariants.

Error messages should identify the actual problem.

Prefer:

``` text
Missing required column: Price
```

over:

``` text
Pipeline failed.
```

Fail fast when continuing would produce incorrect data.

------------------------------------------------------------------------

## 11. Make Task Inputs and Outputs Deterministic

Given the same input and configuration, a task should ideally produce
the same logical result.

Avoid hidden dependencies such as wall-clock time, random values,
current working directories, or global mutable variables unless they are
explicitly part of task behavior.

Execution-specific information should preferably come from Airflow
context, such as:

-   `logical_date`;
-   `data_interval_start`;
-   `data_interval_end`;
-   `run_id`.

This makes backfills and reruns predictable.

------------------------------------------------------------------------

## 12. Design Pipelines Around Airflow Data Intervals

Do not think of scheduled pipelines simply as:

> Run this every hour.

Think instead:

> Process the data belonging to this interval.

Conceptually:

``` text
Airflow run

data_interval_start
        ↓
data_interval_end
        ↓
process exactly this partition
```

This matters for:

-   reruns;
-   backfills;
-   historical processing;
-   partitioned datasets;
-   reproducibility.

Avoid deriving pipeline partitions solely from the wall-clock time at
which the task happens to execute.

------------------------------------------------------------------------

## 13. Make Retries Intentional

Retries should be used for failures that may genuinely recover.

Examples:

-   temporary API failure;
-   network timeout;
-   temporary database connection failure;
-   rate limiting;
-   temporary object-storage issue.

Retries are less useful for deterministic failures such as:

-   invalid schema;
-   missing required column;
-   programming bug;
-   invalid configuration.

Configure deliberately:

-   retries;
-   retry delay;
-   execution timeout.

Avoid infinite or excessive retry behavior.

A task that repeatedly fails because of bad data should fail visibly
rather than quietly retry for hours.

------------------------------------------------------------------------

## 14. Use Sensors Responsibly

Sensors should detect readiness, not occupy resources unnecessarily.

Use appropriate:

-   `poke_interval`;
-   `timeout`.

For long waits, prefer mechanisms that release worker resources, such as
deferrable operators or event-driven approaches where appropriate.

Principle:

> Sensors should check, not camp.

Avoid using a worker slot simply to wait for an external file for hours.

------------------------------------------------------------------------

## 15. Keep DAG Parsing Lightweight

DAG files are repeatedly parsed by Airflow.

Therefore, avoid expensive work at module import time.

Do not perform network requests, large dataset reads, database queries,
or expensive file operations at the top level of a DAG file.

Bad:

``` python
data = pd.read_csv("huge_dataset.csv")

with DAG(...):
    ...
```

Prefer defining the expensive operation inside task runtime.

Top-level DAG code should primarily define orchestration.

------------------------------------------------------------------------

## 16. Be Careful with Dynamic DAG Generation

Dynamic DAG generation can be useful, but should solve a real
orchestration problem.

Avoid generating hundreds or thousands of nearly identical DAGs when one
parameterized DAG or dynamic task mapping would represent the workflow
more clearly.

Prefer:

``` text
one logical pipeline
+
configuration
```

over thousands of nearly identical DAG definitions when the workflows
are fundamentally identical.

------------------------------------------------------------------------

## 17. Control Concurrency Deliberately

More parallelism does not automatically mean better performance.

Consider:

-   Airflow worker capacity;
-   database connection limits;
-   API rate limits;
-   CPU;
-   memory;
-   external service capacity.

Use appropriate controls such as:

-   pools;
-   `max_active_runs`;
-   task concurrency;
-   DAG concurrency;
-   executor capacity.

Do not allow hundreds of tasks to simultaneously overwhelm the same
database or API.

Principle:

> Parallelism must respect the capacity of the systems being
> orchestrated.

------------------------------------------------------------------------

## 18. Keep Configuration Outside Business Logic

Values that vary by environment should not be scattered throughout the
code.

Separate:

``` text
code
configuration
credentials
runtime parameters
```

Use appropriate mechanisms such as:

-   environment variables;
-   Airflow Connections;
-   Airflow Variables;
-   secret backends;
-   runtime parameters.

Do not abuse Airflow Variables as a general-purpose application
database.

------------------------------------------------------------------------

## 19. Never Store Secrets in DAG Code

Credentials must not be committed into source control.

Do not hard-code API keys, database passwords, tokens, or other secrets.

Use:

-   Airflow Connections;
-   environment secrets;
-   HashiCorp Vault;
-   AWS Secrets Manager;
-   GCP Secret Manager;
-   Azure Key Vault;

depending on infrastructure.

Also ensure secrets do not appear accidentally in logs.

------------------------------------------------------------------------

## 20. Make Logging Structured and Useful

Every important task should provide enough information to answer:

-   What ran?
-   What input was used?
-   What output was produced?
-   How many records were processed?
-   How long did it take?
-   Why did it fail?

Useful log information might include:

``` text
dataset=products
input_records=1000
valid_records=930
invalid_records=70
output_path=/data/processed/products.parquet
duration_seconds=4.8
```

Avoid logging entire datasets.

Also avoid meaningless logs such as `Starting...`, `Processing...`, and
`Done...` without useful execution context.

------------------------------------------------------------------------

## 21. Observability Is Part of Pipeline Design

Logs alone are not enough.

Important pipelines should expose useful operational signals such as:

-   task duration;
-   failure rate;
-   retry count;
-   processed record count;
-   data freshness;
-   data volume;
-   pipeline latency.

Alert on meaningful failures.

Avoid alert fatigue by notifying on conditions that actually require
attention.

The goal is not:

> Tell me everything.

The goal is:

> Tell me when something important is wrong.

------------------------------------------------------------------------

## 22. Test Business Logic Without Airflow

Core ETL code should be testable with ordinary unit tests.

For example:

``` python
def test_transform_products():
    ...
```

should not require starting an Airflow scheduler.

Test layers can be separated conceptually into:

``` text
Unit Tests
    ↓
pipeline functions

Integration Tests
    ↓
multiple pipeline stages

DAG Tests
    ↓
DAG structure and dependencies

End-to-End Tests
    ↓
complete execution
```

Most business logic should be covered at the unit and integration
levels.

This keeps tests faster and simpler.

------------------------------------------------------------------------

## 23. Test Pipeline Contracts, Not Only Functions

ETL stages have contracts.

For example, an extraction stage may promise:

``` text
Title
Price
Rating
Colors
Size
Gender
timestamp
```

The next stage should be able to rely on that contract.

Tests should therefore verify:

-   required columns;
-   schema compatibility;
-   expected data types;
-   artifact existence;
-   record count expectations.

This prevents one pipeline stage from silently breaking another.

------------------------------------------------------------------------

## 24. Design for Partial Failure

Distributed workflows fail.

Assume that:

``` text
extract succeeds
validate succeeds
transform succeeds
load fails
```

will eventually happen.

The pipeline should allow the failed `load` task to be retried without
unnecessarily rerunning earlier stages when their outputs remain valid.

This is another reason to use explicit, persistent artifacts between
stages.

------------------------------------------------------------------------

## 25. Avoid Hidden Side Effects

A task should make its side effects obvious.

For example:

``` text
upload_google_sheets
```

clearly indicates external mutation.

A generic task called:

``` text
process_data
```

that unexpectedly writes database records, uploads files, sends email,
and deletes temporary objects is difficult to reason about and dangerous
to retry.

Prefer explicit task names and explicit side effects.

------------------------------------------------------------------------

## 26. Use Clear Naming Conventions

Names should communicate intent.

Prefer:

``` text
fashion_etl
extract_products
validate_products
transform_products
export_products_csv
upload_products_google_sheets
```

Avoid:

``` text
task1
task2
run_data
do_process
final_task
final_v2_fixed
```

The DAG graph itself should be readable as documentation.

------------------------------------------------------------------------

## 27. Document the Pipeline's Intent, Not the Obvious Code

Documentation should explain:

-   Why does this DAG exist?
-   What data does it process?
-   What triggers it?
-   What are its inputs?
-   What are its outputs?
-   What external systems does it touch?
-   What assumptions does it make?
-   What happens on failure?

Avoid documentation that merely repeats code.

Bad:

``` text
transform_task transforms the data.
```

Better:

``` text
Transforms validated raw product records into the canonical
product schema used by downstream exports and analytics.
```

------------------------------------------------------------------------

## 28. Version-Control All Workflow Code

DAGs and pipeline code should follow the same engineering discipline as
application code.

Use:

-   Git;
-   branches;
-   pull requests;
-   code review;
-   automated tests;
-   CI. 

Production DAGs should not be manually edited directly on the Airflow
server.

Deployment should be reproducible.

------------------------------------------------------------------------

## 29. Treat the Metadata Database as Infrastructure

Airflow's metadata database should not become a general-purpose storage
location for pipeline datasets.

Maintain it through appropriate operational procedures such as:

-   database backups;
-   retention policies;
-   XCom cleanup;
-   log retention;
-   Airflow database maintenance;
-   version upgrades.

Pipeline data belongs in the appropriate data storage system, not in
Airflow's metadata database.

------------------------------------------------------------------------

## 30. Choose Infrastructure According to Workload

Executor choice should reflect deployment scale rather than ideology.

For example:

``` text
SequentialExecutor
→ very limited/local use

LocalExecutor
→ single-machine parallel execution

CeleryExecutor
→ distributed workers

KubernetesExecutor
→ dynamically provisioned Kubernetes workloads
```

`LocalExecutor` is not inherently bad for production. It can be
reasonable for smaller production environments.

Choose based on:

-   workload;
-   availability requirements;
-   operational complexity;
-   scalability requirements;
-   infrastructure;
-   team capability.

Do not introduce distributed infrastructure merely because it appears
more sophisticated.

------------------------------------------------------------------------

# Core Engineering Principles

1.  DAGs describe orchestration; modules implement computation.
2.  Keep business logic independent from Airflow.
3.  Keep tasks focused, explicit, and independently retryable.
4.  Make every task idempotent.
5.  Pass artifact references, not datasets.
6.  Make data dependencies and pipeline contracts explicit.
7.  Preserve data lineage through clear pipeline stages.
8.  Validate early and fail clearly.
9.  Design around logical data intervals, not wall-clock execution time.
10. Make failures observable and recovery predictable.
11. Keep configuration and secrets outside application logic.
12. Test pipeline logic independently from Airflow.
13. Prefer simple designs over unnecessary orchestration complexity.
14. Optimize for correctness and recoverability before parallelism.
15. Treat Airflow as an orchestrator, not as the data platform itself.

# Architectural Principle

A recommended thin-orchestrator architecture is:

``` text
                   AIRFLOW
              Orchestration Layer
                     │
         ┌───────────┼───────────┐
         ▼           ▼           ▼
      Extract     Transform     Load
       Task         Task         Task
         │           │           │
         ▼           ▼           ▼
    ───────────────────────────────
             PIPELINE LAYER
    ───────────────────────────────
         │           │           │
    extract.py  transform.py   load.py
         │           │           │
         └───────────┼───────────┘
                     ▼
               STORAGE LAYER
                     │
             ┌───────┼────────┐
             ▼       ▼        ▼
            RAW   PROCESSED  EXPORT
```

The architectural rule is:

> **Airflow owns workflow state.**\
> **Pipeline code owns data-processing logic.**\
> **Storage owns data artifacts.**

This separation keeps orchestration thin, business logic independently
testable, data dependencies explicit, and pipeline execution easier to
observe, retry, and maintain.
