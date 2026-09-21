# Data Orchestration with Apache Airflow

This project uses Apache Airflow to orchestrate the ETL (Extract, Transform, Load) pipeline for fashion data. The business logic resides within the `pipeline/` directory, and Airflow acts as the orchestrator.

**Note:** All commands should be run from the root of the project directory.

## Prerequisites

- Python 3.12
- A virtual environment tool (e.g., `venv`)

## Setup and Installation

1.  **Create and activate a Python virtual environment:**

    ```bash
    python3 -m venv myvenv
    source myvenv/bin/activate
    ```

2.  **Install Apache Airflow:**

    ```bash
    AIRFLOW_VERSION=3.2.2
    PYTHON_VERSION=3.12
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    ```

3.  **Initialize Airflow:**

    Run the activation script to set up the environment variables, then initialize the database.

    ```bash
    source activate-airflow.sh
    airflow db migrate
    ```

4.  **Configure Airflow:**

    In `orchestration/airflow.cfg`, it's recommended to disable the example DAGs:

    ```ini
    load_examples = False
    ```

## Running the Orchestration

For local development, the easiest way to run Airflow is using the `standalone` command, which bundles all necessary services into a single process.

```bash
source activate-airflow.sh
airflow standalone
```

You can now access the Airflow UI at `http://localhost:8080`. To stop all services, press `Ctrl+C` in the terminal.

### Alternative: Running Services Individually

If you need more granular control, you can start the services in separate terminals.

1.  **Start the Airflow Services:**

    Open three separate terminals. In each, activate the environment and start the corresponding service:

    *   **Terminal 1: Scheduler**
        ```bash
        source activate-airflow.sh
        airflow scheduler
        ```

    *   **Terminal 2: Webserver**
        ```bash
        source activate-airflow.sh
        airflow webserver
        ```

    *   **Terminal 3: DAG Processor**
        ```bash
        source activate-airflow.sh
        airflow dag-processor
        ```

2.  **Access the Airflow UI:**

    You can now access the Airflow UI at `http://localhost:8080`.

## Project Structure

```
etl-fashion-data-engineer/
├── orchestration/
│   ├── dags/
│   ├── logs/
│   ├── tasks/
│   ├── airflow.cfg
│   └── airflow.db
└── ...
```

## Task Flow

The ETL process is broken down into a series of tasks orchestrated by Airflow.

```text
                     fashion_etl.py
                            │
                            ▼
                     Dag Processor
                            │
                            ▼
                    DAG Definition
                            │
           ┌────────────┬───┴───┬────────────┐
           │            │       │            │
           ▼            ▼       ▼            ▼
     Extract Task   Validate Task   Transform Task
           │            │       │            │
           └────────────┼───────┼────────────┘
                        │
                        ▼
                      Scheduler
                        │
                        ▼
                   DAG Run #{run_id}
                        │
           ┌────────────┬───┴───┬────────────┐
           │            │       │            │
           ▼            ▼       ▼            │
      Extract TI     Validate TI     Transform TI
           │            │       │            │
           └────────────┼───────┼────────────┘
                        │
                        ▼
                  LocalExecutor
                        │
           ┌────────────┬───┴───┬────────────┐
           │            │       │            │
           ▼            ▼       ▼            ▼
     scrape_data()  validate()    transform()
```

### Task Principles

-   **Airflow as Orchestrator:** Airflow is only responsible for triggering and monitoring tasks.
-   **Thin DAGs:** The DAG definitions are lightweight and focus on task dependencies.
-   **Granular and Atomic Tasks:** Each task has a single, well-defined responsibility.
-   **Artifact-based Communication:** Tasks communicate through artifacts (files), not in-memory data.

### Task Breakdown

| Task         | Input         | Output        |
|--------------|---------------|---------------|
| Extract      | Website       | artifact.path |
| Validate     | artifact.path | artifact.path |
| Transform    | artifact.path | artifact.path |
| Export CSV   | artifact.path | artifact.path |
| Upload to GS | artifact.path | status (bool) |

## Cleanup

To stop all Airflow processes:

```bash
pkill -f "airflow"
```

Reset the database:
```bash
pkill -f "airflow"
rm -f orchestration/airflow.db
```