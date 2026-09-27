"""Map one selected ERA5-Land range through progress-aware ETL boundaries.

Extraction uses a deferrable operator so CDS pacing remains independent of
the eventual scheduler cadence and never occupies a worker while waiting.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.definitions.param import Param, ParamsDict

from orchestration.schedule import campaign_schedule
from orchestration.tasks.cds_extract_operator import CdsPacedExtractOperator
from orchestration.tasks.progress import (
    load_with_progress,
    prepare_monthly_jobs,
    transform_with_progress,
)

DAG_ID = "cds_era5_land_etl"


with DAG(
    dag_id=DAG_ID,
    description="ERA5-Land ETL with durable partition progress",
    schedule=campaign_schedule(),
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags={"cds", "era5-land", "etl"},
    params=ParamsDict(
        {
            "start_date": Param(
                None,
                type=["null", "string"],
                format="date",
                description="Inclusive historical start date (YYYY-MM-DD)",
            ),
            "end_date": Param(
                None,
                type=["null", "string"],
                format="date",
                description="Inclusive historical end date (YYYY-MM-DD)",
            ),
        }
    ),
) as dag:

    @task(
        task_id="build_monthly_requests",
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )
    def build_monthly_requests() -> list[dict[str, object]]:
        context = get_current_context()
        dag_run = context["dag_run"]
        return prepare_monthly_jobs(
            dag_run.run_type, dag_run.conf, dag_run.run_id
        )

    @task(task_id="prepare_retry_pass", retries=0)
    def prepare_retry_pass(
        first_results: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        return [
            {
                "request_spec": result.get("request_spec", result.get("request")),
                "progress_id": result["progress_id"],
                "run_mode": result["run_mode"],
                "first_result": result,
            }
            for result in first_results
        ]

    @task(
        task_id="transform_or_resume",
        retries=0,
        execution_timeout=timedelta(hours=2),
    )
    def transform_or_resume(extract_result: dict[str, object]) -> dict[str, object]:
        return transform_with_progress(extract_result)

    @task(
        task_id="load",
        retries=1,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(hours=2),
    )
    def load(transform_result: dict[str, object]) -> dict[str, object]:
        return load_with_progress(transform_result)

    monthly_jobs = build_monthly_requests()
    first_attempts = CdsPacedExtractOperator.partial(
        task_id="first_extract_attempt",
        pool="cds_api",
        retries=0,
        first_pass=True,
        execution_timeout=timedelta(hours=6),
    ).expand_kwargs(monthly_jobs)
    retry_jobs = prepare_retry_pass(first_attempts.output)
    extracted = CdsPacedExtractOperator.partial(
        task_id="extract_or_resume",
        pool="cds_api",
        retries=2,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
        execution_timeout=timedelta(hours=6),
    ).expand_kwargs(retry_jobs)
    transformed = transform_or_resume.expand(extract_result=extracted.output)
    load.expand(transform_result=transformed)

