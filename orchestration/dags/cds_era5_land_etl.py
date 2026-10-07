"""Map one selected ERA5-Land quarter through depth-first monthly ETL chains.

Each calendar month is one dynamically mapped TaskGroup so a RAW-complete month
can transform and load while sibling months are still deferred for scheduled
start limits, provider pacing, or bounded extraction retry.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, get_current_context, task, task_group
from airflow.sdk.definitions.param import Param, ParamsDict

from config import validate_and_log_app_environment
from orchestration.scheduling.schedule import campaign_schedule
from orchestration.tasks.build_monthly_requests import prepare_monthly_jobs
from orchestration.operators.raw_checkpoint import RawCheckpointOperator
from orchestration.tasks.load import load_with_progress
from orchestration.tasks.transformed_checkpoint import (
    ensure_transformed_checkpoint_with_progress,
)

DAG_ID = "cds_era5_land_etl"

logger = logging.getLogger(__name__)
validate_and_log_app_environment(logger)


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

    @task(
        task_id="ensure_transformed_checkpoint",
        map_index_template="{{ month_label }}",
        retries=0,
        execution_timeout=timedelta(hours=2),
    )
    def ensure_transformed_checkpoint(
        raw_checkpoint_result: dict[str, object],
    ) -> dict[str, object]:
        context = get_current_context()
        request = raw_checkpoint_result.get(
            "request", raw_checkpoint_result.get("request_spec")
        )
        if not isinstance(request, dict) or not isinstance(
            request.get("start_date"), str
        ):
            raise ValueError(
                "Mapped transformation requires request.start_date "
                "for the Airflow month label."
            )
        context["month_label"] = str(request["start_date"])[:7]
        return ensure_transformed_checkpoint_with_progress(raw_checkpoint_result)

    @task(
        task_id="load",
        map_index_template="{{ month_label }}",
        retries=1,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(hours=2),
    )
    def load(transformed_checkpoint_result: dict[str, object]) -> dict[str, object]:
        context = get_current_context()
        request = transformed_checkpoint_result.get(
            "request", transformed_checkpoint_result.get("request_spec")
        )
        if not isinstance(request, dict) or not isinstance(
            request.get("start_date"), str
        ):
            raise ValueError(
                "Mapped load requires request.start_date "
                "for the Airflow month label."
            )
        context["month_label"] = str(request["start_date"])[:7]
        return load_with_progress(transformed_checkpoint_result)

    @task_group(group_id="monthly_etl")
    def monthly_etl(
        request_spec: dict[str, object],
        progress_id: int,
        run_mode: str,
        earliest_start_at: str | None,
    ) -> None:
        attempt_raw_checkpoint = RawCheckpointOperator(
            task_id="attempt_raw_checkpoint",
            request_spec=request_spec,
            progress_id=progress_id,
            run_mode=run_mode,
            earliest_start_at=earliest_start_at,
            map_index_template="{{ task.month_label }}",
            pool="cds_api",
            retries=0,
            first_pass=True,
            execution_timeout=timedelta(hours=6),
        )

        resolve_raw_checkpoint = RawCheckpointOperator(
            task_id="resolve_raw_checkpoint",
            request_spec=request_spec,
            progress_id=progress_id,
            run_mode=run_mode,
            earliest_start_at=earliest_start_at,
            first_result=attempt_raw_checkpoint.output,
            map_index_template="{{ task.month_label }}",
            pool="cds_api",
            retries=1,
            retry_delay=timedelta(minutes=10),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(hours=1),
            execution_timeout=timedelta(hours=6),
        )

        transformed_checkpoint = ensure_transformed_checkpoint(
            resolve_raw_checkpoint.output
        )
        load(transformed_checkpoint)

    monthly_jobs = build_monthly_requests()
    monthly_etl.expand_kwargs(monthly_jobs)
