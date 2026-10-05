"""Map one selected ERA5-Land range through progress-aware ETL boundaries.

Extraction uses a deferrable operator so CDS pacing remains independent of
the eventual scheduler cadence and never occupies a worker while waiting.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.definitions.param import Param, ParamsDict

from config import validate_and_log_app_environment
from orchestration.scheduling.schedule import campaign_schedule
from orchestration.tasks.build_monthly_requests import prepare_monthly_jobs
from orchestration.operators.raw_checkpoint import RawCheckpointOperator
from orchestration.tasks.load import load_with_progress
from orchestration.tasks.transformed_checkpoint import ensure_transformed_checkpoint_with_progress

DAG_ID = "cds_era5_land_etl"

logger = logging.getLogger(__name__)
validate_and_log_app_environment(logger)


def prepare_resolve_raw_checkpoint_kwargs(
    attempt_raw_checkpoint_result: dict[str, object],
) -> dict[str, object]:
    """Build one resolver mapping payload from one first-attempt result."""
    request_spec = attempt_raw_checkpoint_result.get(
        "request_spec",
        attempt_raw_checkpoint_result.get("request"),
    )
    if not isinstance(request_spec, dict):
        raise ValueError("RAW resolver requires first-attempt request metadata.")
    return {
        "request_spec": request_spec,
        "progress_id": attempt_raw_checkpoint_result["progress_id"],
        "run_mode": attempt_raw_checkpoint_result["run_mode"],
        "first_result": attempt_raw_checkpoint_result,
    }


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
        request = raw_checkpoint_result.get("request", raw_checkpoint_result.get("request_spec"))
        if not isinstance(request, dict) or not isinstance(request.get("start_date"), str):
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
        request = transformed_checkpoint_result.get("request", transformed_checkpoint_result.get("request_spec"))
        if not isinstance(request, dict) or not isinstance(request.get("start_date"), str):
            raise ValueError(
                "Mapped load requires request.start_date "
                "for the Airflow month label."
            )
        context["month_label"] = str(request["start_date"])[:7]
        return load_with_progress(transformed_checkpoint_result)

    monthly_jobs = build_monthly_requests()
    attempt_raw_checkpoint_results = RawCheckpointOperator.partial(
        task_id="attempt_raw_checkpoint",
        map_index_template="{{ task.month_label }}",
        pool="cds_api",
        retries=0,
        first_pass=True,
        execution_timeout=timedelta(hours=6),
    ).expand_kwargs(monthly_jobs)

    resolve_raw_checkpoint_inputs = attempt_raw_checkpoint_results.output.map(
        prepare_resolve_raw_checkpoint_kwargs
    )
    resolve_raw_checkpoint_results = RawCheckpointOperator.partial(
        task_id="resolve_raw_checkpoint",
        map_index_template="{{ task.month_label }}",
        pool="cds_api",
        retries=1,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
        execution_timeout=timedelta(hours=6),
    ).expand_kwargs(resolve_raw_checkpoint_inputs)

    ensure_transformed_checkpoint_results = ensure_transformed_checkpoint.expand(
        raw_checkpoint_result=resolve_raw_checkpoint_results.output
    )
    load_results = load.expand(
        transformed_checkpoint_result=ensure_transformed_checkpoint_results
    )

