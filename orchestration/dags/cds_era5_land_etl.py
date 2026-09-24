from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.definitions.param import Param, ParamsDict

from orchestration.tasks.era5_land import (
    build_historical_request_specs,
    extract_partition,
    load_partition,
    transform_partition,
)

DAG_ID = "cds_era5_land_etl"


with DAG(
    dag_id=DAG_ID,
    description="Explicit date-range ERA5-Land ETL",
    schedule=None,
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
        params = context["params"]
        return build_historical_request_specs(
            params.get("start_date"),
            params.get("end_date"),
        )

    @task(
        task_id="extract_or_resume",
        pool="cds_api",
        retries=2,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
        execution_timeout=timedelta(hours=6),
    )
    def extract_or_resume(request_spec: dict[str, object]) -> dict[str, object]:
        return extract_partition(request_spec)

    @task(
        task_id="transform_or_resume",
        retries=0,
        execution_timeout=timedelta(hours=2),
    )
    def transform_or_resume(extract_result: dict[str, object]) -> dict[str, object]:
        return transform_partition(extract_result)

    @task(
        task_id="load",
        retries=1,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(hours=2),
    )
    def load(transform_result: dict[str, object]) -> dict[str, object]:
        return load_partition(transform_result)

    monthly_specs = build_monthly_requests()
    extracted = extract_or_resume.expand(request_spec=monthly_specs)
    transformed = transform_or_resume.expand(extract_result=extracted)
    load.expand(transform_result=transformed)
