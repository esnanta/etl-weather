"""Verify the mapped Airflow DAG graph and execution policy."""

from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DagBag


DAG_ID = "cds_era5_land_etl"
DAG_FOLDER = Path(__file__).resolve().parents[2] / "orchestration" / "dags"


@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(dag_folder=DAG_FOLDER)
    assert dag_bag.import_errors == {}
    assert DAG_ID in dag_bag.dags
    return dag_bag.dags[DAG_ID]


def test_etl_dag_policy(dag) -> None:
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 2
    assert dag.timezone.name == "Asia/Jakarta"


def test_etl_dag_task_graph(dag) -> None:
    assert set(dag.task_ids) == {
        "build_monthly_requests",
        "first_extract_attempt",
        "prepare_retry_pass",
        "resolve_extraction",
        "transform_or_resume",
        "load",
    }
    assert dag.get_task("build_monthly_requests").downstream_task_ids == {
        "first_extract_attempt"
    }
    assert dag.get_task("resolve_extraction").downstream_task_ids == {
        "transform_or_resume"
    }
    assert dag.get_task("transform_or_resume").downstream_task_ids == {"load"}


def test_etl_dag_boundary_policies(dag) -> None:
    first = dag.get_task("first_extract_attempt")
    extract = dag.get_task("resolve_extraction")
    transform = dag.get_task("transform_or_resume")
    load = dag.get_task("load")

    assert first.map_index_template == "{{ task.month_label }}"
    assert extract.map_index_template == "{{ task.month_label }}"
    assert dag.get_task("transform_or_resume").map_index_template == "{{ month_label }}"
    assert dag.get_task("load").map_index_template == "{{ month_label }}"

    assert extract.pool == "cds_api"
    assert extract.retries == 1
    assert extract.execution_timeout == timedelta(hours=6)
    assert transform.retries == 0
    assert transform.execution_timeout == timedelta(hours=2)
    assert load.retries == 1
    assert load.execution_timeout == timedelta(hours=2)


def test_pipeline_package_does_not_import_airflow() -> None:
    project_root = Path(__file__).resolve().parents[2]
    for path in (project_root / "pipeline").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "import airflow" not in source
        assert "from airflow" not in source



def test_first_attempt_barrier_and_retry_pass(dag) -> None:
    first = dag.get_task("first_extract_attempt")
    retry = dag.get_task("resolve_extraction")
    assert first.retries == 0
    assert first.pool == retry.pool == "cds_api"
    assert first.downstream_task_ids == {"prepare_retry_pass"}
    assert dag.get_task("prepare_retry_pass").downstream_task_ids == {"resolve_extraction"}
    assert retry.retries == 1
