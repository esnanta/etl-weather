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
        "extract_or_resume",
        "transform_or_resume",
        "load",
    }
    assert dag.get_task("build_monthly_requests").downstream_task_ids == {
        "extract_or_resume"
    }
    assert dag.get_task("extract_or_resume").downstream_task_ids == {
        "transform_or_resume"
    }
    assert dag.get_task("transform_or_resume").downstream_task_ids == {"load"}


def test_etl_dag_boundary_policies(dag) -> None:
    extract = dag.get_task("extract_or_resume")
    transform = dag.get_task("transform_or_resume")
    load = dag.get_task("load")

    assert extract.pool == "cds_api"
    assert extract.retries == 2
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
