"""Verify the mapped Airflow DAG graph and execution policy."""

from datetime import timedelta

import pytest

from ..._paths import REPOSITORY_ROOT

pytest.importorskip("airflow")
from airflow.models import DagBag


DAG_ID = "cds_era5_land_etl"
DAG_FOLDER = REPOSITORY_ROOT / "orchestration" / "dags"


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
        "attempt_raw_checkpoint",
        "resolve_raw_checkpoint",
        "ensure_transformed_checkpoint",
        "load",
    }
    assert dag.get_task("build_monthly_requests").downstream_task_ids == {
        "attempt_raw_checkpoint"
    }
    assert dag.get_task("attempt_raw_checkpoint").downstream_task_ids == {
        "resolve_raw_checkpoint"
    }
    assert dag.get_task("resolve_raw_checkpoint").downstream_task_ids == {
        "ensure_transformed_checkpoint"
    }
    assert dag.get_task("ensure_transformed_checkpoint").downstream_task_ids == {"load"}


def test_etl_dag_boundary_policies(dag) -> None:
    attempt_raw_checkpoint_task = dag.get_task("attempt_raw_checkpoint")
    resolve_raw_checkpoint_task = dag.get_task("resolve_raw_checkpoint")
    ensure_transformed_checkpoint_task = dag.get_task(
        "ensure_transformed_checkpoint"
    )
    load_task = dag.get_task("load")

    assert attempt_raw_checkpoint_task.map_index_template == "{{ task.month_label }}"
    assert resolve_raw_checkpoint_task.map_index_template == "{{ task.month_label }}"
    assert dag.get_task("ensure_transformed_checkpoint").map_index_template == "{{ month_label }}"
    assert dag.get_task("load").map_index_template == "{{ month_label }}"

    assert resolve_raw_checkpoint_task.pool == "cds_api"
    assert resolve_raw_checkpoint_task.retries == 1
    assert resolve_raw_checkpoint_task.execution_timeout == timedelta(hours=6)
    assert ensure_transformed_checkpoint_task.retries == 0
    assert ensure_transformed_checkpoint_task.execution_timeout == timedelta(hours=2)
    assert load_task.retries == 1
    assert load_task.execution_timeout == timedelta(hours=2)


def test_pipeline_package_does_not_import_airflow() -> None:
    project_root = REPOSITORY_ROOT
    for path in (project_root / "pipeline").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "import airflow" not in source
        assert "from airflow" not in source



def test_successful_raw_attempt_flows_directly_to_resolver(dag) -> None:
    attempt_raw_checkpoint_task = dag.get_task("attempt_raw_checkpoint")
    resolve_raw_checkpoint_task = dag.get_task("resolve_raw_checkpoint")

    assert attempt_raw_checkpoint_task.retries == 0
    assert (
        attempt_raw_checkpoint_task.pool
        == resolve_raw_checkpoint_task.pool
        == "cds_api"
    )
    assert attempt_raw_checkpoint_task.downstream_task_ids == {
        "resolve_raw_checkpoint"
    }
    assert resolve_raw_checkpoint_task.retries == 1
