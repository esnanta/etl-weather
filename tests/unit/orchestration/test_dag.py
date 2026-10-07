"""Verify the mapped monthly TaskGroup graph and Airflow execution policy.

These tests cover orchestration structure only. They parse the production DAG
without running CDS, PostgreSQL, or scientific pipeline logic. Pipeline
scientific behavior remains owned by dedicated pipeline unit/integration tests.
"""

from datetime import timedelta

import pytest

from ..._paths import REPOSITORY_ROOT

pytest.importorskip("airflow")
from airflow.models import DagBag


DAG_ID = "cds_era5_land_etl"
DAG_FOLDER = REPOSITORY_ROOT / "orchestration" / "dags"

ATTEMPT_TASK_ID = "monthly_etl.attempt_raw_checkpoint"
RESOLVE_TASK_ID = "monthly_etl.resolve_raw_checkpoint"
TRANSFORM_TASK_ID = "monthly_etl.ensure_transformed_checkpoint"
LOAD_TASK_ID = "monthly_etl.load"


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


def test_etl_dag_uses_one_mapped_monthly_task_group(dag) -> None:
    assert set(dag.task_ids) == {
        "build_monthly_requests",
        ATTEMPT_TASK_ID,
        RESOLVE_TASK_ID,
        TRANSFORM_TASK_ID,
        LOAD_TASK_ID,
    }

    task_group = dag.task_group.get_child_by_label("monthly_etl")
    assert task_group is not None
    assert task_group.is_mapped is True


def test_etl_dag_monthly_chain_is_depth_first(dag) -> None:
    assert dag.get_task("build_monthly_requests").downstream_task_ids == {
        ATTEMPT_TASK_ID
    }
    assert dag.get_task(ATTEMPT_TASK_ID).downstream_task_ids == {
        RESOLVE_TASK_ID
    }
    assert dag.get_task(RESOLVE_TASK_ID).downstream_task_ids == {
        TRANSFORM_TASK_ID
    }
    assert dag.get_task(TRANSFORM_TASK_ID).downstream_task_ids == {LOAD_TASK_ID}
    assert dag.get_task(LOAD_TASK_ID).downstream_task_ids == set()


def test_etl_dag_boundary_policies(dag) -> None:
    attempt_raw_checkpoint_task = dag.get_task(ATTEMPT_TASK_ID)
    resolve_raw_checkpoint_task = dag.get_task(RESOLVE_TASK_ID)
    ensure_transformed_checkpoint_task = dag.get_task(TRANSFORM_TASK_ID)
    load_task = dag.get_task(LOAD_TASK_ID)

    assert attempt_raw_checkpoint_task.map_index_template == "{{ task.month_label }}"
    assert resolve_raw_checkpoint_task.map_index_template == "{{ task.month_label }}"
    assert ensure_transformed_checkpoint_task.map_index_template == "{{ month_label }}"
    assert load_task.map_index_template == "{{ month_label }}"

    assert attempt_raw_checkpoint_task.pool == "cds_api"
    assert attempt_raw_checkpoint_task.retries == 0
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
    attempt_raw_checkpoint_task = dag.get_task(ATTEMPT_TASK_ID)
    resolve_raw_checkpoint_task = dag.get_task(RESOLVE_TASK_ID)

    assert attempt_raw_checkpoint_task.retries == 0
    assert (
        attempt_raw_checkpoint_task.pool
        == resolve_raw_checkpoint_task.pool
        == "cds_api"
    )
    assert attempt_raw_checkpoint_task.downstream_task_ids == {
        RESOLVE_TASK_ID
    }
    assert resolve_raw_checkpoint_task.retries == 1
