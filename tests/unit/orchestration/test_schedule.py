"""Verify fixed campaign scheduling while keeping the DAG disabled by default."""

from __future__ import annotations

from datetime import timedelta

import pytest

from config import CAMPAIGN_SCHEDULE_INTERVAL_MINUTES
from orchestration.scheduling.schedule import ENABLED_ENV, campaign_schedule
from ..._paths import REPOSITORY_ROOT


def test_disabled_by_default() -> None:
    assert campaign_schedule({}) is None


def test_enabled_uses_fixed_interval() -> None:
    assert CAMPAIGN_SCHEDULE_INTERVAL_MINUTES == 10
    assert campaign_schedule({ENABLED_ENV: "true"}) == timedelta(minutes=10)


@pytest.mark.parametrize("value", ["yes", "1", "enabled"])
def test_invalid_enabled_value_fails_explicitly(value: str) -> None:
    with pytest.raises(ValueError, match="ERA5_CAMPAIGN_SCHEDULE_ENABLED"):
        campaign_schedule({ENABLED_ENV: value})


def test_enabled_dag_preserves_jakarta_and_serial_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    monkeypatch.setenv(ENABLED_ENV, "true")
    folder = REPOSITORY_ROOT / "orchestration" / "dags"
    dag_bag = DagBag(dag_folder=folder, include_examples=False)
    dag_bag.process_file(str(folder / "cds_era5_land_etl.py"), only_if_updated=False, safe_mode=False)
    assert dag_bag.import_errors == {}
    dag = dag_bag.dags["cds_era5_land_etl"]
    assert dag.schedule == timedelta(minutes=10)
    assert dag.timezone.name == "Asia/Jakarta"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
