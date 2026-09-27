"""Keep campaign ticks configurable while the DAG remains disabled by default.

The scheduler cadence chooses when to seek a quarter; it must remain separate
from durable progress identity and the PostgreSQL CDS submission clock.
"""

from __future__ import annotations

import pytest

from orchestration.schedule import (
    CADENCE_ENV,
    ENABLED_ENV,
    campaign_schedule,
)


@pytest.mark.parametrize(
    ("cadence", "cron"),
    [("30m", "*/30 * * * *"), ("1h", "0 * * * *"),
     ("2h", "0 */2 * * *")],
)
def test_explicit_cadence(cadence: str, cron: str) -> None:
    assert campaign_schedule({ENABLED_ENV: "true", CADENCE_ENV: cadence}) == cron


def test_disabled_default_and_independent_pacing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDS_MIN_SUBMISSION_INTERVAL_SECONDS", "3600")
    assert campaign_schedule({}) is None
    assert campaign_schedule({ENABLED_ENV: "true"}) == "0 */2 * * *"


@pytest.mark.parametrize(
    "config",
    [{ENABLED_ENV: "yes"}, {ENABLED_ENV: "true", CADENCE_ENV: "5m"}],
)
def test_invalid_configuration_fails_explicitly(config: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="ERA5_CAMPAIGN_SCHEDULE"):
        campaign_schedule(config)


def test_enabled_dag_preserves_jakarta_and_serial_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    from pathlib import Path

    monkeypatch.setenv(ENABLED_ENV, "true")
    monkeypatch.setenv(CADENCE_ENV, "2h")
    folder = Path(__file__).resolve().parents[2] / "orchestration" / "dags"
    dag_bag = DagBag(dag_folder=folder)
    assert dag_bag.import_errors == {}
    dag = dag_bag.dags["cds_era5_land_etl"]
    assert dag.schedule == "0 */2 * * *"
    assert dag.timezone.name == "Asia/Jakarta"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
