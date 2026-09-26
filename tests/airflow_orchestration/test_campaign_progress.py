from typing import cast

from sqlalchemy.engine import Engine

from orchestration.campaign import CampaignQuarter
from orchestration.repositories.campaign_progress import (
    create_campaign_progress_table,
    seed_campaign_progress,
)


class _Result:
    rowcount = 1


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.params: list[dict[str, object] | None] = []

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        self.params.append(params)
        return _Result()


class _Context:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> _Connection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()

    def begin(self) -> _Context:
        return _Context(self.connection)


def test_campaign_progress_ddl_uses_climate_schema() -> None:
    engine = _Engine()

    create_campaign_progress_table(cast(Engine, cast(object, engine)))

    sql = "\n".join(engine.connection.statements)
    assert "CREATE SCHEMA IF NOT EXISTS climate" in sql
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "climate.quarterly_progress" in sql
    )
    assert "PRIMARY KEY (campaign_key, year, quarter)" in sql
    assert "configuration_identity TEXT NOT NULL" in sql
    assert "failure_count INTEGER NOT NULL DEFAULT 0" in sql
    assert "status IN ('pending', 'failed', 'complete')" in sql
    assert "climate_data.daily_climate" not in sql


def test_seed_preserves_matching_identity_and_invalidates_changed_identity() -> None:
    engine = _Engine()
    identities = {
        CampaignQuarter(2016, 1): "identity-q1",
        CampaignQuarter(2016, 2): "identity-q2",
    }

    seed_campaign_progress(
        cast(Engine, cast(object, engine)),
        identities,
    )

    sql = "\n".join(engine.connection.statements)
    assert "ON CONFLICT (campaign_key, year, quarter)" in sql
    assert "INSERT INTO climate.quarterly_progress AS current_progress" in sql
    assert "THEN current_progress.status" in sql
    assert "ELSE 'pending'" in sql
    assert "ELSE 0" in sql
    assert len(engine.connection.params) == 2
    assert engine.connection.params[0] == {
        "campaign_key": "era5_land_2016_2025",
        "year": 2016,
        "quarter": 1,
        "configuration_identity": "identity-q1",
    }
