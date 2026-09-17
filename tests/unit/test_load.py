from datetime import datetime, timezone

import pandas as pd

from pipeline.database import DatabaseConfig
from pipeline.load import _prepare_observation_dataframe, upsert_daily_climate_metadata
from pipeline.model import (
    BoundingBox,
    DataSource,
    TransformedWeatherDataset,
    WeatherRequest,
)


def _request() -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-01",
        variables=["tp", "t2m", "swvl1", "ssrd", "d2m"],
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )


def _dataset() -> TransformedWeatherDataset:
    return TransformedWeatherDataset(
        request=_request(),
        transformed_at=datetime.now(timezone.utc),
        dataset=pd.DataFrame(
            {
                "valid_time": [pd.Timestamp("2021-12-31 17:00:00")],
                "valid_time_local": [pd.Timestamp("2022-01-01 00:00:00")],
                "latitude": [4.5],
                "longitude": [96.8],
                "tp": [10.0],
                "t2m": [26.0],
                "swvl1": [0.3],
                "ssrd": [12_000_000.0],
                "d2m": [22.0],
            }
        ),
    )


def _config() -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="weather",
        user="weather",
        password="secret",
    )


def test_database_config_uses_daily_climate_table_names() -> None:
    config = _config()

    assert config.daily_climate_table == "daily_climate"
    assert config.daily_climate_detail_table == "daily_climate_detail"


def test_prepare_observation_dataframe_uses_daily_database_columns() -> None:
    dataframe, rename_map = _prepare_observation_dataframe(
        _dataset(), daily_climate_id=7
    )

    assert rename_map == {
        "tp": "tp_daily_total_mm",
        "t2m": "t2m_daily_mean_c",
        "d2m": "d2m_daily_mean_c",
        "swvl1": "swvl1_daily_mean_m3_m3",
        "ssrd": "ssrd_daily_total_j_m2",
    }
    assert dataframe.loc[0, "daily_climate_id"] == 7
    assert dataframe.loc[0, "tp_daily_total_mm"] == 10.0
    assert dataframe.loc[0, "t2m_daily_mean_c"] == 26.0
    assert "tp" not in dataframe.columns
    assert "t2m" not in dataframe.columns


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _UpsertConnection:
    def __init__(self, daily_climate_id: int):
        self.daily_climate_id = daily_climate_id
        self.statements: list[str] = []
        self.params: list[dict | None] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(params)
        if "INSERT INTO" in sql and "ON CONFLICT (request_key)" in sql:
            return _ScalarResult(self.daily_climate_id)
        raise AssertionError(f"Unexpected SQL: {sql}")


def test_upsert_daily_climate_metadata_uses_request_key_upsert() -> None:
    connection = _UpsertConnection(daily_climate_id=42)

    daily_climate_id = upsert_daily_climate_metadata(_dataset(), connection, _config())

    assert daily_climate_id == 42
    assert len(connection.statements) == 1
    sql = connection.statements[0]
    assert "INSERT INTO climate_data.daily_climate" in sql
    assert "ON CONFLICT (request_key)" in sql
    assert "RETURNING id" in sql
    assert connection.params[0]["request_key"]
