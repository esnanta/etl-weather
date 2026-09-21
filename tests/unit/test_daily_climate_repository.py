from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine

import pipeline.repositories.daily_climate as daily_climate_repository
from pipeline.database import DatabaseConfig
from pipeline.model import BoundingBox, DataSource
from pipeline.repositories.daily_climate import (
    DailyClimateSelection,
    load_daily_climate_for_modelling,
    validate_daily_climate_selection,
    validate_modelling_dataframe,
)

build_modelling_statement = getattr(
    daily_climate_repository,
    "_build_modelling_statement",
)


def _config() -> DatabaseConfig:
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="weather",
        user="weather",
        password="secret",
    )


def _selection() -> DailyClimateSelection:
    return DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name="reanalysis-era5-land",
        start_date=date(2022, 1, 1),
        end_date=date(2022, 1, 2),
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )


def _dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "daily_climate_id": [1, 1],
            "valid_time": [
                pd.Timestamp("2021-12-31 17:00:00"),
                pd.Timestamp("2022-01-01 17:00:00"),
            ],
            "valid_time_local": [
                pd.Timestamp("2022-01-01 00:00:00"),
                pd.Timestamp("2022-01-02 00:00:00"),
            ],
            "latitude": [4.5, 4.5],
            "longitude": [96.8, 96.8],
            "tp_daily_total_mm": [10.0, None],
            "t2m_daily_mean_c": [26.0, 25.0],
            "d2m_daily_mean_c": [22.0, 21.0],
            "swvl1_daily_mean_m3_m3": [0.3, 0.31],
            "ssrd_daily_total_j_m2": [12_000_000.0, 13_000_000.0],
        }
    )


def test_build_modelling_statement_uses_canonical_tables_and_filters() -> None:
    sql = str(build_modelling_statement(_config()))

    assert "climate_data.daily_climate AS dc" in sql
    assert "climate_data.daily_climate_detail AS dcd" in sql
    assert "dcd.daily_climate_id = dc.id" in sql
    assert "CAST(dc.source AS TEXT) = :source" in sql
    assert "dc.dataset_name = :dataset_name" in sql
    assert "dcd.valid_time_local >= :start_time" in sql
    assert "dcd.valid_time_local < :end_time_exclusive" in sql


def test_validate_daily_climate_selection_rejects_reversed_period() -> None:
    selection = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name="reanalysis-era5-land",
        start_date=date(2022, 1, 2),
        end_date=date(2022, 1, 1),
        bbox=_selection().bbox,
    )

    with pytest.raises(ValueError, match="start_date"):
        validate_daily_climate_selection(selection)


def test_validate_daily_climate_selection_rejects_empty_dataset_name() -> None:
    selection = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name="   ",
        start_date=date(2022, 1, 1),
        end_date=date(2022, 1, 2),
        bbox=_selection().bbox,
    )

    with pytest.raises(ValueError, match="dataset_name"):
        validate_daily_climate_selection(selection)


def test_validate_daily_climate_selection_rejects_invalid_bbox() -> None:
    invalid_latitude = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name="reanalysis-era5-land",
        start_date=date(2022, 1, 1),
        end_date=date(2022, 1, 2),
        bbox=BoundingBox(
            north_lat=3.4,
            west_lon=96.1,
            south_lat=5.2,
            east_lon=97.6,
        ),
    )
    invalid_longitude = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name="reanalysis-era5-land",
        start_date=date(2022, 1, 1),
        end_date=date(2022, 1, 2),
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=97.6,
            south_lat=3.4,
            east_lon=96.1,
        ),
    )

    with pytest.raises(ValueError, match="north_lat"):
        validate_daily_climate_selection(invalid_latitude)
    with pytest.raises(ValueError, match="east_lon"):
        validate_daily_climate_selection(invalid_longitude)


def test_validate_modelling_dataframe_rejects_missing_required_column() -> None:
    dataframe = _dataframe().drop(columns=["valid_time"])

    with pytest.raises(ValueError, match="valid_time"):
        validate_modelling_dataframe(dataframe, _selection())


def test_validate_modelling_dataframe_rejects_empty_result() -> None:
    dataframe = _dataframe().iloc[0:0].copy()

    with pytest.raises(ValueError, match="No daily climate observations"):
        validate_modelling_dataframe(dataframe, _selection())


def test_validate_modelling_dataframe_rejects_duplicate_local_grid_observation() -> None:
    dataframe = _dataframe()
    duplicate = dataframe.iloc[[0]].copy()
    duplicate["daily_climate_id"] = 2
    dataframe = pd.concat([dataframe, duplicate], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate local-day grid observations"):
        validate_modelling_dataframe(dataframe, _selection())


def test_validate_modelling_dataframe_requires_local_midnight() -> None:
    dataframe = _dataframe()
    dataframe.loc[0, "valid_time_local"] = pd.Timestamp("2022-01-01 01:00:00")

    with pytest.raises(ValueError, match="local midnight"):
        validate_modelling_dataframe(dataframe, _selection())


def test_validate_modelling_dataframe_rejects_rows_outside_requested_period() -> None:
    dataframe = _dataframe()
    dataframe.loc[1, "valid_time_local"] = pd.Timestamp("2022-01-03 00:00:00")

    with pytest.raises(ValueError, match="outside the requested period"):
        validate_modelling_dataframe(dataframe, _selection())


def test_load_daily_climate_for_modelling_preserves_missing_values_and_schema(
    monkeypatch,
) -> None:
    source_dataframe = _dataframe().iloc[::-1].reset_index(drop=True)
    captured: dict[str, object] = {}

    def fake_execute_modelling_query(statement, params):
        captured["sql"] = str(statement)
        captured["params"] = params
        return source_dataframe.copy()

    monkeypatch.setattr(
        "pipeline.repositories.daily_climate._execute_modelling_query",
        fake_execute_modelling_query,
    )

    engine = create_engine("sqlite://", future=True)
    result = load_daily_climate_for_modelling(
        _selection(),
        _config(),
        engine=engine,
    )

    assert list(result.columns) == [
        "daily_climate_id",
        "valid_time",
        "valid_time_local",
        "latitude",
        "longitude",
        "tp_daily_total_mm",
        "t2m_daily_mean_c",
        "d2m_daily_mean_c",
        "swvl1_daily_mean_m3_m3",
        "ssrd_daily_total_j_m2",
    ]
    assert result["valid_time_local"].tolist() == [
        pd.Timestamp("2022-01-01 00:00:00"),
        pd.Timestamp("2022-01-02 00:00:00"),
    ]
    assert pd.isna(result.loc[1, "tp_daily_total_mm"])
    assert captured["params"] == {
        "source": "cds",
        "dataset_name": "reanalysis-era5-land",
        "north_lat": 5.2,
        "south_lat": 3.4,
        "east_lon": 97.6,
        "west_lon": 96.1,
        "start_time": pd.Timestamp("2022-01-01 00:00:00").to_pydatetime(),
        "end_time_exclusive": pd.Timestamp("2022-01-03 00:00:00").to_pydatetime(),
    }
