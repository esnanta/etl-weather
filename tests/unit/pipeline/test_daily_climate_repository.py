"""Verify daily-climate modelling reads through the public repository boundary.

The tests exercise production selection validation, returned-data validation,
and load_daily_climate_for_modelling(). Query execution is isolated so unit
coverage can assert the generated canonical-table statement and parameters
without pretending to execute PostgreSQL. Actual database persistence behavior
belongs to PostgreSQL integration tests.
"""

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine

from database.config import DatabaseConfig
from pipeline.models.weather import BoundingBox, DataSource
from pipeline.repositories.daily_climate_reader import (
    DailyClimateSelection,
    load_daily_climate_for_modelling,
    validate_daily_climate_selection,
    validate_modelling_dataframe,
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
            north_lat=4.6,
            west_lon=96.8,
            south_lat=4.5,
            east_lon=96.9,
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
            "swvl2_daily_mean_m3_m3": [0.4, None],
            "swvl3_daily_mean_m3_m3": [0.5, 0.51],
            "u10_daily_mean_m_s": [3.0, -2.0],
            "v10_daily_mean_m_s": [4.0, 0.0],
            "wind_speed_daily_mean_m_s": [5.0, 2.0],
            "wind_direction_daily_deg": [216.86989764584402, None],
        }
    )


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

    def fake_execute_modelling_query(connection, statement, params):
        del connection
        captured["sql"] = str(statement)
        captured["params"] = params
        return source_dataframe.copy()

    monkeypatch.setattr(
        "pipeline.repositories.daily_climate_reader._execute_modelling_query",
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
        "swvl2_daily_mean_m3_m3",
        "swvl3_daily_mean_m3_m3",
        "u10_daily_mean_m_s",
        "v10_daily_mean_m_s",
        "wind_speed_daily_mean_m_s",
        "wind_direction_daily_deg",
    ]
    assert result["valid_time_local"].tolist() == [
        pd.Timestamp("2022-01-01 00:00:00"),
        pd.Timestamp("2022-01-02 00:00:00"),
    ]
    assert pd.isna(result.loc[1, "tp_daily_total_mm"])
    assert pd.isna(result.loc[1, "swvl2_daily_mean_m3_m3"])
    assert pd.isna(result.loc[1, "wind_direction_daily_deg"])
    assert result.loc[0, "wind_speed_daily_mean_m_s"] == 5.0

    statement_text = str(captured["sql"])
    assert "climate_data.t_daily_climate AS dc" in statement_text
    assert "climate_data.t_daily_climate_detail AS dcd" in statement_text
    assert "dcd.daily_climate_id = dc.id" in statement_text
    assert "CAST(dc.source AS TEXT) = :source" in statement_text
    assert "dc.dataset_name = :dataset_name" in statement_text
    assert "dcd.valid_time_local >= :start_time" in statement_text
    assert "dcd.valid_time_local < :end_time_exclusive" in statement_text

    assert captured["params"] == {
        "source": "cds",
        "dataset_name": "reanalysis-era5-land",
        "north_lat": 4.6,
        "south_lat": 4.5,
        "east_lon": 96.9,
        "west_lon": 96.8,
        "start_time": pd.Timestamp("2022-01-01 00:00:00").to_pydatetime(),
        "end_time_exclusive": pd.Timestamp("2022-01-03 00:00:00").to_pydatetime(),
    }


def test_modelling_selection_filters_nine_variable_generation_explicitly(monkeypatch) -> None:
    """Keep existing selections unchanged while allowing Plan C disambiguation."""
    from dataclasses import replace
    from pipeline.requests.weather import build_variable_signature

    signature = build_variable_signature([
        "tp", "t2m", "d2m", "swvl1", "swvl2", "swvl3", "ssrd", "u10", "v10",
    ])
    selected = replace(_selection(), variable_signature=signature)
    captured = {}

    def fake_query(connection, statement, params):
        del connection
        captured["sql"] = str(statement)
        captured["params"] = params
        return _dataframe()

    monkeypatch.setattr(
        "pipeline.repositories.daily_climate_reader._execute_modelling_query",
        fake_query,
    )
    engine = create_engine("sqlite://", future=True)
    result = load_daily_climate_for_modelling(selected, _config(), engine=engine)
    assert len(result) == 2
    assert "dc.variable_signature = :variable_signature" in captured["sql"]
    assert captured["params"]["variable_signature"] == signature


def test_modelling_selection_rejects_empty_explicit_signature() -> None:
    from dataclasses import replace

    with pytest.raises(ValueError, match="variable_signature"):
        validate_daily_climate_selection(
            replace(_selection(), variable_signature="   ")
        )
