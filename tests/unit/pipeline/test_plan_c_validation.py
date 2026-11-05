"""Verify Plan C RAW, daily validation, and checkpoint safety offline.

Production validators and filesystem checkpoint functions are exercised directly.
Network and PostgreSQL are not involved; those boundaries have separate suites.
"""

from dataclasses import replace
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from config import CDS_VARIABLES
from pipeline.models.weather import (
    BoundingBox, DataSource, RawWeatherDataset, TransformedWeatherDataset, WeatherRequest,
)
from pipeline.storage import (
    calculate_file_sha256, load_raw_netcdf, load_transformed_parquet,
    raw_artifact_for_request, save_raw_netcdf, save_transformed_parquet,
    transformed_artifact_for_request,
)
from pipeline.transformation.era5_land_daily import transform_data
from pipeline.validation import ValidationError, validate_raw_dataset, validate_transformed_dataset


def _request(variables=None) -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS, dataset="reanalysis-era5-land",
        start_date="2024-01-01", end_date="2024-01-01",
        variables=list(CDS_VARIABLES if variables is None else variables),
        bbox=BoundingBox(north_lat=4.5, south_lat=4.5, east_lon=96.8, west_lon=96.8),
    )


def _raw(request=None) -> RawWeatherDataset:
    request = request or _request()
    timestamps = pd.date_range("2023-12-31 00:00:00", periods=48, freq="h")
    data = {}
    for variable in request.variables:
        if variable == "tp":
            values = [0.024 if t.hour == 0 else t.hour * 0.001 for t in timestamps]
        elif variable == "ssrd":
            values = [86400.0 if t.hour == 0 else t.hour * 3600.0 for t in timestamps]
        else:
            values = [0.3 if variable.startswith("swvl") else 3.0] * len(timestamps)
        data[variable] = (("valid_time", "latitude", "longitude"),
                          np.asarray(values, dtype=float).reshape(-1, 1, 1))
    return RawWeatherDataset(
        request=request, downloaded_at=datetime(2024, 2, 1, tzinfo=UTC),
        raw_data=xr.Dataset(data, coords={
            "valid_time": timestamps, "latitude": [4.5], "longitude": [96.8],
        }),
    )


def _transformed(raw=None):
    raw = raw or _raw()
    frame = raw.raw_data.to_dataframe().reset_index()
    from pipeline.models.weather import LoadedWeatherDataset
    loaded = LoadedWeatherDataset(request=raw.request, downloaded_at=raw.downloaded_at,
                                  raw_data=frame)
    return transform_data(loaded)


def test_raw_accepts_all_nine_provider_variables() -> None:
    validate_raw_dataset(_raw())


@pytest.mark.parametrize("missing", ["swvl2", "swvl3", "u10", "v10"])
def test_raw_rejects_missing_new_provider_variable(missing: str) -> None:
    raw = _raw()
    broken = replace(raw, raw_data=raw.raw_data.drop_vars(missing))
    with pytest.raises(ValidationError, match="Missing requested variables"):
        validate_raw_dataset(broken)


def test_raw_still_rejects_missing_hour_and_wrong_grid() -> None:
    raw = _raw()
    skipped = replace(raw, raw_data=raw.raw_data.isel(valid_time=[i for i in range(48) if i != 12]))
    with pytest.raises(ValidationError, match="continuous 1-hour"):
        validate_raw_dataset(skipped)
    outside = replace(raw, raw_data=raw.raw_data.assign_coords(longitude=[97.0]))
    with pytest.raises(ValidationError, match="east of requested bbox"):
        validate_raw_dataset(outside)


def test_raw_rejects_requested_variable_with_extra_dimension() -> None:
    raw = _raw()
    altered = raw.raw_data.copy()
    altered["u10"] = altered["u10"].expand_dims(member=[0, 1])
    with pytest.raises(ValidationError, match="RAW variable 'u10'.*dimensions"):
        validate_raw_dataset(replace(raw, raw_data=altered))


def test_old_five_variable_raw_does_not_validate_as_nine_variable_request() -> None:
    old = _raw(_request(["tp", "t2m", "d2m", "swvl1", "ssrd"]))
    incompatible = replace(old, request=_request())
    with pytest.raises(ValidationError, match="Missing requested variables"):
        validate_raw_dataset(incompatible)


def test_transformed_accepts_fifteen_columns_and_calm_null_direction() -> None:
    transformed = _transformed()
    assert len(transformed.dataset.columns) == 15
    validate_transformed_dataset(transformed)
    frame = transformed.dataset.copy()
    frame.loc[0, "wind_direction"] = np.nan
    validate_transformed_dataset(replace(transformed, dataset=frame))


@pytest.mark.parametrize("missing", ["wind_speed", "wind_direction"])
def test_transformed_rejects_missing_derived_column(missing: str) -> None:
    transformed = _transformed()
    frame = transformed.dataset.drop(columns=[missing])
    with pytest.raises(ValidationError, match="Missing required columns"):
        validate_transformed_dataset(replace(transformed, dataset=frame))


@pytest.mark.parametrize(("column", "value"), [
    ("swvl2", 1.01), ("swvl3", -0.01), ("wind_speed", -1.0),
    ("wind_direction", -1.0), ("wind_direction", 360.0),
    ("wind_direction", float("inf")), ("wind_speed", "invalid"),
])
def test_transformed_rejects_invalid_new_values(column, value) -> None:
    transformed = _transformed()
    frame = transformed.dataset.copy()
    if isinstance(value, str):
        frame[column] = frame[column].astype(object)
    frame.loc[0, column] = value
    with pytest.raises(ValidationError, match=column):
        validate_transformed_dataset(replace(transformed, dataset=frame))


def test_nine_variable_raw_and_parquet_resume_preserve_provenance(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    raw = _raw()
    raw_artifact = save_raw_netcdf(raw)
    saved_raw = load_raw_netcdf(raw_artifact, raw.request)
    validate_raw_dataset(saved_raw)
    transformed = _transformed(saved_raw)
    validate_transformed_dataset(transformed)
    parquet = save_transformed_parquet(
        transformed.dataset, raw.request, raw.downloaded_at,
        transformed.transformed_at, raw_artifact,
    )
    resumed = load_transformed_parquet(parquet, raw.request, raw_artifact)
    validate_transformed_dataset(resumed)
    pd.testing.assert_frame_equal(transformed.dataset, resumed.dataset)
    assert calculate_file_sha256(raw_artifact.path)
    assert raw_artifact.path.name.endswith("_d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10.nc")
    assert parquet.path.name.endswith("_d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10.parquet")


def test_five_variable_artifacts_do_not_match_nine_variable_paths(tmp_path) -> None:
    old = _request(["tp", "t2m", "d2m", "swvl1", "ssrd"])
    current = _request()
    assert raw_artifact_for_request(old, data_directory=tmp_path).path != (
        raw_artifact_for_request(current, data_directory=tmp_path).path
    )
    assert transformed_artifact_for_request(old, data_directory=tmp_path).path != (
        transformed_artifact_for_request(current, data_directory=tmp_path).path
    )


def test_corrupted_parquet_missing_wind_column_is_rejected_on_resume(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    raw = _raw()
    raw_artifact = save_raw_netcdf(raw)
    transformed = _transformed(raw)
    bad = transformed.dataset.drop(columns=["wind_direction"])
    parquet = save_transformed_parquet(
        bad, raw.request, raw.downloaded_at, transformed.transformed_at,
        raw_artifact,
    )
    resumed = load_transformed_parquet(parquet, raw.request, raw_artifact)
    with pytest.raises(ValidationError, match="Missing required columns"):
        validate_transformed_dataset(resumed)


def test_changed_raw_checksum_blocks_parquet_resume(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    raw = _raw()
    raw_artifact = save_raw_netcdf(raw)
    transformed = _transformed(raw)
    parquet = save_transformed_parquet(
        transformed.dataset, raw.request, raw.downloaded_at,
        transformed.transformed_at, raw_artifact,
    )
    with raw_artifact.path.open("ab") as handle:
        handle.write(b"altered raw checksum")
    with pytest.raises(ValueError, match="checksum changed"):
        load_transformed_parquet(parquet, raw.request, raw_artifact)
