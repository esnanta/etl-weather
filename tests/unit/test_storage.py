from datetime import UTC, datetime

import pandas as pd
import pytest
import xarray as xr

from pipeline.model import (
    BoundingBox,
    DataSource,
    RawWeatherDataset,
    WeatherRequest,
)
from pipeline.storage import (
    artifact_exists,
    calculate_file_sha256,
    convert_netcdf_to_dataframe,
    load_raw_netcdf,
    load_transformed_parquet,
    raw_artifact_for_request,
    read_parquet_provenance,
    save_raw_netcdf,
    save_transformed_parquet,
    transformed_artifact_for_request,
)


def _request() -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2015-01-01",
        end_date="2015-01-01",
        variables=["tp"],
        bbox=BoundingBox(
            north_lat=5.0,
            west_lon=95.0,
            south_lat=5.0,
            east_lon=95.0,
        ),
    )


def _downloaded_at() -> datetime:
    return datetime(2015, 1, 2, 3, 4, 5, tzinfo=UTC)


def _transformed_at() -> datetime:
    return datetime(2015, 1, 2, 4, 5, 6, tzinfo=UTC)


def _raw_dataset(request: WeatherRequest) -> RawWeatherDataset:
    dataset = xr.Dataset(
        {
            "tp": (
                ("valid_time", "latitude", "longitude"),
                [[[1.0]]],
            ),
        },
        coords={
            "valid_time": pd.to_datetime(["2015-01-01 00:00:00"]),
            "latitude": [5.0],
            "longitude": [95.0],
        },
    )
    return RawWeatherDataset(
        request=request,
        downloaded_at=_downloaded_at(),
        raw_data=dataset,
    )


def _transformed_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "valid_time": [pd.Timestamp("2014-12-31 17:00:00")],
            "valid_time_local": [pd.Timestamp("2015-01-01 00:00:00")],
            "latitude": [5.0],
            "longitude": [95.0],
            "tp": [10.0],
        }
    )


def test_save_raw_netcdf_and_convert_to_dataframe(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)
    request = _request()

    artifact = save_raw_netcdf(_raw_dataset(request))

    assert artifact_exists(artifact)
    assert artifact.path.suffix == ".nc"
    assert artifact.path.parent == (
        tmp_path
        / "reanalysis-era5-land"
        / "raw"
        / "cds"
        / "2015"
    )
    assert artifact.path.name == "2015_0101_0101_tp.nc"

    loaded = convert_netcdf_to_dataframe(artifact)
    assert list(loaded.columns) == [
        "valid_time",
        "latitude",
        "longitude",
        "tp",
    ]
    assert len(loaded) == 1
    assert loaded.loc[0, "tp"] == 1.0
    assert loaded.loc[0, "latitude"] == 5.0
    assert loaded.loc[0, "longitude"] == 95.0


def test_file_sha256_detects_byte_changes(tmp_path) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"first")
    first = calculate_file_sha256(path)

    path.write_bytes(b"second")
    second = calculate_file_sha256(path)

    assert first != second
    assert len(first) == 64
    assert len(second) == 64


def test_raw_artifact_path_is_deterministic(tmp_path) -> None:
    request = _request()

    first = raw_artifact_for_request(request, data_directory=tmp_path)
    second = raw_artifact_for_request(request, data_directory=tmp_path)

    assert first.path == second.path


def test_transformed_artifact_path_is_deterministic(tmp_path) -> None:
    request = _request()

    first = transformed_artifact_for_request(request, data_directory=tmp_path)
    second = transformed_artifact_for_request(request, data_directory=tmp_path)

    assert first.path == second.path
    assert first.path.name == "2015_0101_0101_tp.parquet"


def test_save_raw_netcdf_refuses_to_overwrite_existing_checkpoint(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_dataset = _raw_dataset(request)

    save_raw_netcdf(raw_dataset)

    with pytest.raises(FileExistsError, match="immutable"):
        save_raw_netcdf(raw_dataset)


def test_load_raw_netcdf_preserves_downloaded_at(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)
    request = _request()
    artifact = save_raw_netcdf(_raw_dataset(request))

    resumed = load_raw_netcdf(artifact, request)

    assert resumed.request == request
    assert resumed.downloaded_at == _downloaded_at()
    assert "tp" in resumed.raw_data.data_vars
    assert resumed.raw_data["tp"].item() == 1.0


def test_save_and_load_transformed_parquet_checkpoint_with_provenance(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_artifact = save_raw_netcdf(_raw_dataset(request))
    dataframe = _transformed_dataframe()

    artifact = save_transformed_parquet(
        dataframe,
        request,
        _downloaded_at(),
        _transformed_at(),
        raw_artifact,
    )
    provenance = read_parquet_provenance(artifact)
    resumed = load_transformed_parquet(artifact, request, raw_artifact)

    assert artifact_exists(artifact)
    assert artifact.path.suffix == ".parquet"
    assert artifact.created_at == _transformed_at()
    assert provenance["raw_artifact_path"].endswith("2015_0101_0101_tp.nc")
    assert provenance["raw_sha256"] == calculate_file_sha256(raw_artifact.path)
    assert provenance["downloaded_at"] == _downloaded_at().isoformat()
    assert provenance["transformed_at"] == _transformed_at().isoformat()
    assert resumed.request == request
    assert resumed.transformed_at == _transformed_at()
    pd.testing.assert_frame_equal(resumed.dataset, dataframe)


def test_transformed_resume_rejects_changed_raw_checksum(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_artifact = save_raw_netcdf(_raw_dataset(request))
    transformed_artifact = save_transformed_parquet(
        _transformed_dataframe(),
        request,
        _downloaded_at(),
        _transformed_at(),
        raw_artifact,
    )

    with raw_artifact.path.open("ab") as file_handle:
        file_handle.write(b"changed")

    with pytest.raises(ValueError, match="checksum changed"):
        load_transformed_parquet(
            transformed_artifact,
            request,
            raw_artifact,
        )


def test_save_transformed_parquet_refuses_to_overwrite_checkpoint(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_artifact = save_raw_netcdf(_raw_dataset(request))
    dataframe = _transformed_dataframe()

    save_transformed_parquet(
        dataframe,
        request,
        _downloaded_at(),
        _transformed_at(),
        raw_artifact,
    )

    with pytest.raises(FileExistsError, match="resume it explicitly"):
        save_transformed_parquet(
            dataframe,
            request,
            _downloaded_at(),
            _transformed_at(),
            raw_artifact,
        )


def test_load_checkpoint_raises_when_artifact_is_missing(tmp_path) -> None:
    request = _request()
    raw_artifact = raw_artifact_for_request(request, data_directory=tmp_path)
    transformed_artifact = transformed_artifact_for_request(
        request, data_directory=tmp_path
    )

    with pytest.raises(FileNotFoundError, match="RAW artifact not found"):
        load_raw_netcdf(raw_artifact, request)

    with pytest.raises(FileNotFoundError, match="Transformed artifact not found"):
        load_transformed_parquet(transformed_artifact, request, raw_artifact)
