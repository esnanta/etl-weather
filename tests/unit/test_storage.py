from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
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


def _save_transformed_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_artifact = save_raw_netcdf(_raw_dataset(request))
    transformed_artifact = save_transformed_parquet(
        _transformed_dataframe(),
        request,
        _downloaded_at(),
        _transformed_at(),
        raw_artifact,
    )
    return request, raw_artifact, transformed_artifact


def _rewrite_parquet_provenance(
    artifact,
    *,
    updates: dict[str, str] | None = None,
    remove: set[str] | None = None,
) -> None:
    table = pq.read_table(artifact.path)
    metadata = dict(table.schema.metadata or {})
    for key in remove or set():
        metadata.pop(key.encode("utf-8"), None)
    for key, value in (updates or {}).items():
        metadata[key.encode("utf-8")] = value.encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), artifact.path)


def test_save_raw_netcdf_and_convert_to_dataframe(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    request = _request()

    artifact = save_raw_netcdf(_raw_dataset(request))

    assert artifact_exists(artifact)
    assert artifact.path.suffix == ".nc"
    assert artifact.path.parent == (
        tmp_path
        / "cds_reanalysis-era5-land"
        / "bbox_n5_w95_s5_e95"
        / "raw"
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


def test_artifact_paths_include_bbox_identity(tmp_path) -> None:
    request = _request()

    raw = raw_artifact_for_request(request, data_directory=tmp_path)
    transformed = transformed_artifact_for_request(request, data_directory=tmp_path)

    assert raw.path.relative_to(tmp_path).parts[:3] == (
        "cds_reanalysis-era5-land",
        "bbox_n5_w95_s5_e95",
        "raw",
    )
    assert transformed.path.relative_to(tmp_path).parts[:3] == (
        "cds_reanalysis-era5-land",
        "bbox_n5_w95_s5_e95",
        "transformed",
    )


def test_different_bounding_boxes_do_not_share_artifact_paths(tmp_path) -> None:
    first_request = _request()
    second_request = replace(
        first_request,
        bbox=BoundingBox(
            north_lat=5.2,
            west_lon=96.1,
            south_lat=3.4,
            east_lon=97.6,
        ),
    )

    first_raw = raw_artifact_for_request(first_request, data_directory=tmp_path)
    second_raw = raw_artifact_for_request(second_request, data_directory=tmp_path)
    first_transformed = transformed_artifact_for_request(
        first_request, data_directory=tmp_path
    )
    second_transformed = transformed_artifact_for_request(
        second_request, data_directory=tmp_path
    )

    assert first_raw.path != second_raw.path
    assert first_transformed.path != second_transformed.path
    assert "bbox_n5p2_w96p1_s3p4_e97p6" in second_raw.path.parts
    assert "bbox_n5p2_w96p1_s3p4_e97p6" in second_transformed.path.parts


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
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_dataset = _raw_dataset(request)

    save_raw_netcdf(raw_dataset)

    with pytest.raises(FileExistsError, match="immutable"):
        save_raw_netcdf(raw_dataset)


def test_failed_atomic_raw_write_leaves_no_final_or_temporary_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    request = _request()
    expected_artifact = raw_artifact_for_request(
        request,
        data_directory=tmp_path,
    )
    temporary_path = expected_artifact.path.with_name(
        f".{expected_artifact.path.name}.tmp"
    )

    def fail_after_partial_write(dataset, path, *args, **kwargs):
        del dataset, args, kwargs
        path = Path(path)
        path.write_bytes(b"partial-netcdf")
        raise OSError("simulated NetCDF write failure")

    monkeypatch.setattr(xr.Dataset, "to_netcdf", fail_after_partial_write)

    with pytest.raises(OSError, match="simulated NetCDF write failure"):
        save_raw_netcdf(_raw_dataset(request))

    assert not expected_artifact.path.exists()
    assert not temporary_path.exists()


def test_load_raw_netcdf_preserves_downloaded_at(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
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
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
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
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
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


def test_parquet_provenance_rejects_missing_required_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    _, _, transformed_artifact = _save_transformed_checkpoint(
        tmp_path,
        monkeypatch,
    )
    _rewrite_parquet_provenance(
        transformed_artifact,
        remove={"raw_sha256"},
    )

    with pytest.raises(
        ValueError,
        match="missing provenance metadata: raw_sha256",
    ):
        read_parquet_provenance(transformed_artifact)


def test_transformed_resume_rejects_different_raw_artifact_path(
    tmp_path,
    monkeypatch,
) -> None:
    request, raw_artifact, transformed_artifact = _save_transformed_checkpoint(
        tmp_path,
        monkeypatch,
    )
    _rewrite_parquet_provenance(
        transformed_artifact,
        updates={"raw_artifact_path": "data/other/raw.nc"},
    )

    with pytest.raises(ValueError, match="path does not match"):
        load_transformed_parquet(
            transformed_artifact,
            request,
            raw_artifact,
        )


@pytest.mark.parametrize("timestamp_field", ["downloaded_at", "transformed_at"])
def test_parquet_provenance_rejects_invalid_timestamp(
    tmp_path,
    monkeypatch,
    timestamp_field,
) -> None:
    _, _, transformed_artifact = _save_transformed_checkpoint(
        tmp_path,
        monkeypatch,
    )
    _rewrite_parquet_provenance(
        transformed_artifact,
        updates={timestamp_field: "not-an-iso-timestamp"},
    )

    with pytest.raises(
        ValueError,
        match=f"Invalid {timestamp_field} provenance timestamp",
    ):
        read_parquet_provenance(transformed_artifact)


def test_save_transformed_parquet_refuses_to_overwrite_checkpoint(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
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


def test_failed_atomic_parquet_write_leaves_no_final_or_temporary_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("pipeline.storage.artifacts.DATA_DIRECTORY", tmp_path)
    request = _request()
    raw_artifact = save_raw_netcdf(_raw_dataset(request))
    expected_artifact = transformed_artifact_for_request(
        request,
        data_directory=tmp_path,
    )
    temporary_path = expected_artifact.path.with_name(
        f".{expected_artifact.path.name}.tmp"
    )

    def fail_after_partial_write(table, path, *args, **kwargs):
        del table, args, kwargs
        path = Path(path)
        path.write_bytes(b"partial-parquet")
        raise OSError("simulated Parquet write failure")

    monkeypatch.setattr(
        "pipeline.storage.local.pq.write_table",
        fail_after_partial_write,
    )

    with pytest.raises(OSError, match="simulated Parquet write failure"):
        save_transformed_parquet(
            _transformed_dataframe(),
            request,
            _downloaded_at(),
            _transformed_at(),
            raw_artifact,
        )

    assert not expected_artifact.path.exists()
    assert not temporary_path.exists()


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
