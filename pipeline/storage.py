from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import xarray as xr

from pipeline.interfaces import DataStorage
from pipeline.logging import _log_dataframe_preview
from pipeline.model import (
    DataLayer,
    DatasetArtifact,
    LoadedWeatherDataset,
    RawWeatherDataset,
    TransformedWeatherDataset,
    WeatherRequest,
)
from pipeline.utils import build_variable_signature, format_request_period

logger = logging.getLogger(__name__)

PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent
DATA_DIRECTORY = PROJECT_DIRECTORY / "data"


def _artifact_timestamp(path: Path) -> datetime:
    """Return artifact modification time as UTC metadata."""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _create_artifact(
    *,
    request: WeatherRequest,
    layer: DataLayer,
    created_at: datetime,
    extension: str,
    data_directory: Path,
) -> DatasetArtifact:
    """Create deterministic artifact metadata for one logical request."""
    period_signature = format_request_period(request.start_date, request.end_date)
    variable_signature = build_variable_signature(request.variables)
    filename = f"{period_signature}_{variable_signature}{extension}"
    path = (
        data_directory
        / request.dataset
        / layer.value
        / request.source.value
        / request.start_date[:4]
        / filename
    )
    return DatasetArtifact(layer=layer, created_at=created_at, path=path)


def raw_artifact_for_request(
    request: WeatherRequest,
    *,
    data_directory: Path | None = None,
) -> DatasetArtifact:
    """Return the deterministic RAW NetCDF artifact for a request."""
    return _create_artifact(
        request=request,
        layer=DataLayer.RAW,
        created_at=datetime.now(UTC),
        extension=".nc",
        data_directory=data_directory or DATA_DIRECTORY,
    )


def transformed_artifact_for_request(
    request: WeatherRequest,
    *,
    data_directory: Path | None = None,
) -> DatasetArtifact:
    """Return the deterministic transformed Parquet artifact for a request."""
    return _create_artifact(
        request=request,
        layer=DataLayer.TRANSFORMED,
        created_at=datetime.now(UTC),
        extension=".parquet",
        data_directory=data_directory or DATA_DIRECTORY,
    )


def artifact_exists(artifact: DatasetArtifact) -> bool:
    """Return True if artifact exists."""
    return artifact.path.exists()


def save_raw_netcdf(dataset: RawWeatherDataset) -> DatasetArtifact:
    """Persist RAW source truth once; RAW artifacts are immutable."""
    nc_artifact = raw_artifact_for_request(dataset.request)
    if artifact_exists(nc_artifact):
        raise FileExistsError(
            f"RAW artifact already exists and is immutable: {nc_artifact.path}"
        )
    nc_artifact.path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing RAW NetCDF checkpoint:\n%s", nc_artifact.path)
    dataset.raw_data.to_netcdf(nc_artifact.path)
    return nc_artifact


def load_raw_netcdf(
    artifact: DatasetArtifact,
    request: WeatherRequest,
) -> RawWeatherDataset:
    """Load an existing RAW NetCDF checkpoint for deterministic resume."""
    if not artifact_exists(artifact):
        raise FileNotFoundError(f"RAW artifact not found: {artifact.path}")
    logger.info("Resuming from RAW NetCDF checkpoint:\n%s", artifact.path)
    with xr.open_dataset(artifact.path) as opened:
        dataset = opened.load()
    return RawWeatherDataset(
        request=request,
        downloaded_at=_artifact_timestamp(artifact.path),
        raw_data=dataset,
    )


def convert_netcdf_to_dataframe(artifact: DatasetArtifact) -> pd.DataFrame:
    """Load a local NetCDF checkpoint into a Pandas DataFrame."""
    logger.info(
        "Loading local NetCDF checkpoint into a Pandas DataFrame:\n%s",
        artifact.path,
    )
    with xr.open_dataset(artifact.path) as local_ds:
        dataframe = local_ds.to_dataframe().reset_index()
    _log_dataframe_preview(dataframe, rows=10)
    return dataframe


def save_transformed_parquet(
    df: pd.DataFrame,
    request: WeatherRequest,
    downloaded_at: datetime,
) -> DatasetArtifact:
    """Persist a transformed checkpoint once for deterministic retries."""
    parquet_artifact = transformed_artifact_for_request(request)
    if artifact_exists(parquet_artifact):
        raise FileExistsError(
            "Transformed artifact already exists; resume it explicitly instead of "
            f"overwriting: {parquet_artifact.path}"
        )
    parquet_artifact = DatasetArtifact(
        layer=parquet_artifact.layer,
        created_at=downloaded_at,
        path=parquet_artifact.path,
    )
    parquet_artifact.path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing transformed Parquet checkpoint:\n%s", parquet_artifact.path)
    df.to_parquet(parquet_artifact.path, index=False)
    return parquet_artifact


def load_transformed_parquet(
    artifact: DatasetArtifact,
    request: WeatherRequest,
) -> TransformedWeatherDataset:
    """Load an existing transformed Parquet checkpoint for resume."""
    if not artifact_exists(artifact):
        raise FileNotFoundError(f"Transformed artifact not found: {artifact.path}")
    logger.info("Resuming from transformed Parquet checkpoint:\n%s", artifact.path)
    dataframe = pd.read_parquet(artifact.path)
    return TransformedWeatherDataset(
        request=request,
        transformed_at=_artifact_timestamp(artifact.path),
        dataset=dataframe,
    )


def load_raw_dataset(
    artifact: DatasetArtifact,
    metadata: RawWeatherDataset,
) -> LoadedWeatherDataset:
    """Backward-compatible helper for legacy Parquet-based RAW callers."""
    logger.info("Loading Parquet artifact:\n%s", artifact.path)
    dataframe = pd.read_parquet(artifact.path)
    return LoadedWeatherDataset(
        request=metadata.request,
        downloaded_at=metadata.downloaded_at,
        raw_data=dataframe,
    )


def delete_artifact(artifact: DatasetArtifact) -> None:
    """Delete artifact if it exists."""
    if artifact.path.exists():
        artifact.path.unlink()


class LocalDataStorage(DataStorage):
    """Local filesystem artifact storage, independent from Airflow."""

    def raw_artifact(self, request: WeatherRequest) -> DatasetArtifact:
        return raw_artifact_for_request(request)

    def transformed_artifact(self, request: WeatherRequest) -> DatasetArtifact:
        return transformed_artifact_for_request(request)

    def save_raw(self, dataset: RawWeatherDataset) -> DatasetArtifact:
        return save_raw_netcdf(dataset)

    def load_raw(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
    ) -> RawWeatherDataset:
        return load_raw_netcdf(artifact, request)

    def save_transformed(
        self,
        df: pd.DataFrame,
        request: WeatherRequest,
        downloaded_at: datetime,
    ) -> DatasetArtifact:
        return save_transformed_parquet(df, request, downloaded_at)

    def load_transformed(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
    ) -> TransformedWeatherDataset:
        return load_transformed_parquet(artifact, request)

    def convert_to_dataframe(self, artifact: DatasetArtifact) -> pd.DataFrame:
        return convert_netcdf_to_dataframe(artifact)
