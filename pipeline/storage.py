from __future__ import annotations

from datetime import datetime
from pathlib import Path

import logging

import pandas as pd
import xarray as xr

from pipeline.model import (
    RawWeatherDataset,
    WeatherRequest, LoadedWeatherDataset, DatasetArtifact, DataLayer,
)
from pipeline.interfaces import DataStorage

from pipeline.logging import (
    log_step,
    _log_dataframe_preview,
)

from pipeline.utils import (
    format_request_date,
    build_variable_signature,
)

logger = logging.getLogger(__name__)


PROJECT_DIRECTORY = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

DATA_DIRECTORY = (
    PROJECT_DIRECTORY
    / "data"
)

@log_step
def save_raw_netcdf(
    dataset: RawWeatherDataset,
) -> DatasetArtifact:
    """
    Persist the raw lazy xarray.Dataset as a NetCDF checkpoint.

    This operation materializes the selected subset from the
    ARCO Zarr store and writes it to a local NetCDF file.
    The resulting NetCDF file is retained as the RAW layer
    artifact and serves as the source for subsequent
    DataFrame conversion and downstream processing.

    Returns
    -------
    DatasetArtifact
        Metadata describing the saved NetCDF artifact.
    """

    nc_artifact = _create_artifact(
        request=dataset.request,
        layer=DataLayer.RAW,
        created_at=dataset.downloaded_at,
        extension=".nc",
        data_directory=DATA_DIRECTORY,
    )

    nc_artifact.path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger.info(
        "Writing RAW NetCDF checkpoint: %s",
        nc_artifact.path,
    )

    dataset.raw_data.to_netcdf(
        nc_artifact.path,
    )

    return nc_artifact


@log_step
def convert_netcdf_to_dataframe(
    artifact: DatasetArtifact,
) -> pd.DataFrame:
    """
    Load a local NetCDF checkpoint into a Pandas DataFrame.
    """
    logger.info(f"Membaca lokal NetCDF dari {artifact.path} ke Pandas DataFrame.")
    local_ds = xr.open_dataset(artifact.path)
    df = local_ds.to_dataframe().reset_index()
    local_ds.close()

    _log_dataframe_preview(df, rows=10)

    return df


@log_step
def save_transformed_parquet(
        df: pd.DataFrame,
        request: 'WeatherRequest',
        downloaded_at: datetime
) -> DatasetArtifact:
    """
    [SRP] Tanggung jawab tunggal: Menyimpan DataFrame ke format .parquet.
    Layer: TRANSFORMED
    """
    parquet_artifact = _create_artifact(
        request=request,
        layer=DataLayer.TRANSFORMED,
        created_at=downloaded_at,
        extension=".parquet",
        data_directory=DATA_DIRECTORY,
    )
    parquet_artifact.path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Menulis Parquet checkpoint (Transformed) ke: {parquet_artifact.path}")
    df.to_parquet(parquet_artifact.path, index=False)

    return parquet_artifact


@log_step
def load_raw_dataset(
        artifact: DatasetArtifact,
        metadata: RawWeatherDataset,
) -> LoadedWeatherDataset:
    """
    Load a raw Parquet artifact into a Pandas DataFrame.
    """
    logger.info(f"Loading Parquet from {artifact.path}")
    df = pd.read_parquet(artifact.path)

    return LoadedWeatherDataset(
        request=metadata.request,
        downloaded_at=metadata.downloaded_at,
        raw_data=df,
    )


def artifact_exists(
    artifact: DatasetArtifact,
) -> bool:
    """Return True if artifact exists."""

    return artifact.path.exists()


def delete_artifact(
    artifact: DatasetArtifact,
) -> None:
    """Delete artifact if it exists."""



    if artifact.path.exists():
        artifact.path.unlink()


def _create_artifact(
    *,
    request: WeatherRequest,
    layer: DataLayer,
    created_at: datetime,
    extension: str,
    data_directory: Path,
) -> DatasetArtifact:
    """
    Create artifact metadata.
    """

    start_date = format_request_date(
        request.start_date,
    )

    end_date = format_request_date(
        request.end_date,
    )

    variable_signature = (
        build_variable_signature(
            request.variables,
        )
    )

    filename = (
        f"{start_date}"
        f"_{end_date}"
        f"_{variable_signature}"
        f"{extension}"
    )

    path = (
        data_directory
        / request.dataset
        / layer.value
        / request.source.value
        / start_date[:4]
        / filename
    )

    return DatasetArtifact(
        layer=layer,
        created_at=created_at,
        path=path,
    )


class LocalDataStorage(DataStorage):
    """
    SOLID: Single Responsibility & Dependency Inversion.
    Handles artifact storage and retrieval on local filesystem.
    """
    def save_raw(self, dataset: RawWeatherDataset) -> DatasetArtifact:
        return save_raw_netcdf(dataset)

    def save_transformed(self, df: pd.DataFrame, request: WeatherRequest, downloaded_at: datetime) -> DatasetArtifact:
        return save_transformed_parquet(df, request, downloaded_at)

    def convert_to_dataframe(self, artifact: DatasetArtifact) -> pd.DataFrame:
        return convert_netcdf_to_dataframe(artifact)
