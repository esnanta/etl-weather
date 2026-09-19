from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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

PARQUET_PROVENANCE_KEYS = (
    "raw_artifact_path",
    "raw_sha256",
    "downloaded_at",
    "transformed_at",
)


def calculate_file_sha256(path: Path) -> str:
    """Return the SHA-256 digest of one artifact file."""
    if not path.exists():
        raise FileNotFoundError(f"Artifact not found for checksum: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_path_for_metadata(path: Path) -> str:
    """Return a stable human-readable artifact path for persisted metadata."""
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_DIRECTORY.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def _datetime_to_metadata(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _datetime_from_metadata(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"Invalid {field} provenance timestamp: {value}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_coordinate_for_path(value: float) -> str:
    """Return a deterministic, filesystem-safe coordinate representation."""
    coordinate = format(Decimal(str(float(value))).normalize(), "f")
    if coordinate == "-0":
        coordinate = "0"
    return coordinate.replace("-", "m").replace(".", "p")


def build_bbox_signature(request: WeatherRequest) -> str:
    """Return a readable spatial identity for request artifact paths."""
    bbox = request.bbox
    return "_".join(
        (
            f"bbox_n{_format_coordinate_for_path(bbox.north_lat)}",
            f"w{_format_coordinate_for_path(bbox.west_lon)}",
            f"s{_format_coordinate_for_path(bbox.south_lat)}",
            f"e{_format_coordinate_for_path(bbox.east_lon)}",
        )
    )


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
    bbox_signature = build_bbox_signature(request)
    filename = f"{period_signature}_{variable_signature}{extension}"
    path = (
        data_directory
        / f"{request.source.value}_{request.dataset}"
        / bbox_signature
        / layer.value
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


def _temporary_path(target: Path) -> Path:
    """Return a same-directory temporary path for atomic replacement."""
    return target.with_name(f".{target.name}.tmp")


def save_raw_netcdf(dataset: RawWeatherDataset) -> DatasetArtifact:
    """Persist RAW source truth once using an atomic same-filesystem write."""
    nc_artifact = raw_artifact_for_request(dataset.request)
    if artifact_exists(nc_artifact):
        raise FileExistsError(
            f"RAW artifact already exists and is immutable: {nc_artifact.path}"
        )

    nc_artifact.path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _temporary_path(nc_artifact.path)
    if temporary_path.exists():
        temporary_path.unlink()

    raw_data = dataset.raw_data.copy(deep=False)
    raw_data.attrs = dict(raw_data.attrs)
    raw_data.attrs["downloaded_at"] = _datetime_to_metadata(dataset.downloaded_at)

    logger.info("Writing RAW NetCDF checkpoint:\n%s", nc_artifact.path)
    try:
        raw_data.to_netcdf(temporary_path)
        temporary_path.replace(nc_artifact.path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
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
        downloaded_at_value = opened.attrs.get("downloaded_at")
        if not downloaded_at_value:
            raise ValueError(
                "RAW checkpoint is missing downloaded_at provenance metadata. "
                "Recreate the RAW checkpoint."
            )
        dataset = opened.load()
    return RawWeatherDataset(
        request=request,
        downloaded_at=_datetime_from_metadata(
            str(downloaded_at_value),
            "downloaded_at",
        ),
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


def _build_parquet_provenance(
    *,
    raw_artifact: DatasetArtifact,
    downloaded_at: datetime,
    transformed_at: datetime,
) -> dict[str, str]:
    return {
        "raw_artifact_path": artifact_path_for_metadata(raw_artifact.path),
        "raw_sha256": calculate_file_sha256(raw_artifact.path),
        "downloaded_at": _datetime_to_metadata(downloaded_at),
        "transformed_at": _datetime_to_metadata(transformed_at),
    }


def read_parquet_provenance(artifact: DatasetArtifact) -> dict[str, str]:
    """Read required P19 provenance metadata from a transformed Parquet file."""
    if not artifact_exists(artifact):
        raise FileNotFoundError(f"Transformed artifact not found: {artifact.path}")

    parquet_metadata = pq.read_metadata(artifact.path).metadata or {}
    decoded = {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in parquet_metadata.items()
    }
    missing = [key for key in PARQUET_PROVENANCE_KEYS if key not in decoded]
    if missing:
        raise ValueError(
            "Transformed checkpoint is missing provenance metadata: "
            + ", ".join(missing)
            + ". Recreate the transformed checkpoint."
        )
    provenance = {key: decoded[key] for key in PARQUET_PROVENANCE_KEYS}
    for timestamp_field in ("downloaded_at", "transformed_at"):
        _datetime_from_metadata(
            provenance[timestamp_field],
            timestamp_field,
        )
    return provenance


def save_transformed_parquet(
    df: pd.DataFrame,
    request: WeatherRequest,
    downloaded_at: datetime,
    transformed_at: datetime,
    raw_artifact: DatasetArtifact,
) -> DatasetArtifact:
    """Persist a transformed checkpoint with RAW provenance metadata."""
    parquet_artifact = transformed_artifact_for_request(request)
    if artifact_exists(parquet_artifact):
        raise FileExistsError(
            "Transformed artifact already exists; resume it explicitly instead of "
            f"overwriting: {parquet_artifact.path}"
        )

    parquet_artifact = DatasetArtifact(
        layer=parquet_artifact.layer,
        created_at=transformed_at,
        path=parquet_artifact.path,
    )
    parquet_artifact.path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _temporary_path(parquet_artifact.path)
    if temporary_path.exists():
        temporary_path.unlink()

    provenance = _build_parquet_provenance(
        raw_artifact=raw_artifact,
        downloaded_at=downloaded_at,
        transformed_at=transformed_at,
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    schema_metadata = dict(table.schema.metadata or {})
    schema_metadata.update(
        {
            key.encode("utf-8"): value.encode("utf-8")
            for key, value in provenance.items()
        }
    )
    table = table.replace_schema_metadata(schema_metadata)

    logger.info("Writing transformed Parquet checkpoint:\n%s", parquet_artifact.path)
    try:
        pq.write_table(table, temporary_path)
        temporary_path.replace(parquet_artifact.path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return parquet_artifact


def load_transformed_parquet(
    artifact: DatasetArtifact,
    request: WeatherRequest,
    raw_artifact: DatasetArtifact,
) -> TransformedWeatherDataset:
    """Load a transformed checkpoint and verify it still matches the RAW artifact."""
    if not artifact_exists(artifact):
        raise FileNotFoundError(f"Transformed artifact not found: {artifact.path}")

    provenance = read_parquet_provenance(artifact)
    expected_raw_path = artifact_path_for_metadata(raw_artifact.path)
    if provenance["raw_artifact_path"] != expected_raw_path:
        raise ValueError(
            "Transformed checkpoint RAW artifact path does not match the current RAW "
            "checkpoint. Recreate the transformed checkpoint."
        )

    current_raw_sha256 = calculate_file_sha256(raw_artifact.path)
    if provenance["raw_sha256"] != current_raw_sha256:
        raise ValueError(
            "RAW artifact checksum changed since the transformed checkpoint was "
            "created. Recreate the transformed checkpoint before loading."
        )

    logger.info("Resuming from transformed Parquet checkpoint:\n%s", artifact.path)
    dataframe = pd.read_parquet(artifact.path)
    return TransformedWeatherDataset(
        request=request,
        transformed_at=_datetime_from_metadata(
            provenance["transformed_at"],
            "transformed_at",
        ),
        dataset=dataframe,
    )


def load_raw_dataset(
    artifact: DatasetArtifact,
    metadata: RawWeatherDataset,
) -> LoadedWeatherDataset:
    """Load a Parquet artifact into a Pandas DataFrame."""
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
        transformed_at: datetime,
        raw_artifact: DatasetArtifact,
    ) -> DatasetArtifact:
        return save_transformed_parquet(
            df,
            request,
            downloaded_at,
            transformed_at,
            raw_artifact,
        )

    def load_transformed(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
        raw_artifact: DatasetArtifact,
    ) -> TransformedWeatherDataset:
        return load_transformed_parquet(artifact, request, raw_artifact)

    def convert_to_dataframe(self, artifact: DatasetArtifact) -> pd.DataFrame:
        return convert_netcdf_to_dataframe(artifact)
