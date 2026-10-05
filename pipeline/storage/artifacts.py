from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pipeline.models.weather import DataLayer, DatasetArtifact, WeatherRequest
from pipeline.requests.weather import build_variable_signature, format_request_period

PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent.parent
DATA_DIRECTORY = PROJECT_DIRECTORY / "data"


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


def delete_artifact(artifact: DatasetArtifact) -> None:
    """Delete artifact if it exists."""
    if artifact.path.exists():
        artifact.path.unlink()
