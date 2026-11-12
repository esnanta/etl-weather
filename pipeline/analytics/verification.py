"""Verify exported Parquet against source rows and publish traceable manifests.

Existing P3 artifacts may be adopted only after a complete streaming
source-to-output comparison. Manifest writes are atomic per JSON file; source
checkpoints and previously published Parquet files remain immutable.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import ACEH_BBOX_COORDINATES, CDS_DATASET
from pipeline.contracts.era5_land import ERA5_LAND_OUTPUT_UNITS
from pipeline.analytics.selection import AnalyticsSelection
from pipeline.storage.artifacts import artifact_path_for_metadata, calculate_file_sha256

_SCHEMA_VERSION = 1
_COMPARE_BATCH_SIZE = 32_768


def _period(selection: AnalyticsSelection, year: int) -> tuple[date, date]:
    return max(selection.start, date(year, 1, 1)), min(
        selection.end, date(year, 12, 31)
    )


def _sources_for_year(selection: AnalyticsSelection, year: int):
    first, last = _period(selection, year)
    return tuple(
        month for month in selection.months
        if month.start <= last and month.end >= first
    )


def _source_tables(selection: AnalyticsSelection, year: int):
    # Import here to avoid an import cycle with the writer.
    from pipeline.analytics.export import _daily_tables, _hourly_tables

    first, last = _period(selection, year)
    lower = pd.Timestamp(first)
    upper = pd.Timestamp(last + timedelta(days=1))
    for month in _sources_for_year(selection, year):
        if selection.resolution == "hourly":
            yield from _hourly_tables(month, selection.variables, lower, upper)
        else:
            yield from _daily_tables(month, selection.variables, lower, upper)


def verify_output(
    selection: AnalyticsSelection, year: int, path: Path,
) -> int:
    """Stream-compare every exported field, identity and null location.

    This intentionally compares actual records, not only row counts or digests.
    It also verifies P3 coverage metadata so incomplete years cannot be
    mistakenly certified as complete.
    """
    from pipeline.analytics.export import _full_year

    first, last = _period(selection, year)
    if not path.is_file():
        raise FileNotFoundError(f"Analytics output missing: {path}")
    with pq.ParquetFile(path) as output:
        metadata = output.schema_arrow.metadata or {}
        expected = {
            b"analytics_resolution": selection.resolution.encode(),
            b"analytics_target_start": first.isoformat().encode(),
            b"analytics_target_end": last.isoformat().encode(),
            b"analytics_coverage": (
                b"complete_year" if _full_year(selection, year) else b"partial_year"
            ),
        }
        for key, value in expected.items():
            if metadata.get(key) != value:
                raise ValueError(f"{path}: incompatible analytics metadata {key!r}")

        batches = iter(output.iter_batches(batch_size=_COMPARE_BATCH_SIZE))
        remainder: pa.Table | None = None
        rows = 0
        for expected_table in _source_tables(selection, year):
            pending = expected_table
            while pending.num_rows:
                if remainder is None or not remainder.num_rows:
                    batch = next(batches, None)
                    if batch is None:
                        raise ValueError(f"{path}: output is shorter than source")
                    remainder = pa.Table.from_batches([batch])
                size = min(pending.num_rows, remainder.num_rows)
                actual = remainder.slice(0, size)
                source = pending.slice(0, size)
                if not actual.equals(source, check_metadata=False):
                    raise ValueError(
                        f"{path}: source/output value, timestamp, grid, "
                        "schema or missing-value mismatch"
                    )
                rows += size
                pending = pending.slice(size)
                remainder = remainder.slice(size)
        if (remainder is not None and remainder.num_rows) or next(batches, None) is not None:
            raise ValueError(f"{path}: output contains extra rows")
        if rows == 0 or output.metadata.num_rows != rows:
            raise ValueError(f"{path}: incorrect output row count")
        return rows


def _manifest_identity(selection: AnalyticsSelection) -> dict[str, object]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "provider": "cds",
        "dataset": CDS_DATASET,
        "bbox": dict(ACEH_BBOX_COORDINATES),
        "resolution": selection.resolution,
        "timezone": "UTC" if selection.resolution == "hourly" else "Asia/Jakarta",
        "variables": list(selection.variables),
        "treatment": (
            "raw_preserving" if selection.resolution == "hourly"
            else "upstream_daily_values_unchanged"
        ),
    }


def _source_inventory(selection: AnalyticsSelection, year: int) -> list[dict[str, str]]:
    return [
        {
            "path": artifact_path_for_metadata(month.path),
            "sha256": calculate_file_sha256(month.path),
        }
        for month in _sources_for_year(selection, year)
    ]


def _read_manifest(path: Path, selection: AnalyticsSelection) -> dict[str, object]:
    identity = _manifest_identity(selection)
    if not path.exists():
        return {**identity, "partitions": {}}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"Invalid analytics manifest: {path}") from error
    if not isinstance(document, dict) or any(
        document.get(key) != value for key, value in identity.items()
    ):
        raise ValueError(f"Analytics manifest identity mismatch: {path}")
    if not isinstance(document.get("partitions"), dict):
        raise ValueError(f"Analytics manifest has invalid partitions: {path}")
    return document


def _verify_manifest_inventory(directory: Path, manifest: dict[str, object]) -> None:
    """Never update a manifest that already names missing or modified outputs."""
    entries = manifest["partitions"]
    assert isinstance(entries, dict)
    for filename, entry in entries.items():
        if not isinstance(filename, str) or Path(filename).name != filename:
            raise ValueError(f"Invalid manifest partition filename: {filename}")
        if not isinstance(entry, dict) or not isinstance(entry.get("sha256"), str):
            raise ValueError(f"Invalid manifest partition record: {filename}")
        path = directory / filename
        if not path.is_file() or calculate_file_sha256(path) != entry["sha256"]:
            raise ValueError(f"Analytics manifest references missing/changed file: {path}")


def _entry(
    selection: AnalyticsSelection, year: int, path: Path, rows: int,
) -> dict[str, object]:
    first, last = _period(selection, year)
    with pq.ParquetFile(path) as reader:
        metadata = reader.schema_arrow.metadata or {}
        units = json.loads(metadata.get(b"analytics_source_units", b"{}"))
        columns = list(reader.schema_arrow.names)
    return {
        "year": year,
        "target_start": first.isoformat(),
        "target_end": last.isoformat(),
        "coverage": (
            "complete_year" if first == date(year, 1, 1)
            and last == date(year, 12, 31) else "partial_year"
        ),
        "rows": rows,
        "sha256": calculate_file_sha256(path),
        "time_columns": (
            ["valid_time"] if selection.resolution == "hourly"
            else ["valid_time", "valid_time_local"]
        ),
        "coordinate_columns": ["latitude", "longitude"],
        "columns": columns,
        "units": (
            units if selection.resolution == "hourly"
            else {key: ERA5_LAND_OUTPUT_UNITS[key] for key in selection.variables}
        ),
        "sources": _source_inventory(selection, year),
    }


def publish_manifest(
    selection: AnalyticsSelection, year: int, path: Path, rows: int,
) -> str:
    """Adopt or register verified immutable Parquet using atomic JSON update.

    The partition record is not published until all source digests and output
    bytes match the expected state. A failed write leaves the previous manifest.
    """
    manifest_path = path.parent / "manifest.json"
    manifest = _read_manifest(manifest_path, selection)
    _verify_manifest_inventory(path.parent, manifest)
    entry = _entry(selection, year, path, rows)
    entries = manifest["partitions"]
    assert isinstance(entries, dict)
    old = entries.get(path.name)
    if old is not None:
        if old != entry:
            raise ValueError(
                f"Existing analytics manifest entry changed: {manifest_path} / {path.name}"
            )
        return "reused"

    entries[path.name] = entry
    manifest["generated_at_utc"] = datetime.now(UTC).isoformat()

    # Same-directory temp + atomic replacement; never partially write JSON.
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", prefix=".manifest.",
            suffix=".tmp", dir=path.parent, delete=False,
        ) as file:
            temporary = Path(file.name)
            json.dump(manifest, file, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, manifest_path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return "adopted" if old is None else "reused"
