"""Stream validated ERA5-Land checkpoints to independent annual Parquet files.

This module never changes source checkpoints. Partial-year selections are
isolated under a period-scoped directory. P4 verifies existing P3 exports
before adopting them into provenance manifests; outputs are never overwritten.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import xarray as xr

from config import CDS_VARIABLES
from pipeline.analytics.selection import AnalyticsSelection, SourceMonth
from pipeline.requests.weather import build_variable_signature
from pipeline.storage.artifacts import DATA_DIRECTORY, build_bbox_signature
from pipeline.models.weather import BoundingBox, DataSource, WeatherRequest
from config import ACEH_BBOX_COORDINATES, CDS_DATASET

_RAW_KEYS = ("valid_time", "latitude", "longitude")
_DAILY_KEYS = ("valid_time", "valid_time_local", "latitude", "longitude")
_BATCH_SIZE = 32_768
_HOURLY_STEPS_PER_BATCH = 24


def _dataset_directory(selection: AnalyticsSelection, data_directory: Path) -> Path:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset=CDS_DATASET,
        start_date=selection.start.isoformat(),
        end_date=selection.end.isoformat(),
        variables=list(selection.variables),
        bbox=BoundingBox(**ACEH_BBOX_COORDINATES),
    )
    signature = (
        "all" if set(selection.variables) == set(CDS_VARIABLES)
        else build_variable_signature(list(selection.variables))
    )
    return (
        data_directory / f"{request.source.value}_{request.dataset}"
        / build_bbox_signature(request) / "analytics"
        / signature / selection.resolution
    )


def _full_year(selection: AnalyticsSelection, year: int) -> bool:
    """Return whether the selection covers the entire target calendar year."""
    return (
        selection.start <= date(year, 1, 1)
        and selection.end >= date(year, 12, 31)
    )


def output_paths(
    selection: AnalyticsSelection,
    *,
    data_directory: Path = DATA_DIRECTORY,
    output_directory: Path | None = None,
) -> tuple[Path, ...]:
    """Return deterministic destinations with partial-year pilots isolated."""
    root = output_directory or _dataset_directory(selection, data_directory)
    years = range(selection.start.year, selection.end.year + 1)
    targets: list[Path] = []
    for year in years:
        if _full_year(selection, year):
            targets.append(root / f"{year}.parquet")
        else:
            first = max(selection.start, date(year, 1, 1))
            last = min(selection.end, date(year, 12, 31))
            pilot = root / f"pilot_{first:%Y%m%d}_{last:%Y%m%d}"
            targets.append(pilot / f"{year}.parquet")
    return tuple(targets)


def _target_limits(selection: AnalyticsSelection, year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    first = max(selection.start, date(year, 1, 1))
    last = min(selection.end, date(year, 12, 31))
    return pd.Timestamp(first), pd.Timestamp(last + timedelta(days=1))


def _hourly_tables(
    month: SourceMonth,
    variables: tuple[str, ...],
    lower: pd.Timestamp,
    upper: pd.Timestamp,
) -> Iterator[pa.Table]:
    """Iterate selected target-hour chunks, never loading full RAW checkpoints."""
    columns = [*_RAW_KEYS, *variables]
    month_lower = max(lower, pd.Timestamp(month.start))
    month_upper = min(upper, pd.Timestamp(month.end + timedelta(days=1)))
    with xr.open_dataset(month.path) as ds:
        timestamps = pd.DatetimeIndex(ds.valid_time.values)
        indices = [
            i for i, stamp in enumerate(timestamps)
            if month_lower <= stamp < month_upper
        ]
        for begin in range(0, len(indices), _HOURLY_STEPS_PER_BATCH):
            position = indices[begin:begin + _HOURLY_STEPS_PER_BATCH]
            window = ds[list(variables)].isel(valid_time=position)
            frame = window.to_dataframe().reset_index()
            yield pa.Table.from_pandas(frame.loc[:, columns], preserve_index=False)


def _daily_tables(
    month: SourceMonth,
    variables: tuple[str, ...],
    lower: pd.Timestamp,
    upper: pd.Timestamp,
) -> Iterator[pa.Table]:
    """Read only selected daily Parquet columns in bounded Arrow batches."""
    columns = [*_DAILY_KEYS, *variables]
    with pq.ParquetFile(month.path) as source:
        for record_batch in source.iter_batches(batch_size=_BATCH_SIZE, columns=columns):
            table = pa.Table.from_batches([record_batch])
            clock = table.column("valid_time_local")
            mask = pc.and_(
                pc.greater_equal(clock, pa.scalar(lower.to_pydatetime(), type=clock.type)),
                pc.less(clock, pa.scalar(upper.to_pydatetime(), type=clock.type)),
            )
            selected = table.filter(mask)
            if selected.num_rows:
                yield selected


def _source_units(selection: AnalyticsSelection, months: tuple[SourceMonth, ...]) -> dict[str, str]:
    """Preserve units provided by hourly RAW metadata without guessing values."""
    if selection.resolution != "hourly":
        return {}
    units: dict[str, str] = {}
    for month in months:
        with xr.open_dataset(month.path) as ds:
            for variable in selection.variables:
                value = ds[variable].attrs.get("units")
                if value is None:
                    continue
                if variable in units and units[variable] != str(value):
                    raise ValueError(f"Conflicting source units for {variable}")
                units[variable] = str(value)
    return units


def _write_year(
    selection: AnalyticsSelection, year: int, destination: Path,
) -> int:
    """Stage a year on the same filesystem and publish without overwriting."""
    lower, upper = _target_limits(selection, year)
    months = tuple(
        month for month in selection.months
        if month.start < upper.date() and month.end >= lower.date()
    )
    units = _source_units(selection, months)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    rows = 0

    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{destination.stem}.", suffix=".tmp",
            dir=destination.parent, delete=False,
        ) as handle:
            temporary = Path(handle.name)

        for month in months:
            if selection.resolution == "hourly":
                tables = _hourly_tables(month, selection.variables, lower, upper)
            else:
                tables = _daily_tables(month, selection.variables, lower, upper)
            for table in tables:
                if not table.num_rows:
                    continue
                if writer is None:
                    metadata = {
                        b"analytics_resolution": selection.resolution.encode(),
                        b"analytics_target_start": max(selection.start, date(year, 1, 1)).isoformat().encode(),
                        b"analytics_target_end": min(selection.end, date(year, 12, 31)).isoformat().encode(),
                        b"analytics_source_units": json.dumps(units, sort_keys=True).encode(),
                        b"analytics_coverage": (
                            b"complete_year" if _full_year(selection, year) else b"partial_year"
                        ),
                    }
                    schema = table.schema.with_metadata(metadata)
                    writer = pq.ParquetWriter(temporary, schema=schema, compression="zstd")
                assert schema is not None
                if not table.schema.equals(schema, check_metadata=False):
                    raise ValueError(f"Incompatible source Parquet schema for {month.path}")
                writer.write_table(table.cast(schema))
                rows += table.num_rows

        if writer is None or rows == 0:
            raise ValueError(f"No rows available for target year {year}")
        writer.close()
        writer = None

        with pq.ParquetFile(temporary) as written:
            if written.metadata.num_rows != rows:
                raise ValueError(f"Staged Parquet row count mismatch for {year}")
        # Exclusive same-filesystem publication: concurrent writers cannot
        # silently overwrite a previously completed file.
        os.link(temporary, destination)
        return rows
    finally:
        if writer is not None:
            writer.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def export_selection(
    selection: AnalyticsSelection,
    *,
    data_directory: Path = DATA_DIRECTORY,
    output_directory: Path | None = None,
) -> tuple[tuple[Path, int], ...]:
    """Write missing partitions or verify/adopt unchanged existing partitions.

    The caller must run validate_sources() before this boundary. A changed
    existing output is rejected; there is deliberately no unsafe --force path.
    Per-year manifests are published only after complete source/output checks.
    """
    from pipeline.analytics.verification import (
        _read_manifest,
        _verify_manifest_inventory,
        publish_manifest,
        verify_output,
    )

    destinations = output_paths(
        selection, data_directory=data_directory, output_directory=output_directory
    )
    years = tuple(range(selection.start.year, selection.end.year + 1))

    # Preflight all existing artifacts and manifests before any new writes.
    # This avoids adding a new partition to a dataset with a corrupt older one.
    for path in destinations:
        if path.exists() and not path.is_file():
            raise ValueError(f"Analytics output is not a regular file: {path}")
        if path.exists():
            verify_output(selection, int(path.stem), path)
        manifest_path = path.parent / "manifest.json"
        if manifest_path.exists():
            manifest = _read_manifest(manifest_path, selection)
            _verify_manifest_inventory(path.parent, manifest)

    results: list[tuple[Path, int]] = []
    for year, path in zip(years, destinations):
        if path.exists():
            rows = verify_output(selection, year, path)
        else:
            _write_year(selection, year, path)
            rows = verify_output(selection, year, path)
        publish_manifest(selection, year, path, rows)
        results.append((path, rows))
    return tuple(results)
