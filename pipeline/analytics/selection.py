"""Select and validate existing ERA5-Land monthly sources for analytics export.

This module only reads checkpoints. It neither writes analytics artifacts nor
downloads data. Period completeness is validated within each available source month. A source-\ndriven discovery mode preserves gaps without fabricating missing months.
"""

from __future__ import annotations

import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import xarray as xr

from config import ACEH_BBOX_COORDINATES, CDS_DATASET, CDS_VARIABLES
from pipeline.models.weather import BoundingBox, DataSource, WeatherRequest
from pipeline.requests.weather import build_variable_signature
from pipeline.storage.artifacts import DATA_DIRECTORY, build_bbox_signature

_SOURCE_NAME = re.compile(
    r"^(?P<year>\d{4})_(?P<start>\d{4})_(?P<end>\d{4})_(?P<variables>[a-z0-9_]+)$"
)
_RAW_KEYS = ("valid_time", "latitude", "longitude")
_DAILY_KEYS = ("valid_time", "valid_time_local", "latitude", "longitude")
_RESOLUTIONS = {"hourly": ("raw", ".nc"), "daily": ("transformed", ".parquet")}


@dataclass(frozen=True)
class SourceMonth:
    """An existing immutable source checkpoint for one target month."""

    path: Path
    start: date
    end: date


@dataclass(frozen=True)
class AnalyticsSelection:
    """Validated source months and exact temporal coverage for a future export."""

    resolution: str
    variables: tuple[str, ...]
    start: date
    end: date
    months: tuple[SourceMonth, ...]


def normalize_variables(selection: str) -> tuple[str, ...]:
    """Normalize selected provider codes without adding derived daily fields."""
    active = set(CDS_VARIABLES)
    if selection.strip().lower() == "all":
        return tuple(sorted(active))
    values = [part.strip().lower() for part in selection.split(",")]
    if not values or any(not value for value in values):
        raise ValueError("Variables must be 'all' or comma-separated provider codes.")
    if len(values) != len(set(values)):
        raise ValueError("Duplicate variables are not allowed.")
    unsupported = set(values) - active
    if unsupported:
        raise ValueError(f"Unsupported provider variables: {', '.join(sorted(unsupported))}")
    return tuple(sorted(values))


def _dataset_root(data_directory: Path) -> Path:
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset=CDS_DATASET,
        start_date="2024-01-01",
        end_date="2024-01-31",
        variables=list(CDS_VARIABLES),
        bbox=BoundingBox(**ACEH_BBOX_COORDINATES),
    )
    return (
        data_directory
        / f"{request.source.value}_{request.dataset}"
        / build_bbox_signature(request)
    )


def _months(start: date, end: date):
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        first = date(year, month, 1)
        last = date(year, month, monthrange(year, month)[1])
        yield max(first, start), min(last, end), first, last
        month += 1
        if month == 13:
            year, month = year + 1, 1


def _source_variables(path: Path, *, first: date, last: date) -> set[str] | None:
    match = _SOURCE_NAME.fullmatch(path.stem)
    if match is None or match["year"] != f"{first.year:04d}":
        return None
    if match["start"] != first.strftime("%m%d") or match["end"] != last.strftime("%m%d"):
        return None
    signature = match["variables"]
    variables = signature.split("_")
    if len(set(variables)) != len(variables) or build_variable_signature(variables) != signature:
        return None
    if not set(variables).issubset(CDS_VARIABLES):
        return None
    return set(variables)


def discover_sources(
    resolution: str,
    variables: str,
    start: date,
    end: date,
    *,
    data_directory: Path = DATA_DIRECTORY,
) -> AnalyticsSelection:
    """Find one unambiguous pre-existing monthly file for every selected month.

    Monthly source partitions must cover entire calendar months. Callers may
    select part of a month, but the source checkpoint remains month-owned.
    """
    if resolution not in _RESOLUTIONS:
        raise ValueError("Resolution must be 'hourly' or 'daily'.")
    if start > end:
        raise ValueError("Start date must not be after end date.")
    requested = normalize_variables(variables)
    layer, extension = _RESOLUTIONS[resolution]
    root = _dataset_root(data_directory) / layer
    selected: list[SourceMonth] = []

    for _, _, first, last in _months(start, end):
        directory = root / str(first.year)
        candidates = []
        for path in sorted(directory.glob(f"*{extension}")):
            available = _source_variables(path, first=first, last=last)
            if available is not None and set(requested).issubset(available):
                candidates.append(path)
        if not candidates:
            raise FileNotFoundError(
                f"No {resolution} checkpoint containing {','.join(requested)} "
                f"for {first:%Y-%m} under {directory}"
            )
        if len(candidates) > 1:
            raise ValueError(
                f"Ambiguous {resolution} source for {first:%Y-%m}: "
                + ", ".join(str(path) for path in candidates)
            )
        selected.append(SourceMonth(candidates[0], first, last))

    return AnalyticsSelection(resolution, requested, start, end, tuple(selected))



def discover_available_sources(
    resolution: str,
    variables: str,
    *,
    data_directory: Path = DATA_DIRECTORY,
) -> tuple[AnalyticsSelection, ...]:
    """Discover all existing checkpoint months, grouped by calendar year.

    Missing calendar months are allowed, but each month with a source must
    have exactly one compatible checkpoint. A historical smaller-variable
    file cannot silently satisfy an 'all' request for nine variables.
    """
    if resolution not in _RESOLUTIONS:
        raise ValueError("Resolution must be 'hourly' or 'daily'.")
    requested = normalize_variables(variables)
    layer, extension = _RESOLUTIONS[resolution]
    root = _dataset_root(data_directory) / layer
    candidates: dict[tuple[int, int], list[Path]] = {}
    available_months: set[tuple[int, int]] = set()

    for path in sorted(root.glob(f"*/*{extension}")):
        if not path.parent.name.isdecimal() or len(path.parent.name) != 4:
            raise ValueError(f"Unexpected {resolution} source directory: {path}")
        match = _SOURCE_NAME.fullmatch(path.stem)
        if match is None:
            raise ValueError(f"Unrecognized {resolution} checkpoint name: {path}")
        year = int(path.parent.name)
        if match["year"] != str(year):
            raise ValueError(f"Source year directory mismatch: {path}")
        try:
            month = int(match["start"][:2])
            first = date(year, month, 1)
            last = date(year, month, monthrange(year, month)[1])
        except ValueError as error:
            raise ValueError(f"Invalid source month: {path}") from error
        available = _source_variables(path, first=first, last=last)
        if available is None:
            raise ValueError(f"Invalid full-month source identity: {path}")
        key = (year, month)
        available_months.add(key)
        if set(requested).issubset(available):
            candidates.setdefault(key, []).append(path)

    if not available_months:
        raise FileNotFoundError(f"No {resolution} checkpoint files found under {root}")

    by_year: dict[int, list[SourceMonth]] = {}
    for year, month in sorted(available_months):
        paths = candidates.get((year, month), [])
        if not paths:
            raise ValueError(
                f"No {resolution} checkpoint containing {','.join(requested)} "
                f"for {year:04d}-{month:02d}; source exists with fewer variables"
            )
        if len(paths) != 1:
            raise ValueError(
                f"Ambiguous {resolution} source for {year:04d}-{month:02d}: "
                + ", ".join(str(path) for path in paths)
            )
        first = date(year, month, 1)
        last = date(year, month, monthrange(year, month)[1])
        by_year.setdefault(year, []).append(SourceMonth(paths[0], first, last))

    return tuple(
        AnalyticsSelection(
            resolution, requested, months[0].start, months[-1].end, tuple(months)
        )
        for year, months in sorted(by_year.items())
    )

def _check_axis(values: np.ndarray, name: str, expected: int) -> None:
    if len(values) != expected or len(np.unique(values)) != expected:
        raise ValueError(f"Incompatible {name} grid axis.")
    if not np.issubdtype(values.dtype, np.number):
        raise ValueError(f"{name} coordinate must be numeric.")
    differences = np.diff(np.asarray(values, dtype=float))
    if not (np.all(differences > 0) or np.all(differences < 0)):
        raise ValueError(f"{name} grid axis must be monotonic.")
    if not np.allclose(np.abs(differences), 0.1, rtol=0, atol=1e-6):
        raise ValueError(f"{name} grid axis must use 0.1 degree spacing.")


def _validate_raw_month(month: SourceMonth, variables: tuple[str, ...]) -> None:
    with xr.open_dataset(month.path) as ds:
        required = set(_RAW_KEYS) | set(variables)
        missing = required - (set(ds.coords) | set(ds.data_vars))
        if missing:
            raise ValueError(f"{month.path}: missing RAW fields {sorted(missing)}")
        for variable in variables:
            if set(ds[variable].dims) != set(_RAW_KEYS):
                raise ValueError(f"{month.path}: invalid dimensions for {variable}")
        _check_axis(ds.latitude.values, "latitude", 43)
        _check_axis(ds.longitude.values, "longitude", 35)
        if not np.allclose(
            [float(ds.latitude.min()), float(ds.latitude.max()),
             float(ds.longitude.min()), float(ds.longitude.max())],
            [ACEH_BBOX_COORDINATES["south_lat"], ACEH_BBOX_COORDINATES["north_lat"],
             ACEH_BBOX_COORDINATES["west_lon"], ACEH_BBOX_COORDINATES["east_lon"]],
            rtol=0, atol=1e-6,
        ):
            raise ValueError(f"{month.path}: mismatched bounding box")
        times = pd.DatetimeIndex(ds.valid_time.values)
        if times.empty or times.has_duplicates or not times.is_monotonic_increasing:
            raise ValueError(f"{month.path}: invalid RAW hourly timestamps")
        if len(times) > 1 and not (times[1:] - times[:-1] == pd.Timedelta(hours=1)).all():
            raise ValueError(f"{month.path}: missing RAW hourly steps")
        target = times[(times >= pd.Timestamp(month.start)) &
                       (times < pd.Timestamp(month.end) + pd.Timedelta(days=1))]
        expected = (month.end - month.start).days * 24 + 24
        if len(target) != expected:
            raise ValueError(f"{month.path}: incomplete target-month UTC hours")
        if times[0] < pd.Timestamp(month.start) - pd.Timedelta(days=1) or times[-1] > pd.Timestamp(month.end) + pd.Timedelta(hours=23):
            raise ValueError(f"{month.path}: unexpected RAW context timestamps")


def _validate_daily_month(month: SourceMonth, variables: tuple[str, ...]) -> None:
    required = set(_DAILY_KEYS) | set(variables)
    with pq.ParquetFile(month.path) as file:
        missing = required - set(file.schema_arrow.names)
        if missing:
            raise ValueError(f"{month.path}: missing daily columns {sorted(missing)}")
        # Only time/grid identity columns are needed; avoid materializing weather data.
        rows = file.read(columns=list(_DAILY_KEYS)).to_pandas()
    if rows.empty or rows[list(_DAILY_KEYS)].isna().any().any():
        raise ValueError(f"{month.path}: empty or null daily identities")
    local = pd.to_datetime(rows.valid_time_local)
    utc = pd.to_datetime(rows.valid_time)
    if not ((local - utc) == pd.Timedelta(hours=7)).all():
        raise ValueError(f"{month.path}: invalid UTC+7 offset")
    if not (local.dt.hour.eq(0) & local.dt.minute.eq(0) & local.dt.second.eq(0)).all():
        raise ValueError(f"{month.path}: local timestamps must be midnight")
    if rows.duplicated(["valid_time_local", "latitude", "longitude"]).any():
        raise ValueError(f"{month.path}: duplicate local-day/grid identities")
    days = pd.DatetimeIndex(local.drop_duplicates().sort_values())
    expected = pd.date_range(month.start, month.end, freq="D")
    if not days.equals(expected):
        raise ValueError(f"{month.path}: incomplete or unexpected local days")
    if not rows.groupby("valid_time_local").size().nunique() == 1:
        raise ValueError(f"{month.path}: inconsistent daily grid counts")
    grid = None
    for _, frame in rows.groupby("valid_time_local"):
        current = set(zip(frame.latitude, frame.longitude))
        if grid is None:
            grid = current
        elif current != grid:
            raise ValueError(f"{month.path}: inconsistent daily grid membership")
    if not (rows.latitude.between(ACEH_BBOX_COORDINATES["south_lat"]-1e-6, ACEH_BBOX_COORDINATES["north_lat"]+1e-6).all()
            and rows.longitude.between(ACEH_BBOX_COORDINATES["west_lon"]-1e-6, ACEH_BBOX_COORDINATES["east_lon"]+1e-6).all()):
        raise ValueError(f"{month.path}: daily grid outside bounding box")


def _validate_raw_overlaps(months: tuple[SourceMonth, ...], variables: tuple[str, ...]) -> None:
    for previous, current in zip(months, months[1:]):
        with xr.open_dataset(previous.path) as left, xr.open_dataset(current.path) as right:
            common = np.intersect1d(left.valid_time.values, right.valid_time.values)
            if not len(common):
                continue
            if not (np.array_equal(left.latitude.values, right.latitude.values)
                    and np.array_equal(left.longitude.values, right.longitude.values)):
                raise ValueError(f"Conflicting overlap grids: {previous.path}, {current.path}")
            for variable in variables:
                a = left[variable].sel(valid_time=common).transpose(*_RAW_KEYS).values
                b = right[variable].sel(valid_time=common).transpose(*_RAW_KEYS).values
                if not np.array_equal(a, b, equal_nan=True):
                    raise ValueError(
                        f"Conflicting RAW overlap {variable}: {previous.path}, {current.path}"
                    )


def validate_sources(selection: AnalyticsSelection) -> None:
    """Validate every selected month without mutating its source checkpoint."""
    for month in selection.months:
        if selection.resolution == "hourly":
            _validate_raw_month(month, selection.variables)
        else:
            _validate_daily_month(month, selection.variables)
    if selection.resolution == "hourly":
        _validate_raw_overlaps(selection.months, selection.variables)
