"""Define weather requests, datasets, and deterministic artifact metadata."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import xarray as xr


class DataLayer(Enum):
    RAW = "raw"
    TRANSFORMED = "transformed"


class DataSource(Enum):
    CDS = "cds"


@dataclass(frozen=True)
class BoundingBox:
    north_lat: float
    west_lon: float
    south_lat: float
    east_lon: float


@dataclass(frozen=True)
class WeatherRequest:
    source: DataSource
    dataset: str
    start_date: str
    end_date: str
    variables: list[str]
    bbox: BoundingBox


@dataclass(frozen=True)
class RawWeatherDataset:
    """RAW provider data represented as an in-memory xarray Dataset."""

    request: WeatherRequest
    downloaded_at: datetime
    raw_data: xr.Dataset


@dataclass(frozen=True)
class LoadedWeatherDataset:
    """RAW NetCDF checkpoint converted into a Pandas DataFrame for transformation."""

    request: WeatherRequest
    downloaded_at: datetime
    raw_data: pd.DataFrame


@dataclass(frozen=True)
class TransformedWeatherDataset:
    """Canonical transformed daily observations held in memory."""

    request: WeatherRequest
    transformed_at: datetime
    dataset: pd.DataFrame


@dataclass(frozen=True)
class DatasetArtifact:
    """Metadata describing one deterministic stored pipeline artifact."""

    layer: DataLayer
    created_at: datetime
    path: Path
