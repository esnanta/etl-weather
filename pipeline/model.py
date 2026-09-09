# pipeline/models.py
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import pandas as pd
import xarray as xr

from pathlib import Path

class DataLayer(Enum):
    RAW = "raw"
    TRANSFORMED = "transformed"

class DataSource(Enum):
    ARCO = "arco"
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
    """
    Raw dataset immediately after extraction.

    The dataset is still a lazy xarray.Dataset.
    Materialization is delegated to the storage layer
    when the dataset is written to NetCDF.
    """

    request: WeatherRequest
    downloaded_at: datetime
    raw_data: xr.Dataset

@dataclass(frozen=True)
class LoadedWeatherDataset:
    """
    Raw dataset after being reloaded from a Parquet checkpoint.

    At this stage the data is represented as a Pandas DataFrame.
    """

    request: WeatherRequest
    downloaded_at: datetime
    raw_data: pd.DataFrame

@dataclass(frozen=True)
class DownloadWeatherDataset:

    request: WeatherRequest
    downloaded_at: datetime
    dataset: pd.DataFrame

@dataclass(frozen=True)
class TransformedWeatherDataset:

    request: WeatherRequest
    transformed_at: datetime
    dataset: pd.DataFrame

@dataclass(frozen=True)
class DatasetArtifact:
    """Metadata describing a stored artifact."""

    layer: DataLayer
    created_at: datetime
    path: Path