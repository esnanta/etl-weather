from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from pipeline.model import (
    DatasetArtifact,
    LoadedWeatherDataset,
    RawWeatherDataset,
    TransformedWeatherDataset,
    WeatherRequest,
)


class WeatherExtractor(ABC):
    @abstractmethod
    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        ...


class DataStorage(ABC):
    @abstractmethod
    def exists(self, artifact: DatasetArtifact) -> bool:
        ...

    @abstractmethod
    def raw_artifact(self, request: WeatherRequest) -> DatasetArtifact:
        ...

    @abstractmethod
    def transformed_artifact(self, request: WeatherRequest) -> DatasetArtifact:
        ...

    @abstractmethod
    def save_raw(self, dataset: RawWeatherDataset) -> DatasetArtifact:
        ...

    @abstractmethod
    def load_raw(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
    ) -> RawWeatherDataset:
        ...

    @abstractmethod
    def save_transformed(
        self,
        df: pd.DataFrame,
        request: WeatherRequest,
        downloaded_at: datetime,
        transformed_at: datetime,
        raw_artifact: DatasetArtifact,
    ) -> DatasetArtifact:
        ...

    @abstractmethod
    def load_transformed(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
        raw_artifact: DatasetArtifact,
    ) -> TransformedWeatherDataset:
        ...

    @abstractmethod
    def convert_to_dataframe(self, artifact: DatasetArtifact) -> pd.DataFrame:
        ...


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, raw_dataset: LoadedWeatherDataset) -> TransformedWeatherDataset:
        ...


class DataLoader(ABC):
    @abstractmethod
    def load(
        self,
        dataset: TransformedWeatherDataset,
        artifact: DatasetArtifact,
    ) -> tuple[int, int]:
        ...
