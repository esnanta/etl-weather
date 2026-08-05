# pipeline/interfaces.py
from abc import ABC, abstractmethod
from pipeline.model import WeatherRequest, RawWeatherDataset, TransformedWeatherDataset, DatasetArtifact


class WeatherExtractor(ABC):
    @abstractmethod
    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        pass


class DataStorage(ABC):
    @abstractmethod
    def save_raw(self, dataset: RawWeatherDataset) -> DatasetArtifact:
        pass

    @abstractmethod
    def save_transformed(self, df, request: WeatherRequest, downloaded_at) -> DatasetArtifact:
        pass


class DataLoader(ABC):
    @abstractmethod
    def load(self, dataset: TransformedWeatherDataset) -> tuple[int, int]:
        pass