from abc import ABC, abstractmethod
import pandas as pd
from pipeline.model import WeatherRequest, RawWeatherDataset, TransformedWeatherDataset, DatasetArtifact, LoadedWeatherDataset


class WeatherExtractor(ABC):
    @abstractmethod
    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        pass


class DataStorage(ABC):
    @abstractmethod
    def save_raw(self, dataset: RawWeatherDataset) -> DatasetArtifact:
        pass

    @abstractmethod
    def save_transformed(self, df: pd.DataFrame, request: WeatherRequest, downloaded_at) -> DatasetArtifact:
        pass

    @abstractmethod
    def convert_to_dataframe(self, artifact: DatasetArtifact) -> pd.DataFrame:
        pass


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, raw_dataset: LoadedWeatherDataset) -> TransformedWeatherDataset:
        pass


class DataLoader(ABC):
    @abstractmethod
    def load(self, dataset: TransformedWeatherDataset) -> tuple[int, int]:
        pass
