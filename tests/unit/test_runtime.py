from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import xarray as xr

from pipeline.interfaces import DataLoader, DataStorage, DataTransformer, WeatherExtractor
from pipeline.model import (
    BoundingBox,
    DataLayer,
    DataSource,
    DatasetArtifact,
    LoadedWeatherDataset,
    RawWeatherDataset,
    TransformedWeatherDataset,
    WeatherRequest,
)
from pipeline.runtime import WeatherPipeline


def _request() -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2022-01-01",
        end_date="2022-01-01",
        variables=["tp"],
        bbox=BoundingBox(
            north_lat=5.0,
            west_lon=95.0,
            south_lat=5.0,
            east_lon=95.0,
        ),
    )


def _raw_dataset(request: WeatherRequest) -> RawWeatherDataset:
    return RawWeatherDataset(
        request=request,
        downloaded_at=datetime(2022, 1, 2, tzinfo=UTC),
        raw_data=xr.Dataset(
            {"tp": (("valid_time", "latitude", "longitude"), [[[0.001]]])},
            coords={
                "valid_time": pd.to_datetime(["2022-01-01 00:00:00"]),
                "latitude": [5.0],
                "longitude": [95.0],
            },
        ),
    )


def _loaded_dataset(request: WeatherRequest) -> LoadedWeatherDataset:
    return LoadedWeatherDataset(
        request=request,
        downloaded_at=datetime(2022, 1, 2, tzinfo=UTC),
        raw_data=pd.DataFrame(
            {
                "valid_time": [pd.Timestamp("2022-01-01 00:00:00")],
                "latitude": [5.0],
                "longitude": [95.0],
                "tp": [0.001],
            }
        ),
    )


def _transformed_dataset(request: WeatherRequest) -> TransformedWeatherDataset:
    return TransformedWeatherDataset(
        request=request,
        transformed_at=datetime(2022, 1, 2, 1, tzinfo=UTC),
        dataset=pd.DataFrame(
            {
                "valid_time": [pd.Timestamp("2021-12-31 17:00:00")],
                "valid_time_local": [pd.Timestamp("2022-01-01 00:00:00")],
                "latitude": [5.0],
                "longitude": [95.0],
                "tp": [10.0],
            }
        ),
    )


class _Extractor(WeatherExtractor):
    def __init__(self, raw_dataset: RawWeatherDataset):
        self.raw_dataset = raw_dataset
        self.calls = 0

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        self.calls += 1
        assert request == self.raw_dataset.request
        return self.raw_dataset


class _Storage(DataStorage):
    def __init__(
        self,
        request: WeatherRequest,
        raw_dataset: RawWeatherDataset,
        loaded_dataset: LoadedWeatherDataset,
        transformed_dataset: TransformedWeatherDataset,
    ):
        self.request = request
        self.raw_dataset_value = raw_dataset
        self.loaded_dataset_value = loaded_dataset
        self.transformed_dataset_value = transformed_dataset
        self.raw_artifact_value = DatasetArtifact(
            layer=DataLayer.RAW,
            created_at=datetime(2022, 1, 2, tzinfo=UTC),
            path=Path("raw.nc"),
        )
        self.transformed_artifact_value = DatasetArtifact(
            layer=DataLayer.TRANSFORMED,
            created_at=datetime(2022, 1, 2, 1, tzinfo=UTC),
            path=Path("daily.parquet"),
        )
        self.load_raw_calls = 0
        self.save_raw_calls = 0
        self.load_transformed_calls = 0
        self.save_transformed_calls = 0
        self.existing_artifacts: set[Path] = set()

    def exists(self, artifact: DatasetArtifact) -> bool:
        return artifact.path in self.existing_artifacts

    def raw_artifact(self, request: WeatherRequest) -> DatasetArtifact:
        assert request == self.request
        return self.raw_artifact_value

    def transformed_artifact(self, request: WeatherRequest) -> DatasetArtifact:
        assert request == self.request
        return self.transformed_artifact_value

    def load_raw(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
    ) -> RawWeatherDataset:
        self.load_raw_calls += 1
        assert artifact == self.raw_artifact_value
        assert request == self.request
        return self.raw_dataset_value

    def save_raw(self, dataset: RawWeatherDataset) -> DatasetArtifact:
        self.save_raw_calls += 1
        assert dataset == self.raw_dataset_value
        return self.raw_artifact_value

    def convert_to_dataframe(self, artifact: DatasetArtifact) -> pd.DataFrame:
        assert artifact == self.raw_artifact_value
        return self.loaded_dataset_value.raw_data.copy()

    def load_transformed(
        self,
        artifact: DatasetArtifact,
        request: WeatherRequest,
        raw_artifact: DatasetArtifact,
    ) -> TransformedWeatherDataset:
        self.load_transformed_calls += 1
        assert artifact == self.transformed_artifact_value
        assert request == self.request
        assert raw_artifact == self.raw_artifact_value
        return self.transformed_dataset_value

    def save_transformed(
        self,
        df: pd.DataFrame,
        request: WeatherRequest,
        downloaded_at: datetime,
        transformed_at: datetime,
        raw_artifact: DatasetArtifact,
    ) -> DatasetArtifact:
        self.save_transformed_calls += 1
        assert request == self.request
        assert list(df.columns) == list(self.transformed_dataset_value.dataset.columns)
        assert downloaded_at == self.loaded_dataset_value.downloaded_at
        assert transformed_at == self.transformed_dataset_value.transformed_at
        assert raw_artifact == self.raw_artifact_value
        return self.transformed_artifact_value


class _Transformer(DataTransformer):
    def __init__(self, transformed_dataset: TransformedWeatherDataset):
        self.transformed_dataset = transformed_dataset
        self.calls = 0

    def transform(
        self,
        raw_dataset: LoadedWeatherDataset,
    ) -> TransformedWeatherDataset:
        self.calls += 1
        assert isinstance(raw_dataset, LoadedWeatherDataset)
        return self.transformed_dataset


class _Loader(DataLoader):
    def __init__(self):
        self.calls = 0
        self.dataset = None
        self.artifact = None

    def load(
        self,
        dataset: TransformedWeatherDataset,
        artifact: DatasetArtifact,
    ) -> tuple[int, int]:
        self.calls += 1
        self.dataset = dataset
        self.artifact = artifact
        return 7, len(dataset.dataset)


def _pipeline():
    request = _request()
    raw_dataset = _raw_dataset(request)
    loaded_dataset = _loaded_dataset(request)
    transformed_dataset = _transformed_dataset(request)
    extractor = _Extractor(raw_dataset)
    storage = _Storage(request, raw_dataset, loaded_dataset, transformed_dataset)
    transformer = _Transformer(transformed_dataset)
    loader = _Loader()
    pipeline = WeatherPipeline(extractor, storage, transformer, loader)
    return request, pipeline, extractor, storage, transformer, loader


def test_extract_or_resume_creates_raw_checkpoint_when_missing(monkeypatch) -> None:
    request, pipeline, extractor, storage, _, _ = _pipeline()
    validation_calls = []

    monkeypatch.setattr(
        "pipeline.runtime.validate_raw_dataset",
        lambda dataset: validation_calls.append(dataset),
    )

    raw_dataset, artifact, loaded_dataset = pipeline.extract_or_resume(request)

    assert extractor.calls == 1
    assert storage.save_raw_calls == 1
    assert storage.load_raw_calls == 0
    assert validation_calls == [raw_dataset]
    assert artifact == storage.raw_artifact_value
    assert loaded_dataset.request == request
    assert loaded_dataset.raw_data.equals(storage.loaded_dataset_value.raw_data)


def test_extract_or_resume_uses_and_validates_existing_raw_checkpoint(monkeypatch) -> None:
    request, pipeline, extractor, storage, _, _ = _pipeline()
    validation_calls = []

    storage.existing_artifacts.add(storage.raw_artifact_value.path)
    monkeypatch.setattr(
        "pipeline.runtime.validate_raw_dataset",
        lambda dataset: validation_calls.append(dataset),
    )

    raw_dataset, _, _ = pipeline.extract_or_resume(request)

    assert extractor.calls == 0
    assert storage.load_raw_calls == 1
    assert storage.save_raw_calls == 0
    assert validation_calls == [raw_dataset]


def test_transform_or_resume_uses_and_validates_existing_checkpoint(monkeypatch) -> None:
    _, pipeline, _, storage, transformer, _ = _pipeline()
    validation_calls = []

    storage.existing_artifacts.add(storage.transformed_artifact_value.path)
    monkeypatch.setattr(
        "pipeline.runtime.validate_transformed_dataset",
        lambda dataset: validation_calls.append(dataset),
    )

    transformed, artifact = pipeline.transform_or_resume(
        storage.loaded_dataset_value,
        storage.raw_artifact_value,
    )

    assert transformer.calls == 0
    assert storage.load_transformed_calls == 1
    assert storage.save_transformed_calls == 0
    assert validation_calls == [transformed]
    assert artifact == storage.transformed_artifact_value


def test_transform_or_resume_validates_before_saving_new_checkpoint(
    monkeypatch,
) -> None:
    _, pipeline, _, storage, transformer, _ = _pipeline()
    validation_calls = []

    monkeypatch.setattr(
        "pipeline.runtime.validate_transformed_dataset",
        lambda dataset: validation_calls.append(dataset),
    )

    transformed, _ = pipeline.transform_or_resume(
        storage.loaded_dataset_value,
        storage.raw_artifact_value,
    )

    assert transformer.calls == 1
    assert storage.load_transformed_calls == 0
    assert storage.save_transformed_calls == 1
    assert validation_calls == [transformed]


def test_load_delegates_dataset_and_artifact_to_idempotent_loader() -> None:
    _, pipeline, _, storage, _, loader = _pipeline()

    result = pipeline.load(
        storage.transformed_dataset_value,
        storage.transformed_artifact_value,
    )

    assert result == (7, 1)
    assert loader.calls == 1
    assert loader.dataset == storage.transformed_dataset_value
    assert loader.artifact == storage.transformed_artifact_value
