from __future__ import annotations

import logging
from datetime import datetime

from pipeline.extract import CdsExtractor
from pipeline.interfaces import DataLoader, DataStorage, DataTransformer, WeatherExtractor
from pipeline.load import PostgresDataLoader
from pipeline.logging import (
    log_metric,
    log_pipeline_header,
    log_pipeline_summary,
    log_section_completed,
    log_section_start,
)
from pipeline.model import (
    DataSource,
    DatasetArtifact,
    LoadedWeatherDataset,
    RawWeatherDataset,
    TransformedWeatherDataset,
    WeatherRequest,
)
from pipeline.storage import (
    LocalDataStorage,
    artifact_exists,
    artifact_path_for_metadata,
)
from pipeline.transform import DefaultDataTransformer
from pipeline.utils import build_request, split_request_by_month, validate_request
from pipeline.validation import validate_raw_dataset, validate_transformed_dataset

logger = logging.getLogger(__name__)


class WeatherPipeline:
    """Coordinate resumable ETL stages using injected dependencies."""

    def __init__(
        self,
        extractor: WeatherExtractor,
        storage: DataStorage,
        transformer: DataTransformer,
        loader: DataLoader,
    ):
        self.extractor = extractor
        self.storage = storage
        self.transformer = transformer
        self.loader = loader

    def extract_or_resume(
        self,
        request: WeatherRequest,
    ) -> tuple[RawWeatherDataset, DatasetArtifact, LoadedWeatherDataset]:
        """Extract a RAW partition once, or resume from its immutable checkpoint."""
        artifact = self.storage.raw_artifact(request)
        resumed = artifact_exists(artifact)

        if resumed:
            raw_dataset = self.storage.load_raw(artifact, request)
        else:
            raw_dataset = self.extractor.extract(request)
            artifact = self.storage.save_raw(raw_dataset)

        validate_raw_dataset(raw_dataset)
        raw_dataframe = self.storage.convert_to_dataframe(artifact)
        loaded_dataset = LoadedWeatherDataset(
            request=raw_dataset.request,
            downloaded_at=raw_dataset.downloaded_at,
            raw_data=raw_dataframe,
        )

        log_metric(
            "RAW checkpoint",
            {
                "status": "resumed" if resumed else "created",
                "path": artifact_path_for_metadata(artifact.path),
            },
        )
        return raw_dataset, artifact, loaded_dataset

    def transform_or_resume(
        self,
        loaded_dataset: LoadedWeatherDataset,
        raw_artifact: DatasetArtifact,
    ) -> tuple[TransformedWeatherDataset, DatasetArtifact]:
        """Transform once, or resume from the deterministic Parquet checkpoint."""
        request = loaded_dataset.request
        artifact = self.storage.transformed_artifact(request)
        resumed = artifact_exists(artifact)

        if resumed:
            transformed_dataset = self.storage.load_transformed(
                artifact,
                request,
                raw_artifact,
            )
        else:
            transformed_dataset = self.transformer.transform(loaded_dataset)
            validate_transformed_dataset(transformed_dataset)
            artifact = self.storage.save_transformed(
                df=transformed_dataset.dataset,
                request=request,
                downloaded_at=loaded_dataset.downloaded_at,
                transformed_at=transformed_dataset.transformed_at,
                raw_artifact=raw_artifact,
            )

        if resumed:
            validate_transformed_dataset(transformed_dataset)
        log_metric(
            "Transformed checkpoint",
            {
                "status": "resumed" if resumed else "created",
                "path": artifact_path_for_metadata(artifact.path),
            },
        )
        return transformed_dataset, artifact

    def load(
        self,
        transformed_dataset: TransformedWeatherDataset,
        transformed_artifact: DatasetArtifact,
    ) -> tuple[int, int]:
        """Load a transformed partition through an idempotent loader."""
        return self.loader.load(transformed_dataset, transformed_artifact)

    def run(self, request: WeatherRequest) -> None:
        """Local wrapper that composes the same stage boundaries Airflow can call."""
        started_at = datetime.now()
        log_pipeline_header(request)

        extract_timer = log_section_start("EXTRACT")
        raw_dataset, raw_artifact, loaded_dataset = self.extract_or_resume(request)
        normalized_request = raw_dataset.request
        log_section_completed("EXTRACT", extract_timer)

        transform_timer = log_section_start("TRANSFORM")
        transformed_dataset, parquet_artifact = self.transform_or_resume(
            loaded_dataset,
            raw_artifact,
        )
        log_section_completed("TRANSFORM", transform_timer)

        load_timer = log_section_start("LOAD DATABASE")
        daily_climate_id, rows_processed = self.load(
            transformed_dataset,
            parquet_artifact,
        )
        log_section_completed("LOAD DATABASE", load_timer)

        finished_at = datetime.now()
        log_pipeline_summary(
            request=normalized_request,
            daily_climate_id=daily_climate_id,
            rows_processed=rows_processed,
            parquet_path=parquet_artifact.path,
            started_at=started_at,
            finished_at=finished_at,
        )


def build_default_pipeline(request: WeatherRequest) -> WeatherPipeline:
    """Build the default pipeline for a preselected source dataset."""
    if request.source is not DataSource.CDS:
        raise ValueError(
            f"Unsupported source for runtime pipeline: {request.source.value}"
        )
    extractor = CdsExtractor()

    return WeatherPipeline(
        extractor=extractor,
        storage=LocalDataStorage(),
        transformer=DefaultDataTransformer(),
        loader=PostgresDataLoader(),
    )


def run_monthly_batch(request: WeatherRequest) -> None:
    """Execute the pipeline for each calendar-month request."""
    validate_request(request)
    normalized_request = build_request(request)
    pipeline = build_default_pipeline(normalized_request)

    for monthly_request in split_request_by_month(normalized_request):
        pipeline.run(monthly_request)

    logger.info("Monthly pipeline batch completed.")
