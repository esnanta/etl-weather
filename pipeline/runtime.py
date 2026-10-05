"""Compose validated, resumable ETL boundaries for CLI and Airflow callers.

Airflow may inject a provider submission gate while the scientific checkpoint
and request identity remain owned by the pipeline runtime.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from pipeline.extraction import CdsEra5LandExtractor
from pipeline.interfaces import DataLoader, DataStorage, DataTransformer, WeatherExtractor
from pipeline.loaders import PostgresDataLoader
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
from pipeline.storage import LocalDataStorage
from pipeline.storage.artifacts import DATA_DIRECTORY
from pipeline.transformation import DefaultDataTransformer
from pipeline.request import (
    build_request,
    build_request_key,
    split_request_by_month,
    validate_request,
    weather_request_to_spec,
)
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

    def ensure_raw_checkpoint(
        self,
        request: WeatherRequest,
    ) -> tuple[RawWeatherDataset, DatasetArtifact, LoadedWeatherDataset]:
        """Extract a RAW partition once, or resume from its immutable checkpoint."""
        artifact = self.storage.raw_artifact(request)
        resumed = self.storage.exists(artifact)

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
                "path": artifact.path.as_posix(),
            },
        )
        return raw_dataset, artifact, loaded_dataset

    def ensure_transformed_checkpoint(
        self,
        loaded_dataset: LoadedWeatherDataset,
        raw_artifact: DatasetArtifact,
    ) -> tuple[TransformedWeatherDataset, DatasetArtifact]:
        """Transform once, or resume from the deterministic Parquet checkpoint."""
        request = loaded_dataset.request
        artifact = self.storage.transformed_artifact(request)
        resumed = self.storage.exists(artifact)

        if resumed:
            transformed_dataset = self.storage.load_transformed(
                artifact,
                request,
                raw_artifact,
            )
            validate_transformed_dataset(transformed_dataset)
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

        log_metric(
            "Transformed checkpoint",
            {
                "status": "resumed" if resumed else "created",
                "path": artifact.path.as_posix(),
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

    def transform_from_checkpoint(
        self,
        request: WeatherRequest,
    ) -> tuple[TransformedWeatherDataset, DatasetArtifact]:
        """Rehydrate a validated RAW checkpoint and run the transform boundary."""
        raw_artifact = self.storage.raw_artifact(request)
        raw_dataset = self.storage.load_raw(raw_artifact, request)
        validate_raw_dataset(raw_dataset)
        loaded_dataset = LoadedWeatherDataset(
            request=raw_dataset.request,
            downloaded_at=raw_dataset.downloaded_at,
            raw_data=self.storage.convert_to_dataframe(raw_artifact),
        )
        return self.ensure_transformed_checkpoint(loaded_dataset, raw_artifact)

    def load_from_checkpoint(self, request: WeatherRequest) -> tuple[int, int]:
        """Rehydrate a validated transformed checkpoint and run the load boundary."""
        raw_artifact = self.storage.raw_artifact(request)
        transformed_artifact = self.storage.transformed_artifact(request)
        transformed_dataset = self.storage.load_transformed(
            transformed_artifact,
            request,
            raw_artifact,
        )
        validate_transformed_dataset(transformed_dataset)
        return self.load(transformed_dataset, transformed_artifact)

    def run(self, request: WeatherRequest) -> None:
        """Local wrapper that composes the same stage boundaries Airflow can call."""
        started_at = datetime.now()
        log_pipeline_header(request)

        extract_timer = log_section_start("EXTRACT")
        raw_dataset, raw_artifact, loaded_dataset = self.ensure_raw_checkpoint(request)
        normalized_request = raw_dataset.request
        log_section_completed("EXTRACT", extract_timer)

        transform_timer = log_section_start("TRANSFORM")
        transformed_dataset, parquet_artifact = self.ensure_transformed_checkpoint(
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


def build_default_pipeline(
    request: WeatherRequest,
    *,
    before_submission: Callable[[WeatherRequest], None] | None = None,
    chunk_cache_directory: Path | None = None,
) -> WeatherPipeline:
    """Build the default pipeline for a preselected source dataset."""
    if request.source is not DataSource.CDS:
        raise ValueError("Unsupported source for runtime pipeline.")
    extractor = CdsEra5LandExtractor(
        before_submission=before_submission,
        chunk_cache_directory=chunk_cache_directory,
    )

    return WeatherPipeline(
        extractor=extractor,
        storage=LocalDataStorage(),
        transformer=DefaultDataTransformer(),
        loader=PostgresDataLoader(),
    )


def ensure_raw_checkpoint_partition(
    request: WeatherRequest,
    *,
    before_submission: Callable[[WeatherRequest], None] | None = None,
) -> dict[str, object]:
    """Ensure one valid RAW checkpoint and return small orchestration metadata."""
    validate_request(request)
    normalized = build_request(request)
    chunk_cache_directory = (
        DATA_DIRECTORY / ".cds_retrieval_chunks" / build_request_key(normalized)
        if before_submission is not None else None
    )
    pipeline = (
        build_default_pipeline(
            normalized, before_submission=before_submission,
            chunk_cache_directory=chunk_cache_directory,
        ) if before_submission is not None else build_default_pipeline(normalized)
    )
    expected_artifact = pipeline.storage.raw_artifact(normalized)
    resumed = pipeline.storage.exists(expected_artifact)
    _, artifact, _ = pipeline.ensure_raw_checkpoint(normalized)
    if chunk_cache_directory is not None and chunk_cache_directory.exists():
        try:
            shutil.rmtree(chunk_cache_directory)
        except OSError:
            logger.warning("Could not remove completed CDS chunk cache: %s", chunk_cache_directory)
    return {
        "boundary": "ensure_raw_checkpoint",
        "request": weather_request_to_spec(normalized),
        "request_key": build_request_key(normalized),
        "artifact_path": artifact.path.as_posix(),
        "artifact_status": "resumed" if resumed else "created",
    }


def ensure_transformed_checkpoint_partition(request: WeatherRequest) -> dict[str, object]:
    """Ensure one valid transformed checkpoint and return control metadata."""
    validate_request(request)
    normalized = build_request(request)
    pipeline = build_default_pipeline(normalized)
    expected_artifact = pipeline.storage.transformed_artifact(normalized)
    resumed = pipeline.storage.exists(expected_artifact)
    _, artifact = pipeline.transform_from_checkpoint(normalized)
    return {
        "boundary": "ensure_transformed_checkpoint",
        "request": weather_request_to_spec(normalized),
        "request_key": build_request_key(normalized),
        "artifact_path": artifact.path.as_posix(),
        "artifact_status": "resumed" if resumed else "created",
    }


def load_request_partition(request: WeatherRequest) -> dict[str, object]:
    """Run one checkpoint-backed load and return a small result summary."""
    validate_request(request)
    normalized = build_request(request)
    pipeline = build_default_pipeline(normalized)
    transformed_artifact = pipeline.storage.transformed_artifact(normalized)
    daily_climate_id, rows_processed = pipeline.load_from_checkpoint(normalized)
    return {
        "boundary": "load",
        "request": weather_request_to_spec(normalized),
        "request_key": build_request_key(normalized),
        "artifact_path": transformed_artifact.path.as_posix(),
        "daily_climate_id": daily_climate_id,
        "rows_processed": rows_processed,
        "load_status": "skipped" if rows_processed == 0 else "processed",
    }


def run_monthly_batch(request: WeatherRequest) -> None:
    """Execute the pipeline for each calendar-month request."""
    validate_request(request)
    normalized_request = build_request(request)
    pipeline = build_default_pipeline(normalized_request)

    for monthly_request in split_request_by_month(normalized_request):
        pipeline.run(monthly_request)

    logger.info("Monthly pipeline batch completed.")
