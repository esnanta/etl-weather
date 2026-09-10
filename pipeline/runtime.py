from __future__ import annotations

import logging
from datetime import datetime

from pipeline.extract import ArcoExtractor, CdsExtractor, _select_bbox, open_dataset
from pipeline.interfaces import DataLoader, DataStorage, DataTransformer, WeatherExtractor
from pipeline.load import PostgresDataLoader
from pipeline.logging import (
    log_pipeline_header,
    log_pipeline_summary,
    log_section_completed,
    log_section_start,
)
from pipeline.model import DataSource, LoadedWeatherDataset, WeatherRequest
from pipeline.storage import LocalDataStorage
from pipeline.transform import DefaultDataTransformer
from pipeline.utils import build_request, split_request_by_week, validate_request
from pipeline.validation import validate_raw_dataset, validate_transformed_dataset

logger = logging.getLogger(__name__)


class WeatherPipeline:
    """
    Coordinate pipeline steps using injected dependencies.
    """

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

    def run(self, request: WeatherRequest) -> None:
        started_at = datetime.now()
        log_pipeline_header(request)

        extract_timer = log_section_start("EXTRACT")
        raw_dataset = self.extractor.extract(request)
        normalized_request = raw_dataset.request
        validate_raw_dataset(raw_dataset)
        nc_artifact = self.storage.save_raw(raw_dataset)
        raw_dataframe = self.storage.convert_to_dataframe(nc_artifact)
        loaded_dataset = LoadedWeatherDataset(
            request=normalized_request,
            downloaded_at=raw_dataset.downloaded_at,
            raw_data=raw_dataframe,
        )
        log_section_completed("EXTRACT", extract_timer)

        transform_timer = log_section_start("TRANSFORM")
        transformed_dataset = self.transformer.transform(loaded_dataset)
        validate_transformed_dataset(transformed_dataset)
        parquet_artifact = self.storage.save_transformed(
            df=transformed_dataset.dataset,
            request=normalized_request,
            downloaded_at=raw_dataset.downloaded_at,
        )
        log_section_completed("TRANSFORM", transform_timer)

        load_timer = log_section_start("LOAD DATABASE")
        dataset_id, rows_inserted = self.loader.load(transformed_dataset)
        log_section_completed("LOAD DATABASE", load_timer)

        finished_at = datetime.now()
        log_pipeline_summary(
            request=normalized_request,
            dataset_id=dataset_id,
            rows_inserted=rows_inserted,
            parquet_path=parquet_artifact.path,
            started_at=started_at,
            finished_at=finished_at,
        )


def build_default_pipeline(request: WeatherRequest) -> WeatherPipeline:
    """
    Build the default pipeline for a preselected source dataset.
    """
    if request.source is DataSource.ARCO:
        global_dataset = open_dataset(request)

        logger.info(
            "Applying spatial subset and variable selection to the source dataset."
        )
        spatial_dataset = _select_bbox(global_dataset, request)
        spatial_dataset = spatial_dataset[request.variables]
        spatial_dataset = spatial_dataset.chunk("auto")
        extractor = ArcoExtractor(spatial_dataset)
    elif request.source is DataSource.CDS:
        extractor = CdsExtractor()
    else:
        raise ValueError(
            f"Unsupported source for runtime pipeline: {request.source.value}"
        )

    return WeatherPipeline(
        extractor=extractor,
        storage=LocalDataStorage(),
        transformer=DefaultDataTransformer(),
        loader=PostgresDataLoader(),
    )


def run_weekly_batch(request: WeatherRequest) -> None:
    """
    Execute the pipeline for each weekly request in the requested range.
    """

    validate_request(request)
    normalized_request = build_request(request)
    pipeline = build_default_pipeline(normalized_request)

    for weekly_request in split_request_by_week(normalized_request):
        pipeline.run(weekly_request)

    logger.info("Weekly pipeline batch completed.")
