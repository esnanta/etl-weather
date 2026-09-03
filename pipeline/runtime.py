from __future__ import annotations

import logging
from datetime import datetime

from pipeline.extract import ArcoExtractor, _select_bbox, open_dataset
from pipeline.interfaces import DataLoader, DataStorage, DataTransformer, WeatherExtractor
from pipeline.load import PostgresDataLoader
from pipeline.logging import log_pipeline_summary
from pipeline.model import DataSource, LoadedWeatherDataset, WeatherRequest
from pipeline.storage import LocalDataStorage
from pipeline.transform import DefaultDataTransformer
from pipeline.utils import split_request_by_week
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
        logger.info("[START] Pipeline | %s -> %s", request.start_date, request.end_date)

        raw_dataset = self.extractor.extract(request)
        validate_raw_dataset(raw_dataset)

        nc_artifact = self.storage.save_raw(raw_dataset)

        raw_dataframe = self.storage.convert_to_dataframe(nc_artifact)
        loaded_dataset = LoadedWeatherDataset(
            request=request,
            downloaded_at=raw_dataset.downloaded_at,
            raw_data=raw_dataframe,
        )

        transformed_dataset = self.transformer.transform(loaded_dataset)
        validate_transformed_dataset(transformed_dataset)

        parquet_artifact = self.storage.save_transformed(
            df=transformed_dataset.dataset,
            request=request,
            downloaded_at=raw_dataset.downloaded_at,
        )

        dataset_id, rows_inserted = self.loader.load(transformed_dataset)

        finished_at = datetime.now()
        log_pipeline_summary(
            request=request,
            dataset_id=dataset_id,
            rows_inserted=rows_inserted,
            parquet_path=parquet_artifact.path,
            started_at=started_at,
            finished_at=finished_at,
        )

        logger.info("[DONE] Pipeline | %s -> %s", request.start_date, request.end_date)


def build_default_pipeline(request: WeatherRequest) -> WeatherPipeline:
    """
    Build the default pipeline for a preselected source dataset.
    """
    if request.source is not DataSource.ARCO:
        raise NotImplementedError(
            f"Default runtime pipeline is not implemented for source: {request.source.value}"
        )

    global_dataset = open_dataset(request)

    logger.info(
        "Applying spatial subset and variable selection to the source dataset."
    )
    spatial_dataset = _select_bbox(global_dataset, request)
    spatial_dataset = spatial_dataset[request.variables]
    spatial_dataset = spatial_dataset.chunk("auto")

    return WeatherPipeline(
        extractor=ArcoExtractor(spatial_dataset),
        storage=LocalDataStorage(),
        transformer=DefaultDataTransformer(),
        loader=PostgresDataLoader(),
    )


def run_weekly_batch(request: WeatherRequest) -> None:
    """
    Execute the pipeline for each weekly request in the requested range.
    """

    logger.info("[START] Batch Pipeline")

    pipeline = build_default_pipeline(request)

    for weekly_request in split_request_by_week(request):
        pipeline.run(weekly_request)

    logger.info("[DONE] Batch Pipeline")
