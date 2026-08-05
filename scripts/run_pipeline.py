import logging
import sys

from datetime import datetime
from pathlib import Path

project_root = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)
sys.path.insert(
    0,
    str(project_root),
)

from pipeline.interfaces import (
    WeatherExtractor,
    DataStorage,
    DataTransformer,
    DataLoader,
)

from pipeline.extract import (
    open_dataset,
    split_request_by_month,
    _select_bbox,
    ArcoExtractor
)
from pipeline.model import (
    BoundingBox,
    DataSource,
    WeatherRequest,
    LoadedWeatherDataset,
)
from pipeline.storage import (
    LocalDataStorage
)

from pipeline.transform import (
    DefaultDataTransformer
)

from pipeline.validation import (
    validate_raw_dataset,
    validate_transformed_dataset,
)

from pipeline.load import (
    PostgresDataLoader
)

from pipeline.logging import (
    log_pipeline_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


class WeatherPipeline:
    """
    SOLID: Single Responsibility & Dependency Inversion.
    Coordinates the pipeline steps using injected dependencies.
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

        # 1. Extract
        raw_dataset = self.extractor.extract(request)
        validate_raw_dataset(raw_dataset)

        # 2. Save Checkpoint 1
        nc_artifact = self.storage.save_raw(raw_dataset)

        # 3. Read to RAM
        raw_dataframe = self.storage.convert_to_dataframe(nc_artifact)
        loaded_dataset = LoadedWeatherDataset(
            request=request,
            downloaded_at=raw_dataset.downloaded_at,
            raw_data=raw_dataframe,
        )

        # 4. Transform
        transformed_dataset = self.transformer.transform(loaded_dataset)
        validate_transformed_dataset(transformed_dataset)

        # 5. Save Checkpoint 2
        parquet_artifact = self.storage.save_transformed(
            df=transformed_dataset.dataset,
            request=request,
            downloaded_at=raw_dataset.downloaded_at
        )

        # 6. Load to PostgreSQL
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


def run_pipeline_batch() -> None:
    """
    Execute the pipeline for every
    monthly request.
    """
    logger.info("[START] Batch Pipeline")

    request = WeatherRequest(
        source=DataSource.ARCO,
        dataset="era5-single-levels",
        start_date="1990-01-01",
        end_date="1990-01-03",
        variables=[
            "surface_solar_radiation_downwards",
            "total_precipitation",
        ],
        bbox=BoundingBox(
            north_lat=6.0,
            south_lat=1.0,
            east_lon=98.5,
            west_lon=95.0,
        ),
    )

    # 1. Open ARCO dataset once (Global Data).
    global_dataset = open_dataset(request)

    # 2. [OPTIMASI] Potong area Bounding Box dan Variabel SEKALI SAJA.
    logger.info("Applying spatial (BBox) and variable subsetting on global dataset...")
    spatial_dataset = _select_bbox(global_dataset, request)
    spatial_dataset = spatial_dataset[request.variables]

    spatial_dataset = spatial_dataset.chunk("auto")

    # 3. Split by month.
    requests = split_request_by_month(request)

    # 4. Looping untuk setiap bulan
    extractor = ArcoExtractor(spatial_dataset)
    storage = LocalDataStorage()
    transformer = DefaultDataTransformer()
    loader = PostgresDataLoader()

    pipeline = WeatherPipeline(
        extractor=extractor,
        storage=storage,
        transformer=transformer,
        loader=loader
    )

    for monthly_request in requests:
        pipeline.run(monthly_request)

    logger.info("[DONE] Batch Pipeline")


if __name__ == "__main__":
    run_pipeline_batch()