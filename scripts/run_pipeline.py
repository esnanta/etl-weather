import logging
import sys

from datetime import datetime
from pathlib import Path

import xarray as xr

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

from pipeline.extract import (
    BoundingBox,
    DataSource,
    WeatherRequest,
    extract_data,
    open_dataset,
    split_request_by_month,
    LoadedWeatherDataset,
    _select_bbox
)

from pipeline.storage import (
    save_raw_netcdf,
    convert_netcdf_to_dataframe,
    save_transformed_parquet,
)

from pipeline.transform import (
    transform_data,
)

from pipeline.validation import (
    validate_raw_dataset,
    validate_transformed_dataset,
)

from pipeline.load import (
    load_to_postgresql,
)

from pipeline.logging import (
    log_pipeline_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def run_pipeline(
    request: WeatherRequest,
    dataset: xr.Dataset,
) -> None:
    """
    Execute the complete pipeline for
    one WeatherRequest using SOLID functions.
    """
    started_at = datetime.now()
    logger.info("[START] Pipeline | %s -> %s", request.start_date, request.end_date)

    # ---------------------------------------------------------
    # 1. Extract (Lazy)
    # ---------------------------------------------------------
    raw_dataset = extract_data(request=request, dataset=dataset)
    validate_raw_dataset(raw_dataset)

    # ---------------------------------------------------------
    # 2. Save Checkpoint 1 (Layer: RAW, Format: .nc)
    # ---------------------------------------------------------
    nc_artifact = save_raw_netcdf(raw_dataset)

    # ---------------------------------------------------------
    # 3. Read to RAM (Konversi .nc ke DataFrame)
    # ---------------------------------------------------------
    raw_dataframe = convert_netcdf_to_dataframe(nc_artifact)
    loaded_dataset = LoadedWeatherDataset(
        request=request,
        downloaded_at=raw_dataset.downloaded_at,
        raw_data=raw_dataframe,
    )
    # ---------------------------------------------------------
    # 4. Transform
    # ---------------------------------------------------------
    transformed_dataset = transform_data(loaded_dataset)
    validate_transformed_dataset(transformed_dataset)

    # ---------------------------------------------------------
    # 5. Save Checkpoint 2 (Layer: TRANSFORMED, Format: .parquet)
    # ---------------------------------------------------------
    parquet_artifact = save_transformed_parquet(
        df=transformed_dataset.dataset,
        request=request,
        downloaded_at=raw_dataset.downloaded_at
    )

    # ---------------------------------------------------------
    # 6. Load to PostgreSQL
    # ---------------------------------------------------------
    dataset_id, rows_inserted = load_to_postgresql(transformed_dataset)

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
        end_date="1990-01-15",
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
    for monthly_request in requests:
        run_pipeline(
            request=monthly_request,
            dataset=spatial_dataset,
        )

    logger.info("[DONE] Batch Pipeline")


if __name__ == "__main__":
    run_pipeline_batch()