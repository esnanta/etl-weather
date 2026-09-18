from __future__ import annotations

import os

from dotenv import load_dotenv

from pipeline.model import BoundingBox, DataSource, WeatherRequest

ACEH_LAND_DOMINANT_BBOX = BoundingBox(
    north_lat=5.2,
    south_lat=3.4,
    east_lon=97.6,
    west_lon=96.1,
)

CDS_DATASET = "reanalysis-era5-land"
CDS_START_DATE = "2022-01-01"
CDS_END_DATE = "2022-01-03"
CDS_VARIABLES = ["tp", "t2m", "swvl1", "ssrd", "d2m"]


def build_cds_request() -> WeatherRequest:
    _require_cds_api_key()
    return WeatherRequest(
        source=DataSource.CDS,
        dataset=CDS_DATASET,
        start_date=CDS_START_DATE,
        end_date=CDS_END_DATE,
        variables=CDS_VARIABLES.copy(),
        bbox=ACEH_LAND_DOMINANT_BBOX,
    )
def _require_cds_api_key() -> None:
    load_dotenv()
    if not os.getenv("CDSAPI_KEY", "").strip():
        raise ValueError(
            "CDSAPI_KEY is missing. Set CDSAPI_KEY=YOUR_UID:YOUR_API_KEY in .env."
        )
