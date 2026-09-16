from __future__ import annotations

import os

from dotenv import load_dotenv

from pipeline.model import BoundingBox, DataSource, WeatherRequest

ACEH_BBOX = BoundingBox(
    north_lat=6.2,
    south_lat=1.8,
    east_lon=98.6,
    west_lon=94.8,
)

# Land-dominant subset inside Aceh to reduce ERA5-Land ocean-only NaNs.
ACEH_LAND_DOMINANT_BBOX = BoundingBox(
    north_lat=5.2,
    south_lat=3.4,
    east_lon=97.6,
    west_lon=96.1,
)

ARCO_DATASET = "era5-single-levels"
ARCO_START_DATE = "1990-01-01"
ARCO_END_DATE = "1990-01-02"
ARCO_VARIABLES = [
    "surface_solar_radiation_downwards",
    "total_precipitation",
]

CDS_DATASET = "reanalysis-era5-land"
CDS_START_DATE = "2022-01-01"
CDS_END_DATE = "2022-01-03"
CDS_VARIABLES = [
    "total_precipitation",
    "2m_temperature",
    "volumetric_soil_water_layer_1",
    "surface_solar_radiation_downwards",
    "2m_dewpoint_temperature"
]

def build_arco_request() -> WeatherRequest:
    """Return default local request for ARCO source."""
    return WeatherRequest(
        source=DataSource.ARCO,
        dataset=ARCO_DATASET,
        start_date=ARCO_START_DATE,
        end_date=ARCO_END_DATE,
        variables=ARCO_VARIABLES.copy(),
        bbox=ACEH_BBOX,
    )


def build_cds_request() -> WeatherRequest:
    """Return default local request for CDS ERA5-Land source."""
    _require_cds_api_key()
    return WeatherRequest(
        source=DataSource.CDS,
        dataset=CDS_DATASET,
        start_date=CDS_START_DATE,
        end_date=CDS_END_DATE,
        variables=CDS_VARIABLES.copy(),
        bbox=ACEH_LAND_DOMINANT_BBOX,
    )


def build_request_from_env() -> WeatherRequest:
    """Wrapper helper for selecting source via WEATHER_DATA_SOURCE."""
    load_dotenv()
    source_name = os.getenv("WEATHER_DATA_SOURCE", DataSource.ARCO.value).strip().lower()

    if source_name == DataSource.ARCO.value:
        return build_arco_request()

    if source_name == DataSource.CDS.value:
        return build_cds_request()

    raise ValueError(
        f"Invalid WEATHER_DATA_SOURCE: '{source_name}'. "
        f"Supported values: {DataSource.ARCO.value}, {DataSource.CDS.value}."
    )


def _require_cds_api_key() -> None:
    """Fail fast when CDS run is selected without CDS API key."""
    load_dotenv()
    if not os.getenv("CDSAPI_KEY", "").strip():
        raise ValueError(
            "CDSAPI_KEY is missing. Set CDSAPI_KEY=YOUR_UID:YOUR_API_KEY in .env."
        )
