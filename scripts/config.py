from __future__ import annotations

from pipeline.model import BoundingBox, DataSource, WeatherRequest

GOOGLE_ARCO_DATA_SOURCE = DataSource.ARCO
GOOGLE_ARCO_DATASET = "era5-single-levels"
GOOGLE_ARCO_START_DATE = "1990-01-01"
GOOGLE_ARCO_END_DATE = "1990-01-02"
GOOGLE_ARCO_VARIABLES = [
    "surface_solar_radiation_downwards",
    "total_precipitation",
]

GOOGLE_ARCO_BBOX = BoundingBox(
    north_lat=6.0,
    south_lat=1.0,
    east_lon=98.5,
    west_lon=95.0,
)


def build_google_arco_request() -> WeatherRequest:
    """Return the default local execution request for the pipeline."""

    return WeatherRequest(
        source=GOOGLE_ARCO_DATA_SOURCE,
        dataset=GOOGLE_ARCO_DATASET,
        start_date=GOOGLE_ARCO_START_DATE,
        end_date=GOOGLE_ARCO_END_DATE,
        variables=GOOGLE_ARCO_VARIABLES.copy(),
        bbox=GOOGLE_ARCO_BBOX,
    )
