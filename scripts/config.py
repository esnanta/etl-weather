from __future__ import annotations

from pipeline.model import BoundingBox, DataSource, WeatherRequest

DEFAULT_DATA_SOURCE = DataSource.ARCO
DEFAULT_DATASET = "era5-single-levels"
DEFAULT_START_DATE = "1990-01-01"
DEFAULT_END_DATE = "1990-01-02"
DEFAULT_VARIABLES = [
    "surface_solar_radiation_downwards",
    "total_precipitation",
]
DEFAULT_BBOX = BoundingBox(
    north_lat=6.0,
    south_lat=1.0,
    east_lon=98.5,
    west_lon=95.0,
)


def build_default_request() -> WeatherRequest:
    """Return the default local execution request for the pipeline."""

    return WeatherRequest(
        source=DEFAULT_DATA_SOURCE,
        dataset=DEFAULT_DATASET,
        start_date=DEFAULT_START_DATE,
        end_date=DEFAULT_END_DATE,
        variables=DEFAULT_VARIABLES.copy(),
        bbox=DEFAULT_BBOX,
    )
