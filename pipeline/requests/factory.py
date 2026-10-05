"""Construct typed CDS requests from lightweight shared configuration."""

from __future__ import annotations

import os

from dotenv import load_dotenv

from config import (
    ACEH_BBOX_COORDINATES,
    CDS_DATASET,
    CDS_VARIABLES,
    resolve_cds_period,
)
from pipeline.models.weather import BoundingBox, DataSource, WeatherRequest


def build_aceh_bbox() -> BoundingBox:
    """Return the configured Aceh bounding box as the pipeline model type."""
    return BoundingBox(**ACEH_BBOX_COORDINATES)


def build_cds_request(
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    require_api_key: bool = True,
) -> WeatherRequest:
    """Build the active CDS request for an explicit or configured period."""
    load_dotenv()
    if start_date is None or end_date is None:
        configured_start, configured_end = resolve_cds_period(os.environ)
        start_date = start_date or configured_start
        end_date = end_date or configured_end
    if require_api_key:
        _require_cds_api_key()
    return WeatherRequest(
        source=DataSource.CDS,
        dataset=CDS_DATASET,
        start_date=start_date,
        end_date=end_date,
        variables=list(CDS_VARIABLES),
        bbox=build_aceh_bbox(),
    )


def _require_cds_api_key() -> None:
    if not os.getenv("CDSAPI_KEY", "").strip():
        raise ValueError(
            "CDSAPI_KEY is missing. Set CDSAPI_KEY=YOUR_UID:YOUR_API_KEY in .env."
        )
