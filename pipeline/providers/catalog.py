"""Resolve source/dataset profiles independently of provider transport."""

from __future__ import annotations

from pipeline.models.weather import DataSource
from pipeline.providers.cds_era5_land import (
    CDS_DATASET_PROFILES,
    get_cds_era5_land_dataset_profile,
)


DATASET_PROFILES_BY_SOURCE = {
    DataSource.CDS: CDS_DATASET_PROFILES,
}


def get_source_dataset_profile(source: DataSource, dataset: str) -> dict:
    """Return one normalized provider dataset profile or reject unsupported input."""
    if source is DataSource.CDS:
        return get_cds_era5_land_dataset_profile(source, dataset)
    raise ValueError(f"Unsupported source: {source.value}")
