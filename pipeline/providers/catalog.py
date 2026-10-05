"""Resolve source/dataset profiles independently of provider transport."""

from __future__ import annotations

from pipeline.model import DataSource
from pipeline.providers.cds_era5_land import CDS_DATASET_PROFILES


DATASET_PROFILES_BY_SOURCE = {
    DataSource.CDS: CDS_DATASET_PROFILES,
}


def get_source_dataset_profile(source: DataSource, dataset: str) -> dict:
    """Return one normalized provider dataset profile or reject unsupported input."""
    source_profiles = DATASET_PROFILES_BY_SOURCE.get(source)
    if source_profiles is None:
        raise ValueError(f"Unsupported source: {source.value}")
    dataset_key = dataset.strip().lower()
    profile = source_profiles.get(dataset_key)
    if profile is None:
        raise ValueError(
            f"Unsupported dataset '{dataset_key}' for source '{source.value}'."
        )
    return profile
