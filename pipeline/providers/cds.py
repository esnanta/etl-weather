from __future__ import annotations

from pipeline.model import DataSource

VARIABLE_DESCRIPTIONS = {
    "tp": "total precipitation",
    "t2m": "2 metre temperature",
    "d2m": "2 metre dewpoint temperature",
    "swvl1": "volumetric soil water layer 1",
    "ssrd": "surface solar radiation downwards",
}

CDS_API_VARIABLE_NAMES = {
    "tp": "total_precipitation",
    "t2m": "2m_temperature",
    "d2m": "2m_dewpoint_temperature",
    "swvl1": "volumetric_soil_water_layer_1",
    "ssrd": "surface_solar_radiation_downwards",
}

CDS_API_TO_PROVIDER_VARIABLES = {
    api_name: provider_name
    for provider_name, api_name in CDS_API_VARIABLE_NAMES.items()
}

CDS_ERA5_LAND_VARIABLES = {
    "tp",
    "t2m",
    "d2m",
    "swvl1",
    "ssrd",
}

CDS_DATASET_PROFILES = {
    "reanalysis-era5-land": {
        "dataset_name": "copernicus-cds-era5-land",
        "api_dataset": "reanalysis-era5-land",
        "data_reference": "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land",
        "grid": "regular lat-lon",
        "horizontal_resolution": "0.1 deg",
        "horizontal_resolution_degrees": 0.1,
        "time_steps_utc": "00..23 (hourly)",
        "levels": "single level / land surface",
        "requires_product_type": False,
        "supported_variables": CDS_ERA5_LAND_VARIABLES,
    },
}

DATASET_PROFILES_BY_SOURCE = {
    DataSource.CDS: CDS_DATASET_PROFILES,
}


def get_source_dataset_profile(source: DataSource, dataset: str) -> dict:
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
