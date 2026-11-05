"""Protect the nine-variable expansion nine-variable provider and request identity contract.

These offline tests verify CDS payload naming, provider normalization, and
artifact isolation without changing daily scientific transformation behavior.
"""

from dataclasses import replace

import numpy as np
import pytest
import xarray as xr

from config import CDS_VARIABLES
from pipeline.models.weather import BoundingBox, DataSource, WeatherRequest
from pipeline.providers.cds_era5_land import (
    CDS_API_VARIABLE_NAMES,
    build_cds_request_payload,
    normalize_cds_dataset,
)
from pipeline.requests.weather import (
    build_request,
    build_request_key,
    build_variable_signature,
)
from pipeline.storage.artifacts import (
    raw_artifact_for_request,
    transformed_artifact_for_request,
)


APPROVED_PROVIDER_VARIABLES = (
    "tp", "t2m", "d2m", "swvl1", "swvl2", "swvl3", "ssrd", "u10", "v10"
)
EXPECTED_SIGNATURE = "d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10"


def _request(variables: tuple[str, ...]) -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2024-01-01",
        end_date="2024-01-31",
        variables=list(variables),
        bbox=BoundingBox(
            north_lat=6.1,
            south_lat=1.9,
            east_lon=98.3,
            west_lon=94.9,
        ),
    )


def test_active_request_has_exactly_nine_approved_provider_variables() -> None:
    assert CDS_VARIABLES == APPROVED_PROVIDER_VARIABLES
    normalized = build_request(_request(CDS_VARIABLES))
    assert normalized.variables == list(APPROVED_PROVIDER_VARIABLES)


def test_payload_maps_all_nine_provider_variables_and_keeps_hourly_contract() -> None:
    payload = build_cds_request_payload(build_request(_request(CDS_VARIABLES)))

    assert payload["variable"] == [
        "total_precipitation",
        "2m_temperature",
        "2m_dewpoint_temperature",
        "volumetric_soil_water_layer_1",
        "volumetric_soil_water_layer_2",
        "volumetric_soil_water_layer_3",
        "surface_solar_radiation_downwards",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
    ]
    assert payload["time"] == [f"{hour:02d}:00" for hour in range(24)]
    assert payload["area"] == [6.1, 94.9, 1.9, 98.3]
    assert payload["data_format"] == "netcdf"
    assert payload["download_format"] == "unarchived"


def test_normalization_maps_all_nine_cds_names_to_provider_codes() -> None:
    source_variables = {
        CDS_API_VARIABLE_NAMES[code]: (
            ("time", "latitude", "longitude"),
            np.full((1, 1, 1), float(index)),
        )
        for index, code in enumerate(APPROVED_PROVIDER_VARIABLES)
    }
    dataset = xr.Dataset(
        source_variables,
        coords={
            "time": np.array(["2024-01-01T00:00:00"], dtype="datetime64[ns]"),
            "latitude": [6.1],
            "longitude": [94.9],
        },
    )
    normalized = normalize_cds_dataset(dataset)

    assert set(normalized.data_vars) == set(APPROVED_PROVIDER_VARIABLES)
    assert "valid_time" in normalized.coords
    assert "time" not in normalized.coords
    for index, code in enumerate(APPROVED_PROVIDER_VARIABLES):
        assert normalized[code].item() == pytest.approx(float(index))


def test_request_builder_rejects_duplicate_and_unknown_variables() -> None:
    with pytest.raises(ValueError, match="Duplicate variables"):
        build_request(_request((*CDS_VARIABLES, "u10")))
    with pytest.raises(ValueError, match="not supported"):
        build_request(_request((*CDS_VARIABLES, "unsupported_variable")))
    with pytest.raises(ValueError, match="not supported"):
        build_request(_request((*CDS_VARIABLES, "wind_speed")))


def test_nine_variable_signature_and_request_key_are_deterministic() -> None:
    current = build_request(_request(CDS_VARIABLES))
    reversed_request = replace(current, variables=list(reversed(current.variables)))
    old = build_request(_request(("tp", "t2m", "d2m", "swvl1", "ssrd")))

    assert build_variable_signature(current.variables) == EXPECTED_SIGNATURE
    assert build_variable_signature(reversed_request.variables) == EXPECTED_SIGNATURE
    assert build_request_key(current) == build_request_key(reversed_request)
    assert build_request_key(current) != build_request_key(old)


def test_old_and_new_request_artifact_paths_are_isolated(tmp_path) -> None:
    current = build_request(_request(CDS_VARIABLES))
    old = build_request(_request(("tp", "t2m", "d2m", "swvl1", "ssrd")))

    current_raw = raw_artifact_for_request(current, data_directory=tmp_path)
    old_raw = raw_artifact_for_request(old, data_directory=tmp_path)
    current_parquet = transformed_artifact_for_request(
        current, data_directory=tmp_path
    )
    old_parquet = transformed_artifact_for_request(
        old, data_directory=tmp_path
    )

    assert current_raw.path.name == (
        f"2024_0101_0131_{EXPECTED_SIGNATURE}.nc"
    )
    assert current_parquet.path.name == (
        f"2024_0101_0131_{EXPECTED_SIGNATURE}.parquet"
    )
    assert current_raw.path != old_raw.path
    assert current_parquet.path != old_parquet.path
