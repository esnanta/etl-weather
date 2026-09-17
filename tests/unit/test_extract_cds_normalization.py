from __future__ import annotations

import numpy as np
import xarray as xr

from pipeline.extract import _normalize_cds_dataset


def test_normalize_cds_dataset_renames_time_coordinate() -> None:
    dataset = xr.Dataset(
        {"tp": (("time", "point"), np.array([[1.0]], dtype=np.float32))},
        coords={
            "time": np.array(["2022-01-01T00:00:00"], dtype="datetime64[ns]"),
            "point": np.array([0], dtype=np.int32),
        },
    )

    normalized = _normalize_cds_dataset(dataset)
    assert "valid_time" in normalized.coords
    assert "time" not in normalized.coords
    assert "tp" in normalized.data_vars


def test_normalize_cds_dataset_converts_api_names_back_to_provider_codes() -> None:
    dataset = xr.Dataset(
        {
            "total_precipitation": (("time", "point"), np.array([[1.0]], dtype=np.float32)),
            "2m_temperature": (("time", "point"), np.array([[300.0]], dtype=np.float32)),
            "volumetric_soil_water_layer_1": (("time", "point"), np.array([[0.2]], dtype=np.float32)),
            "surface_solar_radiation_downwards": (("time", "point"), np.array([[100.0]], dtype=np.float32)),
            "2m_dewpoint_temperature": (("time", "point"), np.array([[295.0]], dtype=np.float32)),
        },
        coords={
            "time": np.array(["2022-01-01T00:00:00"], dtype="datetime64[ns]"),
            "point": np.array([0], dtype=np.int32),
        },
    )

    normalized = _normalize_cds_dataset(dataset)

    for name in ("tp", "t2m", "swvl1", "ssrd", "d2m"):
        assert name in normalized.data_vars
    assert "total_precipitation" not in normalized.data_vars
    assert "2m_temperature" not in normalized.data_vars
