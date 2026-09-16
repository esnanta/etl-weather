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


def test_normalize_cds_dataset_renames_short_variable_codes() -> None:
    dataset = xr.Dataset(
        {
            "tp": (("time", "point"), np.array([[1.0]], dtype=np.float32)),
            "t2m": (("time", "point"), np.array([[300.0]], dtype=np.float32)),
            "swvl1": (("time", "point"), np.array([[0.2]], dtype=np.float32)),
            "ssrd": (("time", "point"), np.array([[100.0]], dtype=np.float32)),
        },
        coords={
            "time": np.array(["2022-01-01T00:00:00"], dtype="datetime64[ns]"),
            "point": np.array([0], dtype=np.int32),
        },
    )

    normalized = _normalize_cds_dataset(dataset)

    assert "total_precipitation" in normalized.data_vars
    assert "2m_temperature" in normalized.data_vars
    assert "volumetric_soil_water_layer_1" in normalized.data_vars
    assert "surface_solar_radiation_downwards" in normalized.data_vars
    assert "tp" not in normalized.data_vars
    assert "t2m" not in normalized.data_vars
    assert "swvl1" not in normalized.data_vars
    assert "ssrd" not in normalized.data_vars
