# tests/unit/test_storage.py

import tempfile

import xarray as xr

from pathlib import Path

from pipeline.storage import (
    save_dataset,
    load_dataset,
    artifact_exists,
    DataLayer,
)


def test_save_and_load_dataset():

    dataset = xr.Dataset(
        {
            "tp": (
                (
                    "time",
                    "values",
                ),
                [[1.0]],
            ),
        },
        coords={
            "time": [
                "2015-01-01",
            ],
            "values": [
                0,
            ],
            "latitude": (
                "values",
                [5.0],
            ),
            "longitude": (
                "values",
                [95.0],
            ),
        },
    )

    with tempfile.TemporaryDirectory() as tmp:

        artifact = save_dataset(
            dataset=dataset,
            dataset_name="tp",
            layer=DataLayer.RAW,
            data_directory=Path(tmp),
        )

        assert artifact_exists(
            artifact,
        )

        loaded = load_dataset(
            artifact,
        )

        assert "tp" in (
            loaded.data_vars
        )

        assert (
            loaded.sizes["time"]
            == 1
        )

        assert (
            loaded.sizes["values"]
            == 1
        )
