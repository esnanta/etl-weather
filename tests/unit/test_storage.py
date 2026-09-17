import tempfile
from pathlib import Path

import xarray as xr

from pipeline.storage import DataLayer, artifact_exists, load_dataset, save_dataset


def test_save_and_load_dataset() -> None:
    dataset = xr.Dataset(
        {
            "tp": (
                ("valid_time", "latitude", "longitude"),
                [[[1.0]]],
            ),
        },
        coords={
            "valid_time": ["2015-01-01"],
            "latitude": [5.0],
            "longitude": [95.0],
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        artifact = save_dataset(
            dataset=dataset,
            dataset_name="tp",
            layer=DataLayer.RAW,
            data_directory=Path(tmp),
        )
        assert artifact_exists(artifact)
        loaded = load_dataset(artifact)
        assert "tp" in loaded.data_vars
        assert loaded.sizes["valid_time"] == 1
        assert loaded.sizes["latitude"] == 1
        assert loaded.sizes["longitude"] == 1
