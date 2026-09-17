from datetime import datetime, timezone

import pandas as pd
import xarray as xr

from pipeline.model import (
    BoundingBox,
    DataSource,
    RawWeatherDataset,
    WeatherRequest,
)
from pipeline.storage import (
    artifact_exists,
    convert_netcdf_to_dataframe,
    save_raw_netcdf,
)


def test_save_raw_netcdf_and_convert_to_dataframe(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("pipeline.storage.DATA_DIRECTORY", tmp_path)

    request = WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date="2015-01-01",
        end_date="2015-01-01",
        variables=["tp"],
        bbox=BoundingBox(
            north_lat=5.0,
            west_lon=95.0,
            south_lat=5.0,
            east_lon=95.0,
        ),
    )

    dataset = xr.Dataset(
        {
            "tp": (
                ("valid_time", "latitude", "longitude"),
                [[[1.0]]],
            ),
        },
        coords={
            "valid_time": pd.to_datetime(["2015-01-01 00:00:00"]),
            "latitude": [5.0],
            "longitude": [95.0],
        },
    )

    raw_dataset = RawWeatherDataset(
        request=request,
        downloaded_at=datetime.now(timezone.utc),
        raw_data=dataset,
    )

    artifact = save_raw_netcdf(raw_dataset)

    assert artifact_exists(artifact)
    assert artifact.path.suffix == ".nc"
    assert artifact.path.parent == (
        tmp_path
        / "reanalysis-era5-land"
        / "raw"
        / "cds"
        / "2015"
    )
    assert artifact.path.name == "2015_0101_0101_tp.nc"

    loaded = convert_netcdf_to_dataframe(artifact)

    assert list(loaded.columns) == [
        "valid_time",
        "latitude",
        "longitude",
        "tp",
    ]
    assert len(loaded) == 1
    assert loaded.loc[0, "tp"] == 1.0
    assert loaded.loc[0, "latitude"] == 5.0
    assert loaded.loc[0, "longitude"] == 95.0
