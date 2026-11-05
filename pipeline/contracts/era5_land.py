from __future__ import annotations

import pandas as pd

ERA5_LAND_DATASET = "reanalysis-era5-land"
ERA5_LAND_START_DATE = "1950-01-01"
ERA5_LAND_FIRST_LOCAL_DAY = pd.Timestamp(ERA5_LAND_START_DATE)

LOCAL_TIMEZONE = "Asia/Jakarta"
LOCAL_UTC_OFFSET_HOURS = 7
LOCAL_MIDNIGHT_IN_UTC_HOUR = (24 - LOCAL_UTC_OFFSET_HOURS) % 24
LOCAL_DAY_CONTEXT_DAYS = 1
EXPECTED_HOURLY_SAMPLES_PER_DAY = 24

ERA5_LAND_ACCUMULATED_VARIABLES = {"tp", "ssrd"}
ERA5_LAND_DAILY_MEAN_VARIABLES = {
    "t2m", "d2m", "swvl1", "swvl2", "swvl3", "u10", "v10"
}
WIND_COMPONENT_VARIABLES = frozenset({"u10", "v10"})
WIND_DERIVED_VARIABLES = ("wind_speed", "wind_direction")
# Resultant speed below this threshold has no meaningful direction (m/s).
WIND_DIRECTION_CALM_TOLERANCE_M_S = 1e-10

ERA5_LAND_OUTPUT_UNITS = {
    "tp": "mm/day",
    "t2m": "degC",
    "d2m": "degC",
    "swvl1": "m3/m3",
    "swvl2": "m3/m3",
    "swvl3": "m3/m3",
    "ssrd": "J/m2/day",
    "u10": "m/s",
    "v10": "m/s",
    "wind_speed": "m/s",
    "wind_direction": "deg",
}
