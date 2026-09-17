from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pipeline.daily_climate_repository import (
    DailyClimateSelection,
    load_daily_climate_for_modelling,
)
from pipeline.model import DataSource
from scripts.config import (
    ACEH_LAND_DOMINANT_BBOX,
    CDS_DATASET,
    CDS_END_DATE,
    CDS_START_DATE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    selection = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name=CDS_DATASET,
        start_date=date.fromisoformat(CDS_START_DATE),
        end_date=date.fromisoformat(CDS_END_DATE),
        bbox=ACEH_LAND_DOMINANT_BBOX,
    )
    dataframe = load_daily_climate_for_modelling(selection)
    print(dataframe.head(10).to_string(index=False))
    print(f"\nrows={len(dataframe):,}")


if __name__ == "__main__":
    main()
