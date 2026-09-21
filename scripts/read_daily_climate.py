from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pipeline.repositories.daily_climate import (
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


def _format_value(value: object) -> str:
    if pd.isna(value):
        return "NaN"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def _build_dataframe_ascii_table(dataframe: pd.DataFrame, rows: int = 10) -> str:
    preview = dataframe.head(rows)
    headers = [str(column) for column in preview.columns]
    body = [
        [_format_value(value) for value in row]
        for row in preview.itertuples(index=False, name=None)
    ]

    widths = [
        max(
            len(headers[index]),
            *(len(row[index]) for row in body),
        )
        for index in range(len(headers))
    ]

    border = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    header_row = "| " + " | ".join(
        header.ljust(width) for header, width in zip(headers, widths)
    ) + " |"
    body_rows = "\n".join(
        "| " + " | ".join(
            value.ljust(width) for value, width in zip(row, widths)
        ) + " |"
        for row in body
    )

    return f"{border}\n{header_row}\n{border}\n{body_rows}\n{border}"


def main() -> None:
    selection = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name=CDS_DATASET,
        start_date=date.fromisoformat(CDS_START_DATE),
        end_date=date.fromisoformat(CDS_END_DATE),
        bbox=ACEH_LAND_DOMINANT_BBOX,
    )
    dataframe = load_daily_climate_for_modelling(selection)
    print(_build_dataframe_ascii_table(dataframe, rows=10))
    print(f"\nrows={len(dataframe):,}")


if __name__ == "__main__":
    main()
