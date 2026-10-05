from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pipeline.repositories.daily_climate_reader import (
    DailyClimateSelection,
    load_daily_climate_for_modelling,
)
from pipeline.models.weather import DataSource
from config import (
    CDS_DATASET,
    resolve_cds_period,
    validate_and_log_app_environment,
)
from pipeline.requests.factory import build_aceh_bbox

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def _format_value(value: Any) -> str:
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
    try:
        validate_and_log_app_environment(logger)
    except ValueError:
        raise SystemExit(2) from None
    start_date, end_date = resolve_cds_period()
    selection = DailyClimateSelection(
        source=DataSource.CDS,
        dataset_name=CDS_DATASET,
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
        bbox=build_aceh_bbox(),
    )
    dataframe = load_daily_climate_for_modelling(selection)
    print(_build_dataframe_ascii_table(dataframe, rows=10))
    print(f"\nrows={len(dataframe):,}")


if __name__ == "__main__":
    main()
