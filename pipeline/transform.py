from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)

import logging
import pandas as pd

from pipeline.model import (
    LoadedWeatherDataset
)

from pipeline.interfaces import DataTransformer

from pipeline.logging import (
    _log_dataframe_preview,
    log_dataframe_changes,
    log_dataframe_info,
    log_metric,
)
from pipeline.model import TransformedWeatherDataset

logger = logging.getLogger(__name__)


def _transform_dataset(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply transformations to a weather DataFrame.
    """

    log_metric(
        "Applying transformation",
        {
            "drop_columns": ["number", "surface"],
        },
    )

    transformed = dataframe.copy()

    transformed = transformed.drop(
        columns=[
            "number",
            "surface",
        ],
        errors="ignore",
    )

    log_dataframe_changes(
        before=dataframe,
        after=transformed,
        step="Transformation",
    )

    return transformed


def transform_data(
    raw_dataset: LoadedWeatherDataset,
) -> TransformedWeatherDataset:

    log_dataframe_info(
        raw_dataset.raw_data,
        title="Transformation input",
    )

    transformed = _transform_dataset(
        raw_dataset.raw_data,
    )

    log_dataframe_info(
        transformed,
        title="Transformation output",
    )

    _log_dataframe_preview(
        transformed,
    )

    return TransformedWeatherDataset(
        request=raw_dataset.request,
        transformed_at=datetime.now(UTC),
        dataset=transformed,
    )

class DefaultDataTransformer(DataTransformer):
    """
    SOLID: Single Responsibility & Dependency Inversion.
    Transforms data according to basic pipeline rules.
    """
    def transform(self, raw_dataset: LoadedWeatherDataset) -> TransformedWeatherDataset:
        return transform_data(raw_dataset)
