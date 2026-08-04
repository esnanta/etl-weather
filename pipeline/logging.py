from __future__ import annotations

import functools
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


def log_step(
    func: Callable,
) -> Callable:
    """
    A decorator to log the execution
    time of a pipeline step.
    """

    @functools.wraps(func)
    def wrapper(
        *args: Any,
        **kwargs: Any,
    ) -> Any:

        logger.info(
            "[START] %s",
            func.__name__,
        )

        start_time = time.perf_counter()

        result = func(
            *args,
            **kwargs,
        )

        elapsed_time = (
            time.perf_counter()
            - start_time
        )

        logger.info(
            "[DONE] %s | Duration: %.2fs",
            func.__name__,
            elapsed_time,
        )

        return result

    return wrapper


def _log_dataframe_preview(
    dataframe: pd.DataFrame,
    rows: int = 10,
) -> None:
    """
    Log the first rows of a DataFrame.
    """

    logger.info(
        "[PREVIEW]\n%s",
        dataframe.head(rows).to_string(
            index=False,
        ),
    )


@log_step
def log_pipeline_summary(
    *,
    request,
    dataset_id: int,
    rows_inserted: int,
    parquet_path: Path,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """
    Log the final summary of one pipeline execution.
    """

    duration = (
        finished_at
        - started_at
    ).total_seconds()

    file_size_mb = (
        parquet_path.stat().st_size
        / (1024 * 1024)
    )

    logger.info(
        "\n"
        "============================================================\n"
        "                 PIPELINE EXECUTION SUMMARY\n"
        "============================================================\n"
        "Status           : SUCCESS\n"
        "Dataset          : %s\n"
        "Source           : %s\n"
        "Period           : %s -> %s\n"
        "Variables        : %s\n"
        "\n"
        "Rows Inserted    : %s\n"
        "Dataset ID       : %d\n"
        "\n"
        "Parquet File     : %s\n"
        "Parquet Size     : %.2f MB\n"
        "\n"
        "Started At       : %s\n"
        "Finished At      : %s\n"
        "Duration         : %.2f s\n"
        "============================================================",
        request.dataset,
        request.source.value,
        request.start_date,
        request.end_date,
        ", ".join(request.variables),
        f"{rows_inserted:,}",
        dataset_id,
        parquet_path.name,
        file_size_mb,
        started_at.strftime("%Y-%m-%d %H:%M:%S"),
        finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        duration,
    )



def log_dataframe_info(
    dataframe: pd.DataFrame,
    *,
    title: str,
) -> None:
    """
    Log basic DataFrame information.
    """

    logger.info(
        "[METRIC] %s | "
        "rows=%d | "
        "cols=%d | "
        "columns=%s",
        title,
        len(dataframe),
        len(dataframe.columns),
        list(dataframe.columns),
    )


def log_dataframe_changes(
    *,
    before: pd.DataFrame,
    after: pd.DataFrame,
    step: str,
) -> None:
    """
    Log structural changes between two DataFrames.
    """

    before_columns = set(before.columns)
    after_columns = set(after.columns)

    removed = sorted(
        before_columns - after_columns
    )

    added = sorted(
        after_columns - before_columns
    )

    logger.info(
        "[METRIC] %s | "
        "rows=%d -> %d | "
        "cols=%d -> %d | "
        "removed=%s | "
        "added=%s",
        step,
        len(before),
        len(after),
        len(before.columns),
        len(after.columns),
        removed,
        added,
    )