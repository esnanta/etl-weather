from __future__ import annotations

import functools
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

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


def log_section_start(section: str) -> float:
    """Log section start and return monotonic start time."""
    logger.info("[START] %s", section)
    return time.perf_counter()


def log_section_completed(section: str, started_at: float) -> None:
    """Log section completion with duration."""
    elapsed_time = time.perf_counter() - started_at
    logger.info("[DONE] %s completed | duration=%.2fs", section, elapsed_time)


def log_metric(title: str, metrics: Mapping[str, Any]) -> None:
    """Log a metric block in a vertical readable format."""
    metric_lines = "\n".join(
        f"  - {key}: {value}"
        for key, value in metrics.items()
    )
    logger.info("[METRIC] %s\n%s", title, metric_lines)


def log_pipeline_header(request) -> None:
    """Log one headline for a pipeline run."""
    logger.info(
        "[PIPELINE] dataset=%s | source=%s | period=%s -> %s | variables=%s",
        request.dataset,
        request.source.value,
        request.start_date,
        request.end_date,
        ", ".join(request.variables),
    )


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
        "[PIPELINE] completed | dataset=%s | source=%s | period=%s -> %s",
        request.dataset,
        request.source.value,
        request.start_date,
        request.end_date,
    )
    log_metric(
        "Pipeline summary",
        {
            "rows_inserted": f"{rows_inserted:,}",
            "dataset_id": dataset_id,
            "parquet_file": parquet_path.name,
            "parquet_size_mb": f"{file_size_mb:.2f}",
            "duration_seconds": f"{duration:.2f}",
        },
    )



def log_dataframe_info(
    dataframe: pd.DataFrame,
    *,
    title: str,
) -> None:
    """
    Log basic DataFrame information.
    """

    log_metric(
        title,
        {
            "rows": len(dataframe),
            "cols": len(dataframe.columns),
            "columns": list(dataframe.columns),
        },
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

    log_metric(
        step,
        {
            "rows_before": len(before),
            "rows_after": len(after),
            "cols_before": len(before.columns),
            "cols_after": len(after.columns),
            "removed": removed,
            "added": added,
        },
    )
