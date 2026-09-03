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
    """Log a metric block using an ASCII table for compact readability."""
    table = _build_ascii_table(metrics)
    logger.info("[METRIC] %s\n%s\n", title, table)


def _build_ascii_table(metrics: Mapping[str, Any]) -> str:
    """Build an ASCII key-value table."""
    headers = ("field", "value")
    rows = [
        (
            str(key),
            str(value),
        )
        for key, value in metrics.items()
    ]

    field_width = max(
        len(headers[0]),
        *(len(field) for field, _ in rows),
    )
    value_width = max(
        len(headers[1]),
        *(len(value) for _, value in rows),
    )

    border = (
        f"+-{'-' * field_width}-+-{'-' * value_width}-+"
    )
    header_row = (
        f"| {headers[0].ljust(field_width)} | {headers[1].ljust(value_width)} |"
    )
    body_rows = "\n".join(
        f"| {field.ljust(field_width)} | {value.ljust(value_width)} |"
        for field, value in rows
    )

    table = (
        f"{border}\n"
        f"{header_row}\n"
        f"{border}\n"
        f"{body_rows}\n"
        f"{border}"
    )
    return table


def log_pipeline_header(request) -> None:
    """Log one headline for a pipeline run."""
    table = _build_ascii_table(
        {
            "dataset": request.dataset,
            "source": request.source.value,
            "period": f"{request.start_date} -> {request.end_date}",
            "variables": ", ".join(request.variables),
        },
    )
    logger.info(
        "[PIPELINE] start\n%s\n",
        table,
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

    completion_table = _build_ascii_table(
        {
            "dataset": request.dataset,
            "source": request.source.value,
            "period": f"{request.start_date} -> {request.end_date}",
            "status": "completed",
        },
    )
    logger.info("[PIPELINE] completed\n%s\n", completion_table)
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
