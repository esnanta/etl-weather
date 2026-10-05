import logging

import pandas as pd

import pipeline.observability.logging as pipeline_logging
from pipeline.observability.logging import (
    log_metric,
    log_section_completed,
    log_section_start,
)

build_key_value_block = getattr(pipeline_logging, "_build_key_value_block")
log_dataframe_preview = getattr(pipeline_logging, "_log_dataframe_preview")


def test_key_value_block_has_no_ascii_table_borders() -> None:
    block = build_key_value_block(
        {
            "dataset": "reanalysis-era5-land",
            "variables": ["tp", "t2m"],
            "units": {"tp": "mm/day", "t2m": "degC"},
        }
    )

    assert "+" not in block
    assert "|" not in block
    assert "dataset   : reanalysis-era5-land" in block
    assert "variables : tp, t2m" in block
    assert "units     : tp=mm/day, t2m=degC" in block


def test_metric_blocks_end_with_a_blank_line(caplog) -> None:
    with caplog.at_level(logging.INFO, logger="pipeline.observability.logging"):
        log_metric("Example", {"status": "ready"})

    assert "[METRIC] Example\nstatus : ready\n" in caplog.text


def test_completed_section_separates_the_next_process(caplog) -> None:
    with caplog.at_level(logging.INFO, logger="pipeline.observability.logging"):
        started_at = log_section_start("EXTRACT")
        log_section_completed("EXTRACT", started_at)
        log_section_start("TRANSFORM")

    messages = [record.getMessage() for record in caplog.records]
    assert messages[0] == "[START] EXTRACT"
    assert messages[1].startswith("[DONE] EXTRACT completed")
    assert messages[1].endswith("\n")
    assert messages[2] == "[START] TRANSFORM"


def test_dataframe_preview_is_hidden_at_info_and_available_at_debug(caplog) -> None:
    dataframe = pd.DataFrame({"value": [1]})

    with caplog.at_level(logging.INFO, logger="pipeline.observability.logging"):
        log_dataframe_preview(dataframe)
    assert "[PREVIEW]" not in caplog.text

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="pipeline.observability.logging"):
        log_dataframe_preview(dataframe)
    assert "[PREVIEW]" in caplog.text
