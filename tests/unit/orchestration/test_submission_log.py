"""Check readable CDS pacing decisions without sending provider requests."""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from config import CDS_DATASET, CDS_VARIABLES
from orchestration.scheduling import submission_gate as pacing
from orchestration.scheduling.submission_gate import SubmissionDeferred
from pipeline.models.weather import DataSource, WeatherRequest
from pipeline.requests.factory import build_aceh_bbox


@pytest.mark.parametrize("granted", [True, False])
def test_submission_log_fields_use_separate_lines(monkeypatch, caplog, granted):
    submitted = datetime(2026, 9, 27, 10, tzinfo=timezone.utc)
    next_allowed = datetime(2026, 9, 27, 10, 30, tzinfo=timezone.utc)
    @contextmanager
    def engine():
        yield object()

    monkeypatch.setattr(
        "orchestration.scheduling.submission_gate.runtime_database_engine",
        engine,
    )
    monkeypatch.setattr(
        "orchestration.scheduling.submission_gate.initialize_pacing_storage",
        lambda engine: None,
    )
    monkeypatch.setattr(
        "orchestration.scheduling.submission_gate.reserve_submission",
        lambda engine: SimpleNamespace(granted=granted, checked_at=submitted,
                                       next_allowed_at=next_allowed),
    )
    request = WeatherRequest(
        source=DataSource.CDS,
        dataset=CDS_DATASET,
        start_date="2016-01-01",
        end_date="2016-01-31",
        variables=list(CDS_VARIABLES),
        bbox=build_aceh_bbox(),
    )
    with caplog.at_level(logging.INFO, logger="orchestration.scheduling.submission_gate"):
        if granted:
            pacing.reserve_or_defer(request)
        else:
            with pytest.raises(SubmissionDeferred):
                pacing.reserve_or_defer(request)
    message = caplog.records[-1].getMessage()
    assert message.splitlines() == (
        ["CDS submission reserved", "chunk=2016-01-01..2016-01-31",
         f"submitted_at={submitted}", f"next_allowed_at={next_allowed}"]
        if granted else
        ["CDS submission deferred", "chunk=2016-01-01..2016-01-31",
         f"next_allowed_at={next_allowed}"]
    )
