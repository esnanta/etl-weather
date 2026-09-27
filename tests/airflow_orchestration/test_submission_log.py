"""Check readable CDS pacing decisions without sending provider requests."""

import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from orchestration import pacing
from pipeline.extraction.cds import CdsSubmissionDeferred


@pytest.mark.parametrize("granted", [True, False])
def test_submission_log_fields_use_separate_lines(monkeypatch, caplog, granted):
    submitted = datetime(2026, 9, 27, 10, tzinfo=timezone.utc)
    next_allowed = datetime(2026, 9, 27, 10, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(pacing, "create_engine", lambda *args, **kwargs: SimpleNamespace(dispose=lambda: None))
    monkeypatch.setattr(pacing.DatabaseConfig, "from_env", lambda: SimpleNamespace(url="offline"))
    monkeypatch.setattr(pacing, "create_cds_pacing_table", lambda engine: None)
    monkeypatch.setattr(
        pacing, "reserve_cds_submission",
        lambda engine: SimpleNamespace(granted=granted, checked_at=submitted,
                                       next_allowed_at=next_allowed),
    )
    request = SimpleNamespace(start_date="2016-01-01", end_date="2016-01-31")
    with caplog.at_level(logging.INFO, logger="orchestration.pacing"):
        if granted:
            pacing.reserve_or_defer(request)
        else:
            with pytest.raises(CdsSubmissionDeferred):
                pacing.reserve_or_defer(request)
    message = caplog.records[-1].getMessage()
    assert message.splitlines() == (
        ["CDS submission reserved", "chunk=2016-01-01..2016-01-31",
         f"submitted_at={submitted}", f"next_allowed_at={next_allowed}"]
        if granted else
        ["CDS submission deferred", "chunk=2016-01-01..2016-01-31",
         f"next_allowed_at={next_allowed}"]
    )
