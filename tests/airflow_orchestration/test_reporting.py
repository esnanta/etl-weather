"""Keep campaign summaries bound to exact quarter identities and read-only.

Manual progress and alternative configurations must never inflate the ten-year
completion count shown to operators.
"""

from __future__ import annotations

from datetime import datetime, timezone

from orchestration.campaign import CampaignQuarter, iter_campaign_quarters
from orchestration.reporting import QuarterReport, format_campaign_report, read_campaign_report


def test_report_queries_exact_quarter_definitions_without_writes() -> None:
    queries = []

    class Result:
        def mappings(self):
            return self

        def one_or_none(self):
            return None

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement, params):
            assert str(statement).lstrip().startswith("SELECT")
            queries.append(params)
            return Result()

    class Engine:
        def connect(self):
            return Connection()

    reports = read_campaign_report(Engine())
    assert len(reports) == 40
    assert all(report.status == "not_initialized" for report in reports)
    assert len({item["configuration_identity"] for item in queries}) == 40
    assert queries[0]["start_date"] == "2016-01-01"
    assert queries[-1]["end_date"] == "2025-12-31"


def test_yearly_summary_counts_only_valid_complete_quarters() -> None:
    timestamp = datetime(2026, 9, 26, tzinfo=timezone.utc)
    reports = tuple(
        QuarterReport(
            quarter,
            "complete" if quarter.quarter <= 3 and quarter.year == 2016
            else "failed" if quarter == CampaignQuarter(2016, 4)
            else "not_initialized",
            completed_months=3 if quarter.quarter <= 3 and quarter.year == 2016 else 0,
            failed_months=1 if quarter == CampaignQuarter(2016, 4) else 0,
            last_error="ValidationError" if quarter == CampaignQuarter(2016, 4) else None,
            last_load_at=timestamp if quarter == CampaignQuarter(2016, 3) else None,
        )
        for quarter in iter_campaign_quarters()
    )
    output = format_campaign_report(reports)
    assert "3/40 quarters complete; 37 remaining; 1 failed" in output
    assert "2016: 3/4 quarters complete" in output
    assert "2017: 0/4 quarters complete" in output
    assert "Failed 2016 Q4: 1 month(s); reason=ValidationError" in output
    assert timestamp.isoformat() in output
