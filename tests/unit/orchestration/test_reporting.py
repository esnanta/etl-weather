"""Keep campaign summaries bound to exact quarter identities and read-only.

Manual progress and alternative configurations must never inflate the
configured campaign completion count shown to operators.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from orchestration.campaign.policy import CampaignQuarter, iter_campaign_quarters
from config import CAMPAIGN_END_DATE, CAMPAIGN_START_DATE

CAMPAIGN_START_YEAR = CAMPAIGN_START_DATE.year
CAMPAIGN_END_YEAR = CAMPAIGN_END_DATE.year
from orchestration.campaign.reporting import MonthReport, QuarterReport, format_campaign_report, read_campaign_report


def test_report_queries_exact_quarter_definitions_without_writes() -> None:
    queries = []

    class Result:
        def mappings(self):
            return self

        @staticmethod
        def one_or_none():
            return None

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        @staticmethod
        def execute(statement, params):
            assert str(statement).lstrip().startswith("SELECT")
            queries.append(params)
            return Result()

    class Engine:
        @staticmethod
        def connect():
            return Connection()

    reports = read_campaign_report(Engine())
    quarter_count = len(iter_campaign_quarters())
    assert len(reports) == quarter_count
    assert all(report.status == "not_initialized" for report in reports)
    assert len({item["configuration_identity"] for item in queries}) == quarter_count
    assert queries[0]["start_date"] == f"{CAMPAIGN_START_YEAR}-01-01"
    assert queries[-1]["end_date"] == f"{CAMPAIGN_END_YEAR}-12-31"


def test_yearly_summary_counts_only_valid_complete_quarters() -> None:
    timestamp = datetime(2026, 9, 26, tzinfo=timezone.utc)
    failed_quarter = CampaignQuarter(CAMPAIGN_START_YEAR, 4)
    partial_quarter = CampaignQuarter(CAMPAIGN_END_YEAR, 2)
    complete_quarters = {
        CampaignQuarter(CAMPAIGN_START_YEAR, quarter)
        for quarter in (1, 2, 3)
    } - {partial_quarter}

    reports = tuple(
        QuarterReport(
            quarter,
            "running" if quarter == partial_quarter
            else "failed" if quarter == failed_quarter
            else "complete" if quarter in complete_quarters
            else "not_initialized",
            completed_months=2 if quarter == partial_quarter
            else 3 if quarter in complete_quarters else 0,
            failed_months=1 if quarter == failed_quarter else 0,
            last_error="ValidationError" if quarter == failed_quarter else None,
            last_load_at=timestamp if quarter == partial_quarter else None,
            last_loaded_month=date(partial_quarter.year, 6, 1)
            if quarter == partial_quarter else None,
            months=(
                MonthReport(
                    date(partial_quarter.year, 4, 1),
                    "pending",
                    "pending",
                    "pending",
                    "pending",
                ),
                MonthReport(
                    date(partial_quarter.year, 5, 1),
                    "complete",
                    "complete",
                    "complete",
                    "valid",
                ),
                MonthReport(
                    date(partial_quarter.year, 6, 1),
                    "complete",
                    "complete",
                    "complete",
                    "valid",
                ),
            ) if quarter == partial_quarter else (),
        )
        for quarter in iter_campaign_quarters()
    )

    quarter_count = len(iter_campaign_quarters())
    complete_count = len(complete_quarters)
    output = format_campaign_report(reports)
    assert (
        f"{complete_count}/{quarter_count} quarters complete; "
        f"{quarter_count - complete_count} remaining; 1 failed"
    ) in output
    assert (
        f"{CAMPAIGN_START_YEAR}: {sum(q.year == CAMPAIGN_START_YEAR for q in complete_quarters)}/4 "
        "quarters complete"
    ) in output
    assert (
        f"Failed {failed_quarter.label}: 1 month(s); reason=ValidationError"
    ) in output
    assert (
        f"Partial {partial_quarter.label}: 2/3 months complete; status=running"
    ) in output
    assert (
        f"{partial_quarter.year}-04: extract=pending, transform=pending, load=pending"
    ) in output
    assert (
        f"{partial_quarter.year}-06: extract=complete, transform=complete, load=complete"
    ) in output
    assert (
        f"Last recorded valid load: {partial_quarter.year}-06 "
        f"({partial_quarter.label}) at {timestamp.isoformat()}"
    ) in output


def test_partial_quarter_details_use_exact_parent_id() -> None:
    from orchestration.campaign.reporting import _load_quarter_report
    from orchestration.campaign.policy import (
        build_campaign_progress_definition,
        build_quarter_request_specs,
    )

    quarter = CampaignQuarter(CAMPAIGN_END_YEAR, 2)
    definition = build_campaign_progress_definition(
        build_quarter_request_specs(quarter, current_local_date=date.max)
    )
    parent = {
        "id": 42, "execution_status": "running", "validation_status": "pending",
        "completed_partition_count": 2, "failed_partition_count": 0,
        "last_error": None, "last_load_at": None, "last_loaded_month": None,
    }
    month = {
        "start_date": date(CAMPAIGN_END_YEAR, 4, 1), "extract_status": "pending",
        "transform_status": "pending", "load_status": "pending",
        "validation_status": "pending",
    }

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def mappings(self):
            return self

        def one_or_none(self):
            return self.rows[0] if self.rows else None

        def all(self):
            return self.rows

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        @staticmethod
        def execute(statement, params):
            if "pipeline_progress_detail\n" in str(statement):
                assert params == {"progress_id": 42}
                return Result([month])
            assert params["configuration_identity"] == definition.configuration_identity
            assert params["start_date"] == f"{CAMPAIGN_END_YEAR}-04-01"
            return Result([parent])

    class Engine:
        @staticmethod
        def connect():
            return Connection()

    report = _load_quarter_report(Engine(), quarter, definition)
    assert report.completed_months == 2
    assert report.months[0].start_date == date(CAMPAIGN_END_YEAR, 4, 1)
    assert report.months[0].load_status == "pending"
