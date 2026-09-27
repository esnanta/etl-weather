"""Keep campaign summaries bound to exact quarter identities and read-only.

Manual progress and alternative configurations must never inflate the ten-year
completion count shown to operators.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from orchestration.campaign import CampaignQuarter, iter_campaign_quarters
from orchestration.reporting import MonthReport, QuarterReport, format_campaign_report, read_campaign_report


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
            else "running" if quarter == CampaignQuarter(2024, 2)
            else "not_initialized",
            completed_months=3 if quarter.quarter <= 3 and quarter.year == 2016
            else 2 if quarter == CampaignQuarter(2024, 2) else 0,
            failed_months=1 if quarter == CampaignQuarter(2016, 4) else 0,
            last_error="ValidationError" if quarter == CampaignQuarter(2016, 4) else None,
            last_load_at=timestamp if quarter == CampaignQuarter(2024, 2) else None,
            last_loaded_month=date(2024, 6, 1) if quarter == CampaignQuarter(2024, 2) else None,
            months=(
                MonthReport(date(2024, 4, 1), "pending", "pending", "pending", "pending"),
                MonthReport(date(2024, 5, 1), "complete", "complete", "complete", "valid"),
                MonthReport(date(2024, 6, 1), "complete", "complete", "complete", "valid"),
            ) if quarter == CampaignQuarter(2024, 2) else (),
        )
        for quarter in iter_campaign_quarters()
    )
    output = format_campaign_report(reports)
    assert "3/40 quarters complete; 37 remaining; 1 failed" in output
    assert "2016: 3/4 quarters complete" in output
    assert "2017: 0/4 quarters complete" in output
    assert "Failed 2016 Q4: 1 month(s); reason=ValidationError" in output
    assert "Partial 2024 Q2: 2/3 months complete; status=running" in output
    assert "2024-04: extract=pending, transform=pending, load=pending" in output
    assert "2024-06: extract=complete, transform=complete, load=complete" in output
    assert f"Last recorded valid load: 2024-06 (2024 Q2) at {timestamp.isoformat()}" in output


def test_partial_quarter_details_use_exact_parent_id() -> None:
    from orchestration.reporting import _load_quarter_report
    from orchestration.campaign import build_quarter_request_specs
    from orchestration.progress import build_pipeline_progress_definition

    quarter = CampaignQuarter(2024, 2)
    definition = build_pipeline_progress_definition(
        build_quarter_request_specs(quarter, current_local_date=date.max)
    )
    parent = {
        "id": 42, "execution_status": "running", "validation_status": "pending",
        "completed_partition_count": 2, "failed_partition_count": 0,
        "last_error": None, "last_load_at": None, "last_loaded_month": None,
    }
    month = {
        "start_date": date(2024, 4, 1), "extract_status": "pending",
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

        def execute(self, statement, params):
            if "pipeline_progress_detail\n" in str(statement):
                assert params == {"progress_id": 42}
                return Result([month])
            assert params["configuration_identity"] == definition.configuration_identity
            assert params["start_date"] == "2024-04-01"
            return Result([parent])

    class Engine:
        def connect(self):
            return Connection()

    report = _load_quarter_report(Engine(), quarter, definition)
    assert report.completed_months == 2
    assert report.months[0].start_date == date(2024, 4, 1)
    assert report.months[0].load_status == "pending"
