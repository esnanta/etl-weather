"""Read the fixed campaign's exact progress identities for operator reporting.

Manual ranges and changed configurations can coexist in PostgreSQL, so a
yearly summary must match the forty current quarter definitions explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from orchestration.campaign import CampaignQuarter, build_quarter_request_specs, iter_campaign_quarters
from orchestration.progress import PipelineProgressDefinition, build_pipeline_progress_definition


@dataclass(frozen=True)
class QuarterReport:
    quarter: CampaignQuarter
    status: str
    completed_months: int = 0
    failed_months: int = 0
    last_error: str | None = None
    last_load_at: datetime | None = None


def _load_quarter_report(
    engine: Engine,
    quarter: CampaignQuarter,
    definition: PipelineProgressDefinition,
) -> QuarterReport:
    with engine.connect() as connection:
        row = connection.execute(
            text("""
                SELECT p.execution_status, p.validation_status,
                       p.completed_partition_count, p.failed_partition_count,
                       p.last_error,
                       (SELECT MAX(d.completed_at)
                        FROM climate.pipeline_progress_detail AS d
                        WHERE d.pipeline_progress_id = p.id
                          AND d.load_status = 'complete'
                          AND d.validation_status = 'valid') AS last_load_at
                FROM climate.pipeline_progress AS p
                WHERE p.pipeline_name = :pipeline_name AND p.source = :source
                  AND p.dataset_name = :dataset_name
                  AND p.start_date = :start_date AND p.end_date = :end_date
                  AND p.configuration_identity = :configuration_identity
            """),
            {
                "pipeline_name": definition.pipeline_name,
                "source": definition.source,
                "dataset_name": definition.dataset_name,
                "start_date": definition.start_date,
                "end_date": definition.end_date,
                "configuration_identity": definition.configuration_identity,
            },
        ).mappings().one_or_none()
    if row is None:
        return QuarterReport(quarter, "not_initialized")
    status = (
        "complete"
        if row["execution_status"] == "complete"
        and row["validation_status"] == "valid"
        else "pending_validation"
        if row["execution_status"] == "complete"
        else str(row["execution_status"])
    )
    return QuarterReport(
        quarter=quarter,
        status=status,
        completed_months=int(row["completed_partition_count"]),
        failed_months=int(row["failed_partition_count"]),
        last_error=row["last_error"],
        last_load_at=row["last_load_at"],
    )


def read_campaign_report(engine: Engine) -> tuple[QuarterReport, ...]:
    """Return current persisted rows without initializing or changing them."""
    reports = []
    for quarter in iter_campaign_quarters():
        specs = build_quarter_request_specs(quarter, current_local_date=datetime.max.date())
        definition = build_pipeline_progress_definition(specs)
        reports.append(_load_quarter_report(engine, quarter, definition))
    return tuple(reports)


def format_campaign_report(reports: tuple[QuarterReport, ...]) -> str:
    """Render one bounded yearly summary with failed quarters and last load."""
    quarters = iter_campaign_quarters()
    if tuple(item.quarter for item in reports) != quarters:
        raise ValueError("Campaign report must contain forty ordered quarters.")
    completed = sum(item.status == "complete" for item in reports)
    failed = sum(item.status == "failed" for item in reports)
    lines = [
        f"Campaign 2016-2025: {completed}/40 quarters complete; "
        f"{40 - completed} remaining; {failed} failed."
    ]
    for year in range(2016, 2026):
        yearly = [item for item in reports if item.quarter.year == year]
        count = sum(item.status == "complete" for item in yearly)
        lines.append(f"{year}: {count}/4 quarters complete")
    latest = max((item.last_load_at for item in reports if item.last_load_at), default=None)
    lines.append(f"Last recorded valid load: {latest.isoformat() if latest else 'none'}")
    for item in reports:
        if item.status == "failed":
            lines.append(
                f"Failed {item.quarter.label}: {item.failed_months} month(s); "
                f"reason={item.last_error or 'unknown'}"
            )
    return "\n".join(lines)
