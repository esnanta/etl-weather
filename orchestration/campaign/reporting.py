"""Read configured campaign progress identities for operator reporting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from orchestration.campaign.policy import (
    CampaignQuarter,
    build_campaign_progress_definition,
    build_quarter_request_specs,
    iter_campaign_quarters,
)
from config import CAMPAIGN_END_DATE, CAMPAIGN_START_DATE
from database.config import (
    PIPELINE_PROGRESS_DETAIL_TABLE,
    PIPELINE_PROGRESS_SCHEMA,
    PIPELINE_PROGRESS_TABLE,
)
from orchestration.progress.definition import PipelineProgressDefinition


@dataclass(frozen=True)
class MonthReport:
    start_date: date
    extract_status: str
    transform_status: str
    load_status: str
    validation_status: str


@dataclass(frozen=True)
class QuarterReport:
    quarter: CampaignQuarter
    status: str
    completed_months: int = 0
    failed_months: int = 0
    last_error: str | None = None
    last_load_at: datetime | None = None
    last_loaded_month: date | None = None
    months: tuple[MonthReport, ...] = ()


def _load_quarter_report(
    engine: Engine,
    quarter: CampaignQuarter,
    definition: PipelineProgressDefinition,
) -> QuarterReport:
    parent_table = f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE}"
    detail_table = f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}"
    with engine.connect() as connection:
        row = connection.execute(
            text(f"""
                SELECT p.id, p.execution_status, p.validation_status,
                       p.completed_partition_count, p.failed_partition_count,
                       p.last_error, latest.last_load_at,
                       latest.last_loaded_month
                FROM {parent_table} AS p
                LEFT JOIN LATERAL (
                    SELECT d.completed_at AS last_load_at,
                           d.start_date AS last_loaded_month
                    FROM {detail_table} AS d
                    WHERE d.pipeline_progress_id = p.id
                      AND d.load_status = 'complete'
                      AND d.validation_status = 'valid'
                      AND d.completed_at IS NOT NULL
                    ORDER BY d.completed_at DESC
                    LIMIT 1
                ) AS latest ON TRUE
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
        month_rows = connection.execute(
            text(f"""
                SELECT start_date, extract_status, transform_status,
                       load_status, validation_status
                FROM {detail_table}
                WHERE pipeline_progress_id = :progress_id
                ORDER BY start_date
            """),
            {"progress_id": row["id"]},
        ).mappings().all()
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
        last_loaded_month=row["last_loaded_month"],
        months=tuple(
            MonthReport(
                start_date=month["start_date"],
                extract_status=str(month["extract_status"]),
                transform_status=str(month["transform_status"]),
                load_status=str(month["load_status"]),
                validation_status=str(month["validation_status"]),
            )
            for month in month_rows
        ),
    )


def read_campaign_report(engine: Engine) -> tuple[QuarterReport, ...]:
    reports = []
    for quarter in iter_campaign_quarters():
        specs = build_quarter_request_specs(quarter, current_local_date=datetime.max.date())
        definition = build_campaign_progress_definition(specs)
        reports.append(_load_quarter_report(engine, quarter, definition))
    return tuple(reports)


def format_campaign_report(reports: tuple[QuarterReport, ...]) -> str:
    quarters = iter_campaign_quarters()
    if tuple(item.quarter for item in reports) != quarters:
        raise ValueError("Campaign report must contain all configured ordered quarters.")
    completed = sum(item.status == "complete" for item in reports)
    failed = sum(item.status == "failed" for item in reports)
    lines = [
        f"Campaign {CAMPAIGN_START_DATE.isoformat()} -> "
        f"{CAMPAIGN_END_DATE.isoformat()}: "
        f"{completed}/{len(quarters)} quarters complete; "
        f"{len(quarters) - completed} remaining; {failed} failed."
    ]
    for year in sorted({quarter.year for quarter in quarters}):
        yearly = [item for item in reports if item.quarter.year == year]
        count = sum(item.status == "complete" for item in yearly)
        lines.append(f"{year}: {count}/{len(yearly)} quarters complete")
    for item in reports:
        if item.status != "complete" and item.completed_months:
            lines.append(
                f"Partial {item.quarter.label}: {item.completed_months}/3 months complete; "
                f"status={item.status}"
            )
            for month in item.months:
                lines.append(
                    f"  {month.start_date.isoformat()[:7]}: "
                    f"extract={month.extract_status}, "
                    f"transform={month.transform_status}, "
                    f"load={month.load_status}, "
                    f"validation={month.validation_status}"
                )
    latest = max(
        (item for item in reports if item.last_load_at),
        key=lambda item: item.last_load_at,
        default=None,
    )
    if latest is None:
        lines.append("Last recorded valid load: none")
    else:
        month = latest.last_loaded_month.isoformat()[:7] if latest.last_loaded_month else "unknown"
        lines.append(
            f"Last recorded valid load: {month} ({latest.quarter.label}) at "
            f"{latest.last_load_at.isoformat()}"
        )
    for item in reports:
        if item.status == "failed":
            lines.append(
                f"Failed {item.quarter.label}: {item.failed_months} month(s); "
                f"reason={item.last_error or 'unknown'}"
            )
    return "\n".join(lines)
