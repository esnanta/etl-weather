from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from calendar import monthrange
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from orchestration.progress import PipelineProgressDefinition

from orchestration.config import (
    AVAILABILITY_LAG_DAYS,
    CAMPAIGN_END_YEAR,
    CAMPAIGN_KEY,
    CAMPAIGN_START_YEAR,
    CAMPAIGN_TIMEZONE,
)


@dataclass(frozen=True, order=True)
class CampaignQuarter:
    year: int
    quarter: int

    def __post_init__(self) -> None:
        if not CAMPAIGN_START_YEAR <= self.year <= CAMPAIGN_END_YEAR:
            raise ValueError(
                f"Campaign year must be between {CAMPAIGN_START_YEAR} and "
                f"{CAMPAIGN_END_YEAR}."
            )
        if self.quarter not in (1, 2, 3, 4):
            raise ValueError("quarter must be between 1 and 4.")

    @property
    def start_date(self) -> date:
        month = (self.quarter - 1) * 3 + 1
        return date(self.year, month, 1)

    @property
    def end_date(self) -> date:
        if self.quarter == 4:
            return date(self.year, 12, 31)
        next_quarter_month = self.quarter * 3 + 1
        return date(self.year, next_quarter_month, 1) - timedelta(days=1)

    @property
    def label(self) -> str:
        return f"{self.year} Q{self.quarter}"


@dataclass(frozen=True)
class CampaignSelection:
    quarter: CampaignQuarter
    progress_id: int
    request_specs: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class CampaignBlocked:
    quarter: CampaignQuarter
    progress_id: int
    reason: str


def iter_campaign_quarters() -> tuple[CampaignQuarter, ...]:
    return tuple(
        CampaignQuarter(year, quarter)
        for year in range(CAMPAIGN_START_YEAR, CAMPAIGN_END_YEAR + 1)
        for quarter in range(1, 5)
    )


def build_campaign_progress_definition(
    request_specs: list[dict[str, object]] | tuple[dict[str, object], ...],
) -> PipelineProgressDefinition:
    """Build durable progress identity scoped to the configured campaign key."""
    from orchestration.progress import build_pipeline_progress_definition

    return build_pipeline_progress_definition(
        request_specs,
        identity_context={"campaign_key": CAMPAIGN_KEY},
    )


def build_quarter_request_specs(
    quarter: CampaignQuarter,
    *,
    current_local_date: date | None = None,
) -> list[dict[str, object]]:
    """Build exactly three validated monthly request specs for one campaign quarter."""
    from orchestration.requests import build_historical_request_specs

    specs = build_historical_request_specs(
        quarter.start_date.isoformat(),
        quarter.end_date.isoformat(),
        current_local_date=current_local_date,
    )
    if len(specs) != 3:
        raise ValueError(
            f"{quarter.label} must resolve to exactly three monthly partitions."
        )
    return specs


def initialize_campaign_progress(
    engine: Engine,
    *,
    current_local_date: date | None = None,
) -> tuple[int, ...]:
    """Idempotently seed all configured quarter batches before selection."""
    from orchestration.repositories.pipeline_progress import initialize_pipeline_progress

    progress_ids = []
    for quarter in iter_campaign_quarters():
        specs = build_quarter_request_specs(
            quarter, current_local_date=current_local_date
        )
        definition = build_campaign_progress_definition(specs)
        progress_ids.append(initialize_pipeline_progress(engine, definition))
    return tuple(progress_ids)


def matching_campaign_progress_definition(
    request_spec: Mapping[str, Any],
) -> PipelineProgressDefinition | None:
    """Match one exact calendar-month request to the current campaign identity."""
    from pipeline.request import build_request_key, weather_request_from_spec

    request = weather_request_from_spec(request_spec)
    start = date.fromisoformat(request.start_date)
    end = date.fromisoformat(request.end_date)
    if not CAMPAIGN_START_YEAR <= start.year <= CAMPAIGN_END_YEAR:
        return None
    if (
        start.day != 1
        or end != date(start.year, start.month, monthrange(start.year, start.month)[1])
    ):
        return None

    quarter = CampaignQuarter(start.year, (start.month - 1) // 3 + 1)
    specs = build_quarter_request_specs(quarter, current_local_date=date.max)
    definition = build_campaign_progress_definition(specs)
    request_key = build_request_key(request)
    if request_key not in {partition.request_key for partition in definition.partitions}:
        return None
    return definition


def select_earliest_incomplete_quarter(
    engine: Engine,
    *,
    claim_owner: str,
    current_local_date: date | None = None,
) -> CampaignSelection | CampaignBlocked | None:
    """Reconcile in order and claim at most one eligible unfinished quarter."""
    from orchestration.repositories.pipeline_progress import (
        get_failed_progress_partitions,
        get_pipeline_progress_by_id,
        reconcile_pipeline_progress,
        try_claim_pipeline_progress,
    )
    today = current_local_date or datetime.now(CAMPAIGN_TIMEZONE).date()
    eligibility_boundary = today - timedelta(days=AVAILABILITY_LAG_DAYS)

    # Seeding is independent of the current provider-availability boundary.
    progress_ids = initialize_campaign_progress(
        engine, current_local_date=date.max
    )
    for quarter, progress_id in zip(
        iter_campaign_quarters(), progress_ids, strict=True
    ):
        if quarter.end_date > eligibility_boundary:
            return None
        reconcile_pipeline_progress(engine, progress_id)
        progress = get_pipeline_progress_by_id(engine, progress_id)
        if progress.failed_partition_count:
            failed = get_failed_progress_partitions(engine, progress_id)
            months = ", ".join(
                f"{start} through {end} ({error or 'unknown error'})"
                for start, end, error in failed
            )
            return CampaignBlocked(
                quarter, progress_id,
                f"Manual repair required for {quarter.label}: {months}",
            )
        if (
            progress.execution_status != "complete"
            or progress.validation_status != "valid"
        ):
            if not try_claim_pipeline_progress(engine, progress_id, claim_owner):
                return CampaignBlocked(
                    quarter, progress_id,
                    f"{quarter.label} is already claimed by another run.",
                )
            specs = build_quarter_request_specs(
                quarter, current_local_date=today
            )
            return CampaignSelection(quarter, progress_id, tuple(specs))
    return None
