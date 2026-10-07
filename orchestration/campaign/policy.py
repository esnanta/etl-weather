"""Define bounded campaign policy, quarter identity, and scheduled selection."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from orchestration.progress.definition import PipelineProgressDefinition

from orchestration.scheduling.time_utils import format_wait_duration, local_iso

logger = logging.getLogger(__name__)

from config import (
    AVAILABILITY_LAG_DAYS,
    CAMPAIGN_END_DATE,
    CAMPAIGN_KEY,
    CAMPAIGN_START_DATE,
    CAMPAIGN_TIMEZONE,
    QUARTER_COOLDOWN_MINUTES,
)


def _quarter_start(year: int, quarter: int) -> date:
    return date(year, (quarter - 1) * 3 + 1, 1)


def _quarter_end(year: int, quarter: int) -> date:
    if quarter == 4:
        return date(year, 12, 31)
    return date(year, quarter * 3 + 1, 1) - timedelta(days=1)


@dataclass(frozen=True, order=True)
class CampaignQuarter:
    year: int
    quarter: int

    def __post_init__(self) -> None:
        if self.quarter not in (1, 2, 3, 4):
            raise ValueError("quarter must be between 1 and 4.")
        if self.start_date < CAMPAIGN_START_DATE or self.end_date > CAMPAIGN_END_DATE:
            raise ValueError(
                "Campaign quarter must be within "
                f"{CAMPAIGN_START_DATE.isoformat()} through "
                f"{CAMPAIGN_END_DATE.isoformat()}."
            )

    @property
    def start_date(self) -> date:
        return _quarter_start(self.year, self.quarter)

    @property
    def end_date(self) -> date:
        return _quarter_end(self.year, self.quarter)

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




@dataclass(frozen=True)
class QuarterCooldownEligibility:
    """Describe whether a completed predecessor has cleared its cooldown."""

    eligible: bool
    eligible_at: datetime
    remaining_seconds: float


def quarter_cooldown_eligibility(
    completed_at: datetime,
    current_time: datetime,
    *,
    cooldown_minutes: int = QUARTER_COOLDOWN_MINUTES,
) -> QuarterCooldownEligibility:
    """Return predecessor cooldown eligibility using timezone-aware instants."""
    if completed_at.tzinfo is None or current_time.tzinfo is None:
        raise ValueError("Quarter cooldown timestamps must be timezone-aware.")
    if isinstance(cooldown_minutes, bool) or not isinstance(cooldown_minutes, int):
        raise ValueError("cooldown_minutes must be a nonnegative integer.")
    if cooldown_minutes < 0:
        raise ValueError("cooldown_minutes must be a nonnegative integer.")

    eligible_at = completed_at + timedelta(minutes=cooldown_minutes)
    remaining_seconds = max(0.0, (eligible_at - current_time).total_seconds())
    return QuarterCooldownEligibility(
        eligible=current_time >= eligible_at,
        eligible_at=eligible_at,
        remaining_seconds=remaining_seconds,
    )


def iter_campaign_quarters() -> tuple[CampaignQuarter, ...]:
    quarters = []
    for year in range(CAMPAIGN_START_DATE.year, CAMPAIGN_END_DATE.year + 1):
        for quarter in range(1, 5):
            start = _quarter_start(year, quarter)
            end = _quarter_end(year, quarter)
            if start >= CAMPAIGN_START_DATE and end <= CAMPAIGN_END_DATE:
                quarters.append(CampaignQuarter(year, quarter))
    return tuple(quarters)


def build_campaign_progress_definition(
    request_specs: list[dict[str, object]] | tuple[dict[str, object], ...],
) -> PipelineProgressDefinition:
    from orchestration.progress.definition import build_pipeline_progress_definition
    return build_pipeline_progress_definition(
        request_specs,
        identity_context={"campaign_key": CAMPAIGN_KEY},
    )


def build_quarter_request_specs(
    quarter: CampaignQuarter,
    *,
    current_local_date: date | None = None,
) -> list[dict[str, object]]:
    from orchestration.scheduling.requests import build_historical_request_specs
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
    from orchestration.repositories.pipeline_progress import initialize_pipeline_progress
    progress_ids = []
    for quarter in iter_campaign_quarters():
        specs = build_quarter_request_specs(quarter, current_local_date=current_local_date)
        definition = build_campaign_progress_definition(specs)
        progress_ids.append(initialize_pipeline_progress(engine, definition))
    return tuple(progress_ids)


def matching_campaign_progress_definition(
    request_spec: Mapping[str, Any],
) -> PipelineProgressDefinition | None:
    from pipeline.requests.weather import build_request_key, weather_request_from_spec
    request = weather_request_from_spec(request_spec)
    start = date.fromisoformat(request.start_date)
    end = date.fromisoformat(request.end_date)
    if start < CAMPAIGN_START_DATE or end > CAMPAIGN_END_DATE:
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
    current_time: datetime | None = None,
) -> CampaignSelection | CampaignBlocked | None:
    from orchestration.repositories.pipeline_progress import (
        get_failed_progress_partitions,
        get_pipeline_progress_by_id,
        reconcile_pipeline_progress,
        try_claim_pipeline_progress,
    )
    now = current_time or datetime.now(CAMPAIGN_TIMEZONE)
    if now.tzinfo is None:
        raise ValueError("current_time must be timezone-aware.")
    today = current_local_date or now.astimezone(CAMPAIGN_TIMEZONE).date()
    eligibility_boundary = today - timedelta(days=AVAILABILITY_LAG_DAYS)
    progress_ids = initialize_campaign_progress(engine, current_local_date=date.max)

    previous_quarter: CampaignQuarter | None = None
    previous_progress = None

    for quarter, progress_id in zip(iter_campaign_quarters(), progress_ids, strict=True):
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
            if previous_progress is not None:
                if (
                    previous_progress.execution_status != "complete"
                    or previous_progress.validation_status != "valid"
                ):
                    return None
                if previous_progress.completed_at is None:
                    assert previous_quarter is not None
                    return CampaignBlocked(
                        quarter,
                        progress_id,
                        f"{previous_quarter.label} is complete and valid but "
                        "has no completed_at timestamp.",
                    )

                cooldown = quarter_cooldown_eligibility(
                    previous_progress.completed_at,
                    now,
                )
                assert previous_quarter is not None
                if not cooldown.eligible:
                    logger.info(
                        "Campaign quarter cooldown active | previous_quarter=%s | "
                        "next_quarter=%s | completed_at_local=%s | cooldown=%dm | "
                        "eligible_at_local=%s | wait=%s",
                        previous_quarter.label,
                        quarter.label,
                        local_iso(previous_progress.completed_at, CAMPAIGN_TIMEZONE),
                        QUARTER_COOLDOWN_MINUTES,
                        local_iso(cooldown.eligible_at, CAMPAIGN_TIMEZONE),
                        format_wait_duration(cooldown.remaining_seconds),
                    )
                    return None
                logger.info(
                    "Campaign quarter cooldown satisfied | previous_quarter=%s | "
                    "next_quarter=%s | eligible_at_local=%s",
                    previous_quarter.label,
                    quarter.label,
                    local_iso(cooldown.eligible_at, CAMPAIGN_TIMEZONE),
                )

            if not try_claim_pipeline_progress(engine, progress_id, claim_owner):
                return CampaignBlocked(
                    quarter, progress_id,
                    f"{quarter.label} is already claimed by another run.",
                )
            specs = build_quarter_request_specs(quarter, current_local_date=today)
            return CampaignSelection(quarter, progress_id, tuple(specs))

        previous_quarter = quarter
        previous_progress = progress

    return None
