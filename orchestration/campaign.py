from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

CAMPAIGN_KEY = "era5_land_2016_2025"
CAMPAIGN_START_YEAR = 2016
CAMPAIGN_END_YEAR = 2025
CAMPAIGN_TIMEZONE = ZoneInfo("Asia/Jakarta")


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


def iter_campaign_quarters() -> tuple[CampaignQuarter, ...]:
    return tuple(
        CampaignQuarter(year, quarter)
        for year in range(CAMPAIGN_START_YEAR, CAMPAIGN_END_YEAR + 1)
        for quarter in range(1, 5)
    )


def build_quarter_request_specs(
    quarter: CampaignQuarter,
    *,
    current_local_date: date | None = None,
) -> list[dict[str, object]]:
    """Build exactly three validated monthly request specs for one campaign quarter."""
    from orchestration.tasks.era5_land import build_historical_request_specs

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
    """Idempotently seed all forty quarter batches before scheduled selection."""
    from orchestration.progress import build_pipeline_progress_definition
    from orchestration.repositories.pipeline_progress import initialize_pipeline_progress

    progress_ids = []
    for quarter in iter_campaign_quarters():
        specs = build_quarter_request_specs(
            quarter, current_local_date=current_local_date
        )
        definition = build_pipeline_progress_definition(specs)
        progress_ids.append(initialize_pipeline_progress(engine, definition))
    return tuple(progress_ids)


def select_earliest_incomplete_quarter(
    engine: Engine,
    *,
    current_local_date: date | None = None,
) -> CampaignSelection | None:
    """Reconcile in calendar order and select at most one eligible quarter."""
    from orchestration.repositories.pipeline_progress import (
        get_pipeline_progress_by_id,
        reconcile_pipeline_progress,
    )
    from orchestration.tasks.era5_land import AVAILABILITY_LAG_DAYS

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
        if (
            progress.execution_status != "complete"
            or progress.validation_status != "valid"
        ):
            specs = build_quarter_request_specs(
                quarter, current_local_date=today
            )
            return CampaignSelection(quarter, progress_id, tuple(specs))
    return None
