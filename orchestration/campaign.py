from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

CAMPAIGN_KEY = "era5_land_2016_2025"
CAMPAIGN_START_YEAR = 2016
CAMPAIGN_END_YEAR = 2025


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
