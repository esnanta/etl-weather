"""Build validated monthly ERA5-Land request specifications for orchestration."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from orchestration.config import AVAILABILITY_LAG_DAYS, PIPELINE_TIMEZONE

# Preserve the historical orchestration task logger identity after module movement.
logger = logging.getLogger("orchestration.tasks.era5_land")


def build_historical_request_specs(
    start_date: str | None,
    end_date: str | None,
    *,
    current_local_date: date | None = None,
) -> list[dict[str, object]]:
    """Validate an explicit historical range and return monthly request specs."""
    from pipeline.requests.weather import (
        split_request_by_month,
        validate_request,
        weather_request_to_spec,
    )
    from pipeline.requests.factory import build_cds_request

    if not start_date or not end_date:
        raise ValueError(
            "Historical start_date and end_date must be supplied explicitly."
        )

    request = build_cds_request(
        start_date=start_date,
        end_date=end_date,
        require_api_key=False,
    )
    validate_request(request)
    today = current_local_date or datetime.now(PIPELINE_TIMEZONE).date()
    eligibility_boundary = today - timedelta(days=AVAILABILITY_LAG_DAYS)
    requested_end = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    if requested_end > eligibility_boundary:
        raise ValueError(
            f"Historical end_date {requested_end.isoformat()} is not eligible; "
            f"the current boundary is {eligibility_boundary.isoformat()} "
            f"({AVAILABILITY_LAG_DAYS} completed local days)."
        )

    monthly_requests = split_request_by_month(request)
    logger.info(
        "Prepared %d monthly ERA5-Land partitions for %s through %s; "
        "eligibility_boundary=%s",
        len(monthly_requests),
        request.start_date,
        request.end_date,
        eligibility_boundary.isoformat(),
    )
    return [weather_request_to_spec(monthly) for monthly in monthly_requests]
