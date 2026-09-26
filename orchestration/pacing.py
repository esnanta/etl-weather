"""Connect CDS extraction to a durable gate without importing Airflow here.

Both scheduled and manual DagRuns call this gate immediately before each
provider retrieve; a denied reservation signals the Airflow operator to defer.
"""

from __future__ import annotations

import logging

from sqlalchemy import create_engine

from orchestration.repositories.cds_pacing import (
    create_cds_pacing_table,
    reserve_cds_submission,
)
from pipeline.database import DatabaseConfig
from pipeline.extraction.cds import CdsSubmissionDeferred
from pipeline.model import WeatherRequest

logger = logging.getLogger(__name__)


def reserve_or_defer(request: WeatherRequest) -> None:
    """Reserve the database clock for one real CDS retrieval attempt."""
    engine = create_engine(DatabaseConfig.from_env().url, future=True)
    try:
        create_cds_pacing_table(engine)
        decision = reserve_cds_submission(engine)
    finally:
        engine.dispose()
    if not decision.granted:
        logger.info(
            "CDS submission deferred | chunk=%s..%s | next_allowed_at=%s",
            request.start_date, request.end_date, decision.next_allowed_at,
        )
        raise CdsSubmissionDeferred(decision.next_allowed_at)
    logger.info(
        "CDS submission reserved | chunk=%s..%s | submitted_at=%s | next_allowed_at=%s",
        request.start_date, request.end_date, decision.checked_at,
        decision.next_allowed_at,
    )
