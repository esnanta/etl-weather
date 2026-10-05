"""Connect CDS extraction to a durable gate without importing Airflow here.

Both scheduled and manual DagRuns call this gate immediately before each
provider retrieve; a denied reservation signals the Airflow operator to defer.
"""

from __future__ import annotations

import logging

from orchestration.repositories.database import initialize_pacing_storage, runtime_database_engine
from orchestration.repositories.submission_pacing import reserve_submission
from pipeline.extraction.cds import CdsSubmissionDeferred
from pipeline.model import WeatherRequest

logger = logging.getLogger(__name__)


def reserve_or_defer(request: WeatherRequest) -> None:
    """Reserve the database clock for one real CDS retrieval attempt."""
    with runtime_database_engine() as engine:
        initialize_pacing_storage(engine)
        decision = reserve_submission(engine)
    if not decision.granted:
        logger.info(
            "CDS submission deferred\nchunk=%s..%s\nnext_allowed_at=%s",
            request.start_date, request.end_date, decision.next_allowed_at,
        )
        raise CdsSubmissionDeferred(decision.next_allowed_at)
    logger.info(
        "CDS submission reserved\nchunk=%s..%s\nsubmitted_at=%s\nnext_allowed_at=%s",
        request.start_date, request.end_date, decision.checked_at,
        decision.next_allowed_at,
    )

