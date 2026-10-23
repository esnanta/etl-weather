"""Run one daily ERA5-Land ETL stage using the configured request."""

import logging

from config import validate_and_log_app_environment
from pipeline.requests.factory import build_cds_request
from pipeline.runtime import transform_monthly_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Run only the transform stage for each configured monthly partition."""
    try:
        validate_and_log_app_environment(logger)
    except ValueError:
        raise SystemExit(2) from None
    request = build_cds_request(require_api_key=False)
    logger.info("Running transform stage for source '%s'.", request.source.value)
    transform_monthly_batch(request)


if __name__ == "__main__":
    main()
