import logging
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config import validate_and_log_app_environment
from pipeline.runtime import run_monthly_batch
from request_factory import build_cds_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def main() -> None:
    """Run a monthly pipeline batch for a CDS source."""
    validate_and_log_app_environment(logger)
    request = build_cds_request()
    logger.info("Running monthly pipeline batch for source '%s'.", request.source.value)
    run_monthly_batch(request)


if __name__ == "__main__":
    main()
