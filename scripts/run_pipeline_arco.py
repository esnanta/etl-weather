import logging
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pipeline.runtime import run_weekly_batch
from scripts.config import build_arco_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def main() -> None:
    """Run weekly pipeline batch for ARCO source."""
    request = build_arco_request()
    logger.info("Running weekly pipeline batch for source '%s'.", request.source.value)
    run_weekly_batch(request)


if __name__ == "__main__":
    main()
