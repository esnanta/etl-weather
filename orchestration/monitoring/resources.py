"""Monitor disk capacity and process peak memory around ETL task boundaries."""

from __future__ import annotations

import logging
import os
import resource
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from orchestration.config import DISK_WARNING_GIB, GIB

# Preserve the historical task logger identity after moving monitoring helpers.
logger = logging.getLogger("orchestration.tasks.era5_land")


def warn_low_disk_space(boundary: str) -> None:
    """Warn about low space on artifact and temporary-data filesystems."""
    data_directory = Path(__file__).resolve().parents[2] / "data"
    artifact_path = data_directory if data_directory.is_dir() else data_directory.parent
    locations = [("artifacts", artifact_path)]
    try:
        locations.append(("temporary", Path(tempfile.gettempdir())))
    except OSError as exc:
        logger.warning(
            "[DISK] temporary directory unavailable | boundary=%s | error=%s",
            boundary,
            exc,
        )
    checked_devices: set[int] = set()
    for location, path in locations:
        try:
            device = path.stat().st_dev
            if device in checked_devices:
                continue
            available_bytes = shutil.disk_usage(path).free
            checked_devices.add(device)
        except OSError as exc:
            logger.warning(
                "[DISK] free space check unavailable | boundary=%s | "
                "location=%s | path=%s | error=%s",
                boundary,
                location,
                path,
                exc,
            )
            continue

        if available_bytes < DISK_WARNING_GIB * GIB:
            logger.warning(
                "[DISK] low free space | boundary=%s | location=%s | "
                "path=%s | available_gib=%.2f | threshold_gib=%d",
                boundary,
                location,
                path,
                available_bytes / GIB,
                DISK_WARNING_GIB,
            )


@contextmanager
def measure_peak_memory(boundary: str) -> Iterator[None]:
    """Log the process RSS high-water mark even when a boundary fails."""
    before_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    try:
        yield
    finally:
        after_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info(
            "[METRIC] task memory | boundary=%s | pid=%d | "
            "process_peak_rss_mib=%.2f | peak_before_mib=%.2f",
            boundary,
            os.getpid(),
            after_kib / 1024,
            before_kib / 1024,
        )
