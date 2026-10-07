"""Resolve a bounded campaign tick cadence without starting it by default.

Quarter selection is persisted independently, so changing the scheduler tick
must never change request identity or the CDS submission interval.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import timedelta

from config import CADENCE_CRON, CADENCE_ENV, DEFAULT_CADENCE, ENABLED_ENV


def campaign_schedule(
    environ: Mapping[str, str] | None = None,
) -> str | timedelta | None:
    """Return a Jakarta-local cron tick only when explicitly enabled."""
    values = os.environ if environ is None else environ
    enabled = values.get(ENABLED_ENV, "false").strip().lower()
    if enabled not in {"true", "false"}:
        raise ValueError(f"{ENABLED_ENV} must be 'true' or 'false'.")
    if enabled == "false":
        return None

    cadence = values.get(CADENCE_ENV, DEFAULT_CADENCE).strip().lower()
    if cadence not in CADENCE_CRON:
        raise ValueError(
            f"{CADENCE_ENV} must be one of: {', '.join(CADENCE_CRON)}."
        )
    return CADENCE_CRON[cadence]
