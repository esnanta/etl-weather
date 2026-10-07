"""Small timezone-safe helpers for orchestration wait observability."""

from __future__ import annotations

from datetime import datetime
from math import ceil


def format_wait_duration(seconds: float) -> str:
    """Render a non-negative wait as compact hours, minutes, and seconds."""
    total_seconds = max(0, ceil(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def local_iso(moment: datetime, timezone) -> str:
    """Render one timezone-aware timestamp in the supplied local timezone."""
    if moment.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware.")
    return moment.astimezone(timezone).isoformat()
