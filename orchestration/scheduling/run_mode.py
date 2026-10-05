from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping


@dataclass(frozen=True)
class RunMode:
    mode: str
    start_date: str | None = None
    end_date: str | None = None


def resolve_run_mode(
    run_type: str,
    conf: Mapping[str, Any] | None,
) -> RunMode:
    """Resolve Airflow run provenance and validate operator-supplied dates."""
    normalized_type = getattr(run_type, "value", run_type)
    configuration = {} if conf is None else conf
    if not isinstance(configuration, Mapping):
        raise ValueError("DagRun configuration must be an object.")

    if normalized_type == "scheduled":
        if configuration:
            raise ValueError("Scheduled DagRuns must not provide configuration.")
        return RunMode(mode="scheduled")

    if normalized_type != "manual":
        raise ValueError(f"Unsupported DagRun type: {normalized_type!r}.")

    if set(configuration) != {"start_date", "end_date"}:
        raise ValueError(
            "Manual DagRuns require only start_date and end_date."
        )

    start = _parse_date(configuration["start_date"], "start_date")
    end = _parse_date(configuration["end_date"], "end_date")
    if end < start:
        raise ValueError("Manual end_date must not precede start_date.")

    return RunMode(
        mode="manual",
        start_date=start.isoformat(),
        end_date=end.isoformat(),
    )


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a date in YYYY-MM-DD format.")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a date in YYYY-MM-DD format.") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field} must be a date in YYYY-MM-DD format.")
    return parsed
