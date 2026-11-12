"""Preview and validate existing analytics sources without writing artifacts.

P2 intentionally has no export execution mode. Parquet output and manifests
will be added in P3/P4 after the read-only source contract is validated.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

import config
from pipeline.analytics.selection import discover_sources, validate_sources

logger = logging.getLogger(__name__)


def _date_bounds(args: argparse.Namespace) -> tuple[date, date]:
    dates = args.start_date is not None or args.end_date is not None
    years = args.start_year is not None or args.end_year is not None
    if dates and years:
        raise ValueError("Use either --start-date/--end-date or --start-year/--end-year.")
    if dates:
        if args.start_date is None or args.end_date is None:
            raise ValueError("Both --start-date and --end-date are required.")
        start, end = date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    else:
        start_year = args.start_year if args.start_year is not None else config.CAMPAIGN_START_YEAR
        end_year = args.end_year if args.end_year is not None else config.CAMPAIGN_END_YEAR
        start, end = date(start_year, 1, 1), date(end_year, 12, 31)
    if start > end:
        raise ValueError("Start date must not be after end date.")
    return start, end


def main(argv: list[str] | None = None) -> int:
    """Print the validated source plan; never write RAW, Parquet, or database."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolution", choices=("hourly", "daily"), required=True)
    parser.add_argument("--variables", default="all")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args(argv)

    try:
        start, end = _date_bounds(args)
        plan = discover_sources(args.resolution, args.variables, start, end)
        validate_sources(plan)
    except (ValueError, FileNotFoundError) as error:
        parser.error(str(error))

    print(f"Resolution: {plan.resolution}")
    print(f"Target period: {plan.start} to {plan.end}")
    print(f"Variables: {','.join(plan.variables)}")
    print(f"Source months: {len(plan.months)}")
    for month in plan.months:
        print(f"  {month.start:%Y-%m}: {month.path}")
    print("P2 validation passed. Preview only; no analytics dataset was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
