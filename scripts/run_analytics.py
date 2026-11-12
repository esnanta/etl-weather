"""Preview validated sources or export annual analytics Parquet checkpoints.

Exports read existing RAW/transformed files only and never download data.
The P3 writer refuses all overwrites. Dataset manifests are deferred to P4.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import config
from pipeline.analytics.selection import discover_sources, validate_sources
from pipeline.analytics.export import export_selection, output_paths

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
    parser.add_argument("--execute", action="store_true", help="Write Parquet; default is preview only.")
    parser.add_argument("--output", type=Path, help="Explicit dataset output directory.")
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
    destinations = output_paths(plan, output_directory=args.output)
    for destination in destinations:
        print(f"  Output: {destination}")
    if args.execute:
        try:
            for destination, rows in export_selection(plan, output_directory=args.output):
                print(f"  Exported: {destination} | rows={rows}")
        except (ValueError, FileExistsError, OSError) as error:
            parser.error(str(error))
        print("P3 export completed. Dataset manifest is deferred to P4.")
    else:
        print("P2 validation passed. Preview only; no analytics dataset was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
