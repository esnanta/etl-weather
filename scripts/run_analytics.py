"""Preview validated sources or export annual analytics Parquet checkpoints.

Exports read existing RAW/transformed files only and never download data.
Existing output is verified before reuse; P4 writes source/output manifests.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from pipeline.analytics.selection import discover_sources, validate_sources
from pipeline.analytics.export import export_selection, output_paths

def _date_bounds(args: argparse.Namespace) -> tuple[date, date]:
    """Validate the required inclusive ISO date range."""
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("Start date must not be after end date.")
    return start, end


def main(argv: list[str] | None = None) -> int:
    """Preview or export a selected period without changing source checkpoints."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolution", choices=("hourly", "daily"), required=True)
    parser.add_argument("--variables", default="all")
    parser.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Inclusive end date (YYYY-MM-DD).")
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
                print(f"  Verified/published: {destination} | rows={rows}")
        except (ValueError, FileExistsError, OSError) as error:
            parser.error(str(error))
        print("P4 export verification and manifest publication completed.")
    else:
        print("P2 validation passed. Preview only; no analytics dataset was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
