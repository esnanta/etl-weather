"""Preview or export all available ERA5-Land monthly checkpoints by source year.

Never downloads data or alters RAW/transformed sources. Existing analytics
outputs are verified and reused only when coverage and values still match.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.analytics.selection import (
    AnalyticsSelection,
    discover_available_sources,
    validate_sources,
)
from pipeline.analytics.export import export_selection, output_paths


def main(argv: list[str] | None = None) -> int:
    """Discover and optionally publish all available source years."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolution", choices=("hourly", "daily"), required=True)
    parser.add_argument("--variables", default="all")
    parser.add_argument("--execute", action="store_true", help="Write/verify Parquet; preview is the default.")
    parser.add_argument("--output", type=Path, help="Explicit dataset output directory.")
    args = parser.parse_args(argv)

    try:
        years = discover_available_sources(args.resolution, args.variables)
        # Validate all discovered months together, including RAW overlaps
        # spanning adjacent calendar years, before writing any outputs.
        months = tuple(month for selection in years for month in selection.months)
        combined = AnalyticsSelection(
            args.resolution, years[0].variables,
            months[0].start, months[-1].end, months,
        )
        validate_sources(combined)
    except (ValueError, FileNotFoundError) as error:
        parser.error(str(error))

    print(f"Resolution: {args.resolution}")
    print(f"Variables: {','.join(years[0].variables)}")
    print(f"Available years: {len(years)}")
    for selection in years:
        print(f"  Year: {selection.start.year} | source months: {len(selection.months)}")
        print("  Months: " + ", ".join(f"{month.start:%Y-%m}" for month in selection.months))
        for path in output_paths(selection, output_directory=args.output):
            print(f"  Output: {path}")

    if args.execute:
        try:
            for selection in years:
                for destination, rows in export_selection(
                    selection, output_directory=args.output
                ):
                    print(f"  Verified/published: {destination} | rows={rows}")
        except (ValueError, FileExistsError, OSError) as error:
            parser.error(str(error))
        print("Analytics export verification and manifest publication completed.")
    else:
        print("Validation passed. Preview only; no analytics dataset was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
