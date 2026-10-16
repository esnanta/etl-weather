"""Explicit PostGIS bootstrap CLI; canonical implementation is database.bootstrap."""

from __future__ import annotations

import logging

from database.bootstrap import bootstrap


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    version = bootstrap()
    print(f"PostGIS ready (version {version}).")


if __name__ == "__main__":
    main()
