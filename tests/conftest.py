"""Provide deterministic environment defaults for the repository test suite.

Runtime configuration still comes from the developer or deployment .env. Tests
set only non-secret configuration values here so campaign coverage does not
depend on a developer's current trial range.
"""

from __future__ import annotations

import os

# Tests exercise multiple campaign quarters independently of the local trial range.
os.environ["CAMPAIGN_KEY"] = "era5_land_test"
os.environ["CAMPAIGN_START_DATE"] = "2016-01-01"
os.environ["CAMPAIGN_END_DATE"] = "2016-12-31"

# Standalone request tests require deterministic environment-backed defaults.
os.environ["CDS_START_DATE"] = "2022-01-01"
os.environ["CDS_END_DATE"] = "2022-01-03"
