"""Regression tests for configurable PostgreSQL region-view identifiers."""
from __future__ import annotations

from unittest.mock import patch

from database import vw_daily_climate_region as daily
from database import vw_monthly_climate_region as monthly


def test_daily_region_view_uses_canonical_identifiers():
    query = daily._build_region_view_query()
    assert f"CREATE OR REPLACE VIEW {daily.APPLICATION_SCHEMA}.{daily.DAILY_CLIMATE_REGION_VIEW}" in query
    for table in (
        daily.DAILY_CLIMATE_TABLE, daily.DAILY_CLIMATE_DETAIL_TABLE,
        daily.GRID_REGION_TABLE, daily.PROVINCE_TABLE, daily.DISTRICT_TABLE,
    ):
        assert f"{daily.APPLICATION_SCHEMA}.{table}" in query


def test_monthly_region_view_uses_canonical_identifiers():
    query = monthly._build_region_view_query()
    assert f"CREATE OR REPLACE VIEW {monthly.APPLICATION_SCHEMA}.{monthly.MONTHLY_CLIMATE_REGION_VIEW}" in query
    for table in (
        monthly.MONTHLY_CLIMATE_TABLE, monthly.MONTHLY_CLIMATE_DETAIL_TABLE,
        monthly.GRID_REGION_TABLE, monthly.PROVINCE_TABLE, monthly.DISTRICT_TABLE,
    ):
        assert f"{monthly.APPLICATION_SCHEMA}.{table}" in query


def test_region_view_sql_responds_to_configured_names():
    for module, mapping in (
        (daily, {"APPLICATION_SCHEMA": "other_schema",
                 "DAILY_CLIMATE_REGION_VIEW": "custom_daily_view",
                 "DAILY_CLIMATE_TABLE": "custom_daily_table"}),
        (monthly, {"APPLICATION_SCHEMA": "other_schema",
                   "MONTHLY_CLIMATE_REGION_VIEW": "custom_monthly_view",
                   "MONTHLY_CLIMATE_TABLE": "custom_monthly_table"}),
    ):
        with patch.multiple(module, **mapping):
            sql = module._build_region_view_query()
        assert "other_schema." in sql
        assert list(mapping.values())[1] in sql
        assert list(mapping.values())[2] in sql
        assert "climate_data." not in sql
