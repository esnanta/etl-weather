"""Guard Phase 6 daily-region query projections without requiring PostGIS.

The production view builder and checked-in SQL reference are verified as text.
Execution and spatial join behavior remain integration concerns.
"""

from pathlib import Path

from database.vw_daily_climate_region import _build_region_view_query
from tests._paths import REPOSITORY_ROOT


NEW_COLUMNS = (
    "swvl2_daily_mean_m3_m3",
    "swvl3_daily_mean_m3_m3",
    "u10_daily_mean_m_s",
    "v10_daily_mean_m_s",
    "wind_speed_daily_mean_m_s",
    "wind_direction_daily_deg",
)


def test_region_view_appends_new_columns_without_reordering_old_projection() -> None:
    sql = _build_region_view_query()
    assert "CREATE OR REPLACE VIEW climate_data.vw_daily_climate_region" in sql
    assert "dist.district_name," in sql
    for column in NEW_COLUMNS:
        assert f"d.{column}" in sql
        assert sql.index(f"d.{column}") > sql.index("dist.district_name")
    assert "LEFT JOIN climate_data.t_grid_region AS g" in sql


def test_checked_in_region_view_sql_exposes_same_new_columns() -> None:
    sql = (
        REPOSITORY_ROOT / "geospatial/sql/05_daily_climate_region_view.sql"
    ).read_text(encoding="utf-8")
    for column in NEW_COLUMNS:
        assert f"d.{column}" in sql
    assert "CREATE OR REPLACE VIEW climate_data.vw_daily_climate_region" in sql
