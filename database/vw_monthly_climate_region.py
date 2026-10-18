"""Prospective monthly administrative view; creates no monthly observations."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import (
    APPLICATION_SCHEMA, MONTHLY_CLIMATE_REGION_VIEW,
    MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE,
    GRID_REGION_TABLE, PROVINCE_TABLE, DISTRICT_TABLE,
)

_REQUIRED_MONTHLY_TABLES = (
    MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE,
    GRID_REGION_TABLE, PROVINCE_TABLE, DISTRICT_TABLE,
)

_MONTHLY_REGION_VIEW_QUERY = """CREATE OR REPLACE VIEW climate_data.vw_monthly_climate_region AS
SELECT
    d.id AS monthly_climate_detail_id,
    d.monthly_climate_id,
    c.request_key,
    c.dataset_name,
    c.source,
    c.month_start AS request_month_start,
    c.month_end AS request_month_end,
    d.month_start,
    d.latitude,
    d.longitude,
    d.tp_monthly_total_mm,
    d.t2m_monthly_mean_c,
    d.d2m_monthly_mean_c,
    d.swvl1_monthly_mean_m3_m3,
    d.ssrd_monthly_total_j_m2,
    g.id AS grid_region_id,
    g.match_status,
    g.match_count,
    g.mapped_at,
    g.province_id,
    p.province_name,
    g.district_id,
    dist.district_name
FROM climate_data.t_monthly_climate_detail AS d
JOIN climate_data.t_monthly_climate AS c
    ON c.id = d.monthly_climate_id
LEFT JOIN climate_data.t_grid_region AS g
    ON g.latitude = d.latitude AND g.longitude = d.longitude
LEFT JOIN climate_data.t_province AS p
    ON p.id = g.province_id
LEFT JOIN climate_data.t_district AS dist
    ON dist.id = g.district_id"""


def create_monthly_region_view(engine: Engine) -> None:
    """Create empty-until-populated monthly administrative view."""
    with engine.begin() as conn:
        missing = [
            name for name in _REQUIRED_MONTHLY_TABLES
            if conn.execute(
                text("SELECT to_regclass(:relation)"),
                {"relation": f"{APPLICATION_SCHEMA}.{name}"},
            ).scalar_one_or_none() is None
        ]
        if missing:
            raise RuntimeError("Monthly region view requires tables: " + ", ".join(missing))
        conn.execute(text(_MONTHLY_REGION_VIEW_QUERY))
        conn.execute(text(
            f"COMMENT ON VIEW {APPLICATION_SCHEMA}.{MONTHLY_CLIMATE_REGION_VIEW} IS "
            "'Prospective monthly point-to-region read-only view; monthly aggregation not implemented.'"
        ))
