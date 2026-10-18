"""Daily climate region view owned by geospatial schema initialization.

Daily region interpretation requires canonical climate tables AND spatial tables.
The standalone geospatial initializer deliberately fails if climate tables are
absent: it never creates scientific data tables as an implicit side effect.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from database.config import (
    APPLICATION_SCHEMA, DAILY_CLIMATE_REGION_VIEW,
    DAILY_CLIMATE_DETAIL_TABLE, DAILY_CLIMATE_TABLE,
    DISTRICT_TABLE, GRID_REGION_TABLE, PROVINCE_TABLE,
)

_REQUIRED_TABLES = (
    DAILY_CLIMATE_TABLE, DAILY_CLIMATE_DETAIL_TABLE, GRID_REGION_TABLE,
    PROVINCE_TABLE, DISTRICT_TABLE,
)

_DAILY_REGION_VIEW_QUERY = """CREATE OR REPLACE VIEW climate_data.vw_daily_climate_region AS
SELECT
    d.id AS daily_climate_detail_id,
    d.daily_climate_id,
    c.request_key,
    c.dataset_name,
    c.source,
    c.start_date AS request_start_date,
    c.end_date AS request_end_date,
    d.valid_time,
    d.valid_time_local,
    d.latitude,
    d.longitude,
    d.tp_daily_total_mm,
    d.t2m_daily_mean_c,
    d.d2m_daily_mean_c,
    d.swvl1_daily_mean_m3_m3,
    d.ssrd_daily_total_j_m2,
    g.id AS grid_region_id,
    g.match_status,
    g.match_count,
    g.mapped_at,
    g.province_id,
    p.province_name,
    g.district_id,
    dist.district_name
FROM climate_data.t_daily_climate_detail AS d
JOIN climate_data.t_daily_climate AS c
    ON c.id = d.daily_climate_id
LEFT JOIN climate_data.t_grid_region AS g
    ON g.latitude = d.latitude
   AND g.longitude = d.longitude
LEFT JOIN climate_data.t_province AS p
    ON p.id = g.province_id
LEFT JOIN climate_data.t_district AS dist
    ON dist.id = g.district_id"""


def create_daily_region_view(engine: Engine) -> None:
    """Create or replace the read-only daily region interpretation in place."""
    with engine.begin() as connection:
        missing = [
            name for name in _REQUIRED_TABLES
            if connection.execute(
                text("SELECT to_regclass(:relation)"),
                {"relation": f"{APPLICATION_SCHEMA}.{name}"},
            ).scalar_one_or_none() is None
        ]
        if missing:
            raise RuntimeError(
                "Daily region view requires existing climate/spatial tables: "
                + ", ".join(missing)
                + ". Run 'python -m database.climate --execute' first, "
                  "then rerun geospatial initialization."
            )
        connection.execute(text(_DAILY_REGION_VIEW_QUERY))
        connection.execute(text(
            f"COMMENT ON VIEW {APPLICATION_SCHEMA}.{DAILY_CLIMATE_REGION_VIEW} IS "
            "'Read-only point-to-region interpretation; unmatched observations remain; "
            "not area-weighted.'"
        ))
