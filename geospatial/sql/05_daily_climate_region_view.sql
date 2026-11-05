-- P5 daily administrative region view. Apply explicitly to weather after P5.2 PASS.
-- This CREATE OR REPLACE VIEW changes only the query layer; no source rows are modified.
-- One result row per t_daily_climate_detail row, including unmatched/unmapped points.
-- No numeric aggregation or administrative area-weighting is performed.
CREATE OR REPLACE VIEW climate_data.vw_daily_climate_region AS
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
    dist.district_name,
    d.swvl2_daily_mean_m3_m3,
    d.swvl3_daily_mean_m3_m3,
    d.u10_daily_mean_m_s,
    d.v10_daily_mean_m_s,
    d.wind_speed_daily_mean_m_s,
    d.wind_direction_daily_deg
FROM climate_data.t_daily_climate_detail AS d
JOIN climate_data.t_daily_climate AS c
    ON c.id = d.daily_climate_id
LEFT JOIN climate_data.t_grid_region AS g
    ON g.latitude = d.latitude
   AND g.longitude = d.longitude
LEFT JOIN climate_data.t_province AS p
    ON p.id = g.province_id
LEFT JOIN climate_data.t_district AS dist
    ON dist.id = g.district_id;

COMMENT ON VIEW climate_data.vw_daily_climate_region IS
'Read-only interpretation of existing daily observations by unique administrative grid mapping. Unmatched or unmapped points retain NULL administrative IDs; not area-weighted.';
