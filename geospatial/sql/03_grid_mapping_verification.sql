-- P5 grid-to-region verification: read-only, run on weather.climate_data.
-- Check persisted coordinate identity, status consistency and daily observation join.
WITH coordinates AS (
    SELECT DISTINCT latitude, longitude
    FROM climate_data.t_daily_climate_detail
),
summary AS (
    SELECT COUNT(*) AS mapped_rows,
           COUNT(*) FILTER (WHERE match_status = 'matched') AS matched,
           COUNT(*) FILTER (WHERE match_status = 'unmatched') AS unmatched,
           COUNT(*) FILTER (WHERE match_status = 'ambiguous') AS ambiguous,
           COUNT(*) FILTER (WHERE match_status = 'matched'
                              AND (province_id IS NULL OR district_id IS NULL
                                   OR match_count <> 1)) AS invalid_matched,
           COUNT(*) FILTER (WHERE match_status <> 'matched'
                              AND (province_id IS NOT NULL OR district_id IS NOT NULL)) AS invalid_unassigned
    FROM climate_data.t_grid_region
),
membership AS (
    SELECT COUNT(*) AS source_coordinates,
           COUNT(*) FILTER (WHERE g.id IS NULL) AS missing_mappings
    FROM coordinates c
    LEFT JOIN climate_data.t_grid_region g
      ON g.latitude = c.latitude AND g.longitude = c.longitude
),
observations AS (
    SELECT COUNT(*) AS source_rows
    FROM climate_data.t_daily_climate_detail
),
joined AS (
    SELECT COUNT(*) AS joined_rows
    FROM climate_data.t_daily_climate_detail d
    LEFT JOIN climate_data.t_grid_region g
      ON g.latitude = d.latitude AND g.longitude = d.longitude
)
SELECT m.source_coordinates, s.mapped_rows, s.matched, s.unmatched, s.ambiguous,
       m.missing_mappings, s.invalid_matched, s.invalid_unassigned,
       o.source_rows, j.joined_rows,
       CASE WHEN m.source_coordinates = s.mapped_rows
                 AND m.missing_mappings = 0
                 AND s.invalid_matched = 0
                 AND s.invalid_unassigned = 0
                 AND o.source_rows = j.joined_rows
            THEN 'PASS' ELSE 'FAIL' END AS validation_status
FROM membership m CROSS JOIN summary s
CROSS JOIN observations o CROSS JOIN joined j;

-- Export this second result as CSV for an exact spatial map.
SELECT g.latitude, g.longitude, g.match_status, g.match_count,
       p.province_name, d.district_name
FROM climate_data.t_grid_region g
LEFT JOIN climate_data.t_province p ON p.id = g.province_id
LEFT JOIN climate_data.t_district d ON d.id = g.district_id
ORDER BY g.latitude DESC, g.longitude;
