-- P5 coordinate export: read-only; export result as CSV for an exact map.
SELECT g.latitude, g.longitude, g.match_status, g.match_count,
       p.province_name, d.district_name
FROM climate_data.t_grid_region g
LEFT JOIN climate_data.t_province p ON p.id = g.province_id
LEFT JOIN climate_data.t_district d ON d.id = g.district_id
ORDER BY g.latitude DESC, g.longitude;
