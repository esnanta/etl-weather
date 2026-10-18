-- P5 read-only acceptance. Execute after 05_daily_climate_region_view.sql.
-- PASS requires one view row per original observation and consistent region metadata.
-- Works regardless of the number of days or mapped points.
WITH baseline AS (
    SELECT COUNT(*) AS source_rows FROM climate_data.t_daily_climate_detail
),
view_summary AS (
    SELECT COUNT(*) AS view_rows,
           COUNT(DISTINCT daily_climate_detail_id) AS distinct_detail_ids,
           COUNT(*) FILTER (WHERE grid_region_id IS NULL) AS missing_mappings,
           COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_observations,
           COUNT(*) FILTER (WHERE match_status = 'unmatched') AS unmatched_observations,
           COUNT(*) FILTER (WHERE match_status = 'ambiguous') AS ambiguous_observations,
           COUNT(*) FILTER (
               WHERE match_status = 'matched'
                 AND (province_id IS NULL OR district_id IS NULL
                      OR province_name IS NULL OR district_name IS NULL OR match_count <> 1)
           ) AS invalid_matched,
           COUNT(*) FILTER (
               WHERE match_status IN ('unmatched', 'ambiguous')
                 AND (province_id IS NOT NULL OR district_id IS NOT NULL
                      OR province_name IS NOT NULL OR district_name IS NOT NULL)
           ) AS invalid_unassigned
    FROM climate_data.vw_daily_climate_region
),
identity_check AS (
    SELECT COUNT(*) AS missing_detail_ids
    FROM climate_data.t_daily_climate_detail AS d
    LEFT JOIN climate_data.vw_daily_climate_region AS v
        ON v.daily_climate_detail_id = d.id
    WHERE v.daily_climate_detail_id IS NULL
)
SELECT b.source_rows, v.view_rows, v.distinct_detail_ids,
       v.missing_mappings, v.matched_observations, v.unmatched_observations,
       v.ambiguous_observations, v.invalid_matched, v.invalid_unassigned,
       i.missing_detail_ids,
       CASE WHEN b.source_rows = v.view_rows
             AND v.view_rows = v.distinct_detail_ids
             AND i.missing_detail_ids = 0
             AND v.missing_mappings = 0
             AND v.invalid_matched = 0
             AND v.invalid_unassigned = 0
            THEN 'PASS' ELSE 'FAIL' END AS validation_status
FROM baseline AS b CROSS JOIN view_summary AS v CROSS JOIN identity_check AS i;
