-- P5 EXPLAIN baseline and region-view comparison (read-only).
-- Execute each statement separately in PhpStorm to examine full plans.
-- These SELECTs deliberately filter by a proven pilot date and a district name;
-- replace dates only when comparing another persisted cohort.
-- EXPLAIN ANALYZE executes SELECT but does not modify application rows.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT d.id, d.valid_time_local, d.latitude, d.longitude
FROM climate_data.t_daily_climate_detail AS d
WHERE d.valid_time_local >= TIMESTAMP '2024-05-01'
  AND d.valid_time_local < TIMESTAMP '2024-05-04';

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT v.daily_climate_detail_id, v.valid_time_local, v.latitude,
       v.longitude, v.match_status, v.district_name
FROM climate_data.vw_daily_climate_region AS v
WHERE v.valid_time_local >= TIMESTAMP '2024-05-01'
  AND v.valid_time_local < TIMESTAMP '2024-05-04';

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT v.district_name, COUNT(*) AS observation_count
FROM climate_data.vw_daily_climate_region AS v
WHERE v.valid_time_local >= TIMESTAMP '2024-05-01'
  AND v.valid_time_local < TIMESTAMP '2024-05-04'
  AND v.match_status = 'matched'
GROUP BY v.district_name
ORDER BY v.district_name;

-- Evaluate execution time, buffer hits/reads, actual rows, join types,
-- sequential scans and planned/actual cardinality. With only 1,590 pilot
-- observations, sequential scans may outperform new indexes; do not
-- propose indexes or denormalized FK until a representative workload is measured.
