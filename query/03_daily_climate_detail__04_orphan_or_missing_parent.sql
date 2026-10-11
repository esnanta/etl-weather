/*
Title: Daily Climate Detail — Parent/Child Presence
Context: Foreign keys prevent ordinary orphan writes; incomplete or newly seeded partitions may have no observations.
Purpose: Parent/Child Presence.
Tables: climate_data.daily_climate; climate_data.daily_climate_detail.
Grain: One row per anomalous parent/child link.
Scope/parameters: All partitions and detail rows.
Expected result: Zero orphan observations; any parent with zero observations appears as INFO, not automatic FAIL.
Interpretation: Missing-detail partitions can be expected during an active run; orphan rows violate the FK contract.
Limitations: Read-only SQL cannot inspect RAW or Parquet artifact bytes or guarantee scientific validity.
Read-only: Yes; SELECT only.
*/
WITH anomalies AS (
 SELECT 'ORPHAN_DETAIL'::text AS issue, d.daily_climate_id, COUNT(*) AS affected_rows,
        'FAIL'::text AS result
 FROM climate_data.daily_climate_detail d
 LEFT JOIN climate_data.daily_climate dc ON dc.id = d.daily_climate_id
 WHERE dc.id IS NULL
 GROUP BY d.daily_climate_id
 UNION ALL
 SELECT 'EMPTY_PARTITION', dc.id, 0::bigint, 'INFO'
 FROM climate_data.daily_climate dc
 WHERE NOT EXISTS (
   SELECT 1 FROM climate_data.daily_climate_detail d WHERE d.daily_climate_id = dc.id
 )
)
SELECT issue, daily_climate_id, affected_rows, result
FROM anomalies ORDER BY issue, daily_climate_id;
