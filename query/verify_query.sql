/*
 * Daily climate database verification queries
 *
 * Purpose
 * -------
 * Run these queries after the CDS ERA5-Land pipeline has loaded data into
 * PostgreSQL, especially after an identical rerun, to verify that the
 * canonical daily-climate persistence contract is still correct.
 *
 * These checks provide runtime evidence for P11 — idempotent database load:
 *
 * 1. `daily_climate` contains one metadata row for the current logical
 *    request partition. Re-running the same request must reuse that partition
 *    through the deterministic `request_key` instead of creating another row.
 *
 * 2. `daily_climate_detail` contains the expected number of daily grid-cell
 *    observations for that partition. For the current sanity request
 *    (2022-01-01 through 2022-01-03), the expected result is 747 rows:
 *    3 local days × 249 valid land grid cells.
 *
 * 3. No duplicate observation keys exist. The canonical observation identity
 *    is:
 *
 *        daily_climate_id + valid_time + latitude + longitude
 *
 *    An identical pipeline rerun must UPSERT these observations rather than
 *    append duplicates.
 *
 * The numeric expectations below apply to the current development sanity run.
 * If the request period, bounding box, or valid grid-cell count changes, update
 * the expected counts accordingly. The duplicate check should always return
 * zero rows.
 */

/* Expected result: 1 logical daily-climate partition */
SELECT COUNT(*)
FROM climate_data.daily_climate;

/* Expected result: 747 observations for the current sanity partition */
SELECT COUNT(*)
FROM climate_data.daily_climate_detail
WHERE daily_climate_id = 1;

/* Expected result: 0 duplicate observation keys */
SELECT
    daily_climate_id,
    valid_time,
    latitude,
    longitude,
    COUNT(*)
FROM climate_data.daily_climate_detail
GROUP BY
    daily_climate_id,
    valid_time,
    latitude,
    longitude
HAVING COUNT(*) > 1;