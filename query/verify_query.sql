/* Expected result 1 */
SELECT COUNT(*)
FROM climate_data.daily_climate;

/* Expected result 747 */
SELECT COUNT(*)
FROM climate_data.daily_climate_detail
WHERE daily_climate_id = 1;

/* Duplicate check */
/* Expected result 0 */
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