-- P4.2 PostGIS capability check (read-only).
-- Run this against a dedicated isolated TEST database in PhpStorm.
-- No CREATE EXTENSION, schema modification, or Airflow interaction.
WITH extension_state AS (
    SELECT
        EXISTS (
            SELECT 1 FROM pg_available_extensions
            WHERE name = 'postgis'
        ) AS package_available,
        (SELECT default_version
         FROM pg_available_extensions
         WHERE name = 'postgis') AS available_version,
        (SELECT extversion
         FROM pg_extension
         WHERE extname = 'postgis') AS installed_version
),
database_context AS (
    SELECT
        current_database() AS database_name,
        current_user AS connected_user,
        current_setting('server_version') AS postgresql_version,
        current_setting('transaction_read_only') AS transaction_read_only,
        has_database_privilege(current_user, current_database(), 'CREATE') AS can_create_in_database
)
SELECT
    c.database_name,
    c.connected_user,
    c.postgresql_version,
    c.transaction_read_only,
    e.package_available,
    e.available_version,
    e.installed_version,
    c.can_create_in_database,
    CASE
        WHEN c.database_name IN ('weather', 'postgres', 'template0', 'template1')
            THEN 'STOP_USE_ISOLATED_TEST_DB'
        WHEN NOT e.package_available
            THEN 'POSTGIS_PACKAGE_NOT_AVAILABLE'
        WHEN e.installed_version IS NULL
            THEN 'AVAILABLE_NOT_INSTALLED'
        ELSE 'POSTGIS_INSTALLED'
    END AS preflight_status
FROM database_context c
CROSS JOIN extension_state e;

-- If the status says POSTGIS_INSTALLED, run this separate read-only
-- geometry check against the SAME isolated test database:
-- SELECT postgis_full_version(), ST_AsEWKT(
--     ST_GeomFromEWKT('SRID=4326;MULTIPOLYGON Z (((95 2 0, 96 2 0, 96 3 0, 95 2 0)))')
-- ) AS round_trip_xyz;
