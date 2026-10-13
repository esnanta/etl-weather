-- One-time non-destructive column rename for existing P4.3 spatial tables.
-- Run on weather database before the updated GeoJSON importer or verification.
-- Preserves rows, geometry XYZ, constraints and existing indexes.
BEGIN;
DO $$
BEGIN
    IF current_database() <> 'weather' THEN
        RAISE EXCEPTION 'Expected weather database, got %', current_database();
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='climate_data' AND table_name='t_province'
          AND column_name='geom'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='climate_data' AND table_name='t_province'
          AND column_name='boundary_geometry'
    ) THEN
        ALTER TABLE climate_data.t_province RENAME COLUMN geom TO boundary_geometry;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='climate_data' AND table_name='t_district'
          AND column_name='geom'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='climate_data' AND table_name='t_district'
          AND column_name='boundary_geometry'
    ) THEN
        ALTER TABLE climate_data.t_district RENAME COLUMN geom TO boundary_geometry;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='climate_data' AND table_name='t_province'
          AND column_name='boundary_geometry'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='climate_data' AND table_name='t_district'
          AND column_name='boundary_geometry'
    ) THEN
        RAISE EXCEPTION 'Expected boundary_geometry columns are missing after migration';
    END IF;
END $$;
COMMIT;

SELECT table_name, column_name, udt_name
FROM information_schema.columns
WHERE table_schema = 'climate_data'
  AND table_name IN ('t_province','t_district')
  AND column_name = 'boundary_geometry'
ORDER BY table_name;
