/*
Title: Application Schema — Constraint and Index Inventory
Context: The pipeline relies on primary keys, foreign keys, uniqueness,
    status CHECK constraints, and unique indexes for idempotent persistence.
Purpose: Display existing constraints and indexes on application tables.
Tables: pg_catalog.pg_constraint, pg_catalog.pg_class,
    pg_catalog.pg_namespace, pg_catalog.pg_index, pg_catalog.pg_attribute.
Grain: One row per constraint or index (an index backing a constraint
    may also appear as an INDEX row for visibility).
Scope/parameters: All application tables in climate_data.
Expected result: One or more rows describing actual PK/FK/UNIQUE/CHECK
    constraints and indexes for an initialized schema.
Interpretation: INFO inventory to compare with the DDL; an empty result
    requires investigation. This query does not assert each expected object.
Limitations: DDL comparison and constraint enforcement are separate tests.
Read-only: Yes; SELECT only.
*/
WITH app_tables AS (
    SELECT cls.oid AS table_oid, cls.relname AS table_name
    FROM pg_catalog.pg_class AS cls
    JOIN pg_catalog.pg_namespace AS ns ON ns.oid = cls.relnamespace
    WHERE ns.nspname = 'climate_data'
      AND cls.relkind IN ('r', 'p')
),
items AS (
    SELECT t.table_name, 'CONSTRAINT'::text AS object_type,
           c.conname::text AS object_name,
           CASE c.contype
               WHEN 'p' THEN 'PRIMARY KEY'
               WHEN 'f' THEN 'FOREIGN KEY'
               WHEN 'u' THEN 'UNIQUE'
               WHEN 'c' THEN 'CHECK'
               ELSE c.contype::text
           END AS object_kind,
           pg_catalog.pg_get_constraintdef(c.oid) AS definition
    FROM app_tables AS t
    JOIN pg_catalog.pg_constraint AS c ON c.conrelid = t.table_oid
    UNION ALL
    SELECT t.table_name, 'INDEX'::text AS object_type,
           idx.relname::text AS object_name,
           CASE WHEN ix.indisunique THEN 'UNIQUE INDEX' ELSE 'INDEX' END AS object_kind,
           pg_catalog.pg_get_indexdef(idx.oid) AS definition
    FROM app_tables AS t
    JOIN pg_catalog.pg_index AS ix ON ix.indrelid = t.table_oid
    JOIN pg_catalog.pg_class AS idx ON idx.oid = ix.indexrelid
)
SELECT table_name, object_type, object_name, object_kind, definition
FROM items
ORDER BY table_name, object_type, object_name;
