#!/bin/bash

# scripts/reset_db.sh

# This script will drop the existing weather schema and all its tables,
# then recreate the schema and tables using the db_query.sql file.

# WARNING: This will permanently delete all data in the weather schema.

# Database connection parameters
DB_NAME="weather"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

# Path to the SQL query file
SQL_FILE="$(dirname "$0")/db_query.sql"

# PSQL command to drop the schema
DROP_SCHEMA_COMMAND="DROP SCHEMA IF EXISTS weather CASCADE;"

# Execute the drop schema command
echo "Dropping schema 'weather'..."
PGPASSWORD=myadmin psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$DROP_SCHEMA_COMMAND"

# Execute the SQL file to create the schema and tables
echo "Creating schema and tables from $SQL_FILE..."
PGPASSWORD=myadmin psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE"

echo "Database reset complete."