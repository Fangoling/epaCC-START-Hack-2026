#!/bin/bash

# Configuration
DB_NAME="Hack2026"
DB_USER="postgres"       # Change this to your Postgres username
DB_HOST="localhost"      # Change if connecting to a remote host
DB_PORT="5432"           # Default Postgres port

echo "-----------------------------------------"
echo "Creating PostgreSQL Database: $DB_NAME"
echo "-----------------------------------------"

# Connect to the default 'postgres' database to create the new one
# (We drop it first if it already exists to ensure a clean slate)
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d postgres -c "CREATE DATABASE \"$DB_NAME\";"

echo "-----------------------------------------"
echo "Creating Tables in $DB_NAME"
echo "-----------------------------------------"

# Run the schema script against the newly created database
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -f "CreateImportTables_Postgres.sql"

echo "-----------------------------------------"
echo "Database setup complete!"
echo "-----------------------------------------"