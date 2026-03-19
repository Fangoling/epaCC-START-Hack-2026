#!/bin/bash

# Configuration
CONTAINER_NAME="case-db"
DB_NAME="CaseDB"
DB_USER="postgres"
DB_PASS="postgres"
DB_PORT="5432"
SQL_FILE="DB/CreateImportTables_Postgres.sql"

echo "================================================="
echo "   PostgreSQL Docker Database Setup for epaCC-START-Hack-2026"
echo "================================================="

# 1. Check if the container is already running
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "[INFO] Container '$CONTAINER_NAME' is already running."
# 2. Check if the container exists but is stopped
elif [ "$(docker ps -aq -f status=exited -f name=$CONTAINER_NAME)" ]; then
    echo "[INFO] Container '$CONTAINER_NAME' exists but is stopped. Starting it now..."
    docker start $CONTAINER_NAME
else
    echo "[INFO] Starting a fresh PostgreSQL container..."
    docker run --name $CONTAINER_NAME \
        -e POSTGRES_PASSWORD=$DB_PASS \
        -p $DB_PORT:5432 \
        -d postgres

    echo "[WAIT] Waiting 5 seconds for the database to boot up..."
    sleep 5
fi

echo "-------------------------------------------------"
echo "[INFO] Copying schema to the container..."
docker cp "$SQL_FILE" $CONTAINER_NAME:/CreateImportTables_Postgres.sql

echo "-------------------------------------------------"
echo "[INFO] Creating database '$DB_NAME'..."
# Ignore the error if the database already exists
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d postgres -c "CREATE DATABASE \"$DB_NAME\";" 2>/dev/null || echo "[INFO] Database already exists, skipping creation."

echo "-------------------------------------------------"
echo "[INFO] Running schema import script..."
# Ensure we connect to the newly created database and run the script
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -f /CreateImportTables_Postgres.sql

echo "================================================="
echo "   SUCCESS! Database is running and configured!  "
echo "================================================="
echo ""
echo "Connection Details:"
echo "Host:     localhost"
echo "Port:     $DB_PORT"
echo "Database: $DB_NAME"
echo "Username: $DB_USER"
echo "Password: $DB_PASS"
echo "================================================="