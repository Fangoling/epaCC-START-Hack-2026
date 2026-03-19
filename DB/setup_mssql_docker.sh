#!/bin/bash

# Configuration
CONTAINER_NAME="case-db"
DB_NAME="CaseDB"
# Microsoft SQL Server requires a strong password (at least 8 chars, uppercase, lowercase, numbers)
DB_PASS="StartHack2026!"
DB_PORT="1433"
SQL_FILE="DB/CreateImportTables.sql"

echo "================================================="
echo "   Microsoft SQL Server Docker Setup for Hack2026"
echo "================================================="

# 1. Check if the container is already running
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "[INFO] Container '$CONTAINER_NAME' is already running."
# 2. Check if the container exists but is stopped
elif [ "$(docker ps -aq -f status=exited -f name=$CONTAINER_NAME)" ]; then
    echo "[INFO] Container '$CONTAINER_NAME' exists but is stopped. Starting it now..."
    docker start $CONTAINER_NAME
else
    echo "[INFO] Starting a fresh MS SQL Server container..."
    docker run --name $CONTAINER_NAME \
        -e "ACCEPT_EULA=Y" \
        -e "MSSQL_SA_PASSWORD=$DB_PASS" \
        -p $DB_PORT:1433 \
        -d mcr.microsoft.com/mssql/server:2022-latest

    echo "[WAIT] Waiting 15 seconds for the database to boot up..."
    sleep 15
fi

echo "-------------------------------------------------"
echo "[INFO] Creating database '$DB_NAME'..."
# Ignore the error if the database already exists. Using mssql-tools18 which requires -C for trusting server cert
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$DB_PASS" -C -Q "CREATE DATABASE $DB_NAME;" 2>/dev/null || echo "[INFO] Database already exists, skipping creation."

echo "-------------------------------------------------"
echo "[INFO] Copying schema to the container..."
docker cp "$SQL_FILE" $CONTAINER_NAME:/CreateImportTables.sql

echo "-------------------------------------------------"
echo "[INFO] Running schema import script..."
# Ensure we connect to the newly created database and run the script
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$DB_PASS" -C -d $DB_NAME -i /CreateImportTables.sql

echo "================================================="
echo "   SUCCESS! MS SQL Server is running!            "
echo "================================================="
echo ""
echo "Connection Details:"
echo "Host:     localhost"
echo "Port:     $DB_PORT"
echo "Database: $DB_NAME"
echo "Username: SA"
echo "Password: $DB_PASS"
echo "================================================="