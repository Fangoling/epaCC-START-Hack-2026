#!/bin/bash

# Configuration
CONTAINER_NAME="case-db"
DB_NAME="CaseDB"
DB_PASS="StartHack2026!"
MOCK_DATA_FILE="DB/test-seeds/InsertMockData_MSSQL.sql"

echo "================================================="
echo "   Injecting Mock Data into MS SQL Server Database"
echo "================================================="

# Check if the container is running
if [ ! "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "[ERROR] Container '$CONTAINER_NAME' is not running!"
    echo "Please run DB/setup_mssql_docker.sh first."
    exit 1
fi

echo "[INFO] Copying mock data script to the container..."
docker cp "$MOCK_DATA_FILE" $CONTAINER_NAME:/InsertMockData_MSSQL.sql

echo "[INFO] Running mock data insertion script..."
# Run the SQL script inside the container using sqlcmd
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$DB_PASS" -C -d $DB_NAME -i /InsertMockData_MSSQL.sql

echo "================================================="
echo "   SUCCESS! Mock data has been inserted.         "
echo "================================================="