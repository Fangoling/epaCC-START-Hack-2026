#!/bin/bash

# Configuration
CONTAINER_NAME="case-db"
DB_NAME="CaseDB"
DB_USER="postgres"
MOCK_DATA_FILE="DB/test-seeds/InsertMockData_Missing_Postgres.sql"

echo "================================================="
echo "   Injecting Mock Data (with Missing Values)     "
echo "================================================="

# Check if the container is running
if [ ! "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "[ERROR] Container '$CONTAINER_NAME' is not running!"
    echo "Please run DB/setup_postgres_docker.sh first."
    exit 1
fi

echo "[INFO] Running mock data insertion script..."
# Feed the file directly from the host into the container's psql process
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < "$MOCK_DATA_FILE"

echo "================================================="
echo "   SUCCESS! Mock data has been inserted.         "
echo "================================================="