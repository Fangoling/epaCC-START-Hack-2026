#!/bin/bash

# =================================================================
#  epaCC-START-Hack-2026 - Master Stop Script
#  Shuts down all Docker containers associated with the project
# =================================================================

echo "🛑 Shutting down the epaCC Data Ingestion Stack..."
echo "-----------------------------------------------------------------"

# Stop and remove the React Dashboard container
if [ "$(docker ps -aq -f name=dashboard-ui)" ]; then
    echo "[INFO] Stopping Frontend Dashboard (dashboard-ui)..."
    docker stop dashboard-ui > /dev/null
    docker rm dashboard-ui > /dev/null
fi

# Stop and remove the Python API container
if [ "$(docker ps -aq -f name=missing-data-api-container)" ]; then
    echo "[INFO] Stopping Backend API (missing-data-api-container)..."
    docker stop missing-data-api-container > /dev/null
    docker rm missing-data-api-container > /dev/null
fi

# Stop and remove the Database container
if [ "$(docker ps -aq -f name=case-db)" ]; then
    echo "[INFO] Stopping MS SQL Server Database (case-db)..."
    docker stop case-db > /dev/null
    docker rm case-db > /dev/null
fi

echo "-----------------------------------------------------------------"
echo "✅ All project containers have been successfully stopped and removed."
echo "================================================================="