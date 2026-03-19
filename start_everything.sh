#!/bin/bash

# =================================================================
#  epaCC-START-Hack-2026 - Master Start Script
#  Spins up the entire stack: Database, Backend API, and Frontend UI
# =================================================================

# Exit immediately if a command exits with a non-zero status
set -e

echo "🚀 Starting the epaCC Data Ingestion Stack..."
echo "-----------------------------------------------------------------"

# Step 1: Start the PostgreSQL Database
echo "📦 [Step 1/4] Setting up the PostgreSQL Database..."
bash DB/setup_postgres_docker.sh

# Step 2: Inject Mock Data (with missing values to demonstrate the tool)
echo "🧬 [Step 2/4] Injecting test data with missing values..."
bash DB/test-seeds/insert_mock_data_missing.sh

# Step 3: Start the Backend API (FastAPI)
echo "⚙️  [Step 3/4] Starting the Python Missing Data API..."
# We run this in the background using '&' so it doesn't block the script
bash src/missing_data/run_api_docker.sh > /dev/null 2>&1 &
# Give the API a few seconds to fully boot inside its container
sleep 3

# Step 4: Start the Frontend Dashboard (React)
echo "🖥️  [Step 4/4] Starting the React Interactive Dashboard..."
# We run this in the background using '&'
bash src/dashboard/run_dashboard_docker.sh > /dev/null 2>&1 &
# Give the UI a few seconds to compile and start
sleep 5

echo "-----------------------------------------------------------------"
echo "🎉 SUCCESS! The entire stack is now running."
echo "-----------------------------------------------------------------"
echo ""
echo "👉 The Interactive Dashboard is available at: http://localhost:3000"
echo "👉 The Backend API is available at:           http://localhost:8000/docs"
echo "👉 The PostgreSQL Database is available at:   localhost:5432 (User: postgres)"
echo ""
echo "To shut everything down, run: bash stop_everything.sh"
echo "================================================================="