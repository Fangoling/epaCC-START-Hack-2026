#!/bin/bash

# =================================================================
#  epaCC-START-Hack-2026 - Master Start Script
#  Spins up the entire stack: Database, Backend API, and Frontend UI
# =================================================================

# Exit immediately if a command exits with a non-zero status
set -e

echo "🚀 Starting the epaCC Data Ingestion Stack..."
echo "-----------------------------------------------------------------"

# Step 1: Start the MS SQL Server Database
echo "📦 [Step 1/5] Setting up the MS SQL Server Database..."
bash DB/setup_mssql_docker.sh

# Step 2: Inject Mock Data (with missing values to demonstrate the tool)
echo "🧬 [Step 2/5] Injecting test data with missing values..."
bash DB/test-seeds/insert_mock_data_missing.sh

# Step 3: Run the data ingestion pipeline (CSV files → MSSQL)
echo "🔄 [Step 3/5] Running data ingestion pipeline into MSSQL..."
python run.py "Endtestdaten_ohne_Fehler_ einheitliche ID/" --glob "*.csv"

# Step 4: Start the Backend API (FastAPI)
echo "⚙️  [Step 4/5] Starting the Python Missing Data API..."
# We run this in the background using '&' so it doesn't block the script
bash src/missing_data/run_api_docker.sh > /dev/null 2>&1 &
# Give the API a few seconds to fully boot inside its container
sleep 3

# Step 5: Start the Frontend Dashboard (React)
echo "🖥️  [Step 5/5] Starting the React Interactive Dashboard..."
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
echo "👉 The MS SQL Server DB is available at:      localhost:1433 (User: SA)"
echo ""
echo "To shut everything down, run: bash stop_everything.sh"
echo "================================================================="