#!/bin/bash

echo "================================================="
echo "   Starting React Dashboard in Docker            "
echo "================================================="

# Navigate to the dashboard directory so Docker build context is correct
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 1. Build the Docker image
echo "[INFO] Building Docker image 'missing-data-dashboard'..."
docker build -t missing-data-dashboard .

# 2. Stop and remove any existing container with the same name
if [ "$(docker ps -aq -f name=dashboard-ui)" ]; then
    echo "[INFO] Removing old container 'dashboard-ui'..."
    docker rm -f dashboard-ui
fi

# 3. Run the container
echo "[INFO] Starting dashboard container on port 3000..."
docker run -d \
    --name dashboard-ui \
    -p 3000:3000 \
    missing-data-dashboard

echo "================================================="
echo " ✅ Dashboard is now running in Docker!          "
echo " 🌐 Dashboard is available at:                   "
echo "    http://localhost:3000                        "
echo "================================================="
echo "To stop the dashboard, run: docker stop dashboard-ui"
