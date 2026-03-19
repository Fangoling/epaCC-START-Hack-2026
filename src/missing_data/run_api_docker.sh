#!/bin/bash

echo "================================================="
echo "   Starting Python Missing Data API in Docker    "
echo "================================================="

cd src/missing_data

# 1. Build the Docker image
echo "[INFO] Building Docker image 'missing-data-api'..."
docker build -t missing-data-api .

# 2. Stop and remove any existing container with the same name
if [ "$(docker ps -aq -f name=missing-data-api-container)" ]; then
    echo "[INFO] Removing old container 'missing-data-api-container'..."
    docker rm -f missing-data-api-container
fi

# 3. Run the container and link it to the DB container explicitly!
# By using --link or putting them on the same custom network, we avoid the localhost/host.docker.internal trap entirely.
# We will use the container name `case-db` as the host.

echo "[INFO] Starting container on port 8000..."
docker run -d \
    --name missing-data-api-container \
    --link case-db:case-db \
    -e DB_HOST=case-db \
    -p 8000:8000 \
    missing-data-api

echo "================================================="
echo " ✅ Python API is now running in Docker!         "
echo " 🌐 API is listening on:                         "
echo "    http://localhost:8000                        "
echo "================================================="
echo "To stop the API, run: docker stop missing-data-api-container"