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

# 3. Run the container
# If running locally without docker-compose, use --network="host" to hit localhost DB
# However, Mac OS Docker doesn't support host networking correctly out of the box for hitting other containers via localhost
# So we pass DB_HOST=host.docker.internal to allow Python inside this container to talk to the DB container!
echo "[INFO] Starting container on port 8000..."
docker run -d --name missing-data-api-container -e DB_HOST=host.docker.internal -p 8000:8000 missing-data-api

echo "================================================="
echo " ✅ Python API is now running in Docker!         "
echo " 🌐 API is listening on:                         "
echo "    http://localhost:8000                        "
echo "================================================="
echo "To stop the API, run: docker stop missing-data-api-container"