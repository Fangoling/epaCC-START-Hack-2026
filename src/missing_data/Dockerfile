# Step 1: Base image
FROM python:3.11-slim

# Install system dependencies for the MS SQL Server ODBC Driver
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Step 2: Set working directory
WORKDIR /app

# Step 3: Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Step 4: Copy the python files
COPY . .

# Step 5: Expose the port FastAPI runs on
EXPOSE 8000

# Step 6: Start the FastAPI server using Uvicorn
CMD ["uvicorn", "missing_data_api:app", "--host", "0.0.0.0", "--port", "8000"]