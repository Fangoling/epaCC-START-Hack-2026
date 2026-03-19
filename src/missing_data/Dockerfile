# Step 1: Base image
FROM python:3.11-slim

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