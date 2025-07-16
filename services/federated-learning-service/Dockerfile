FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service code
COPY app/ app/
COPY ../../libs/shared /app/libs/shared
COPY ../../processing/spark/ml /app/processing/spark/ml

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV SERVICE_NAME=federated-learning-service

# Expose port
EXPOSE 8000

# Run the service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"] 