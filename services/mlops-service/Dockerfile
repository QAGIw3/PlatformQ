FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service code
COPY app/ app/
COPY ../../libs/platformq-shared /app/libs/platformq-shared
COPY ../../libs/platformq-events /app/libs/platformq-events

# Create non-root user
RUN useradd -m -u 1000 mlops && \
    chown -R mlops:mlops /app

USER mlops

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV SERVICE_NAME=mlops-service

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"] 