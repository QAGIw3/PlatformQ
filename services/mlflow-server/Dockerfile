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

# Copy the MLflow server extensions
COPY app/ app/

# Create non-root user
RUN useradd -m -u 1000 mlflow && \
    chown -R mlflow:mlflow /app

USER mlflow

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV MLFLOW_TRACKING_URI=http://0.0.0.0:5000

# Expose MLflow tracking server port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Run MLflow server
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"] 