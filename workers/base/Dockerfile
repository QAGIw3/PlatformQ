FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install common Python packages for distributed computing
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pulsar-client==3.3.0 \
    minio==7.2.0 \
    prometheus-client==0.19.0 \
    opentelemetry-api==1.21.0 \
    opentelemetry-sdk==1.21.0 \
    opentelemetry-instrumentation==0.42b0 \
    numpy==1.26.2 \
    scipy==1.11.4 \
    pandas==2.1.3 \
    psutil==5.9.6

# Create app directory
WORKDIR /app

# Add worker base scripts
COPY worker_base.py /app/
COPY health_check.py /app/

# Environment variables for distributed compute
ENV SPARK_HOME=/opt/spark
ENV PULSAR_URL=pulsar://pulsar:6650
ENV MINIO_URL=minio:9000
ENV PROMETHEUS_PUSHGATEWAY_URL=pushgateway:9091

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python /app/health_check.py || exit 1

# Default command
CMD ["python", "/app/worker_base.py"] 