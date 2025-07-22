FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Java for PySpark
RUN apt-get update && apt-get install -y openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy shared libraries first
COPY libs/platformq-shared /app/libs/platformq-shared
COPY libs/platformq-events /app/libs/platformq-events

# Install shared libraries
RUN pip install -e /app/libs/platformq-shared
RUN pip install -e /app/libs/platformq-events

# Copy service requirements
COPY services/dataset-marketplace/requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy service code
COPY services/dataset-marketplace /app

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"] 