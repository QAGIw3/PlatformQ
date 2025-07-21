FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.in .
RUN pip install --no-cache-dir pip-tools && \
    pip-compile requirements.in && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create necessary directories
RUN mkdir -p /tmp/analytics-reports /tmp/analytics-models

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app /tmp/analytics-reports /tmp/analytics-models
USER appuser

# Expose ports
EXPOSE 8092 9097

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8092/health').raise_for_status()"

# Run the application
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8092"] 