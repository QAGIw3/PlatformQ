FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 sbtuser && chown -R sbtuser:sbtuser /app
USER sbtuser

# Expose port
EXPOSE 8053

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8053"] 