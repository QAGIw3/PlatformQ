FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libgmp3-dev \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 zkpuser && chown -R zkpuser:zkpuser /app
USER zkpuser

# Expose port
EXPOSE 8052

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8052"] 