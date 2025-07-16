# --- Builder Stage ---
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install all dependencies
COPY ./requirements.txt /app/
COPY ../shared /app/shared
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY ./app /app/app
COPY ./scripts /app/scripts


# --- Final Stage ---
FROM python:3.11-slim as final

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd --create-home appuser
USER appuser

# Copy installed dependencies from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy application code
COPY --from=builder /app/app /app/app
COPY --from=builder /app/shared /app/shared

# Make port 80 available
EXPOSE 80

# Run uvicorn server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"] 