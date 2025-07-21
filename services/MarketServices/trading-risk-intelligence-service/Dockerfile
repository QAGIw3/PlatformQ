# --- Builder Stage ---
FROM python:3.11-slim as builder

WORKDIR /app

# Install dependencies
COPY ./requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ./app /app/app

# --- Final Stage ---
FROM python:3.11-slim as final

WORKDIR /app

# Create a non-root user for security
RUN useradd --create-home appuser
USER appuser

# Copy installed dependencies from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy application code
COPY --from=builder /app/app /app/app

# Make port 80 available
EXPOSE 80

# Run uvicorn server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"] 