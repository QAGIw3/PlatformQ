# --- Builder Stage ---
FROM python:3.11-slim as builder
WORKDIR /app
COPY ./requirements.txt /app/
COPY ../shared /app/shared
RUN pip install --no-cache-dir -r requirements.txt
COPY ./app /app/app
COPY ../../schemas /app/schemas

# --- Final Stage ---
FROM debian:bullseye-slim

# Install LibreOffice and Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice-headless \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copy python dependencies and application code from builder stage
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /app/app /app/app
COPY --from=builder /app/schemas /app/schemas

# Set the entrypoint
CMD ["python3", "app/main.py"] 