#!/bin/bash

# Cognitive Orchestration Service startup script

echo "Starting Cognitive Orchestration Service..."

# Create necessary directories
mkdir -p /app/models
mkdir -p /app/logs

# Wait for dependencies
echo "Waiting for dependencies..."
sleep 10

# Start the service
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1 