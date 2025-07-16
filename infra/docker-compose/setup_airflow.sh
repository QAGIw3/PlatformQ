#!/bin/bash
#
# Setup script for Apache Airflow integration with PlatformQ
# This script initializes Airflow and configures it for use with the platform

set -e

echo "=== PlatformQ Airflow Setup Script ==="
echo

# Check if running in the correct directory
if [ ! -f "docker-compose.airflow.yml" ]; then
    echo "Error: Please run this script from the infra/docker-compose directory"
    exit 1
fi

# Set Airflow UID
export AIRFLOW_UID=$(id -u)
echo "Setting AIRFLOW_UID=$AIRFLOW_UID"

# Create necessary directories
echo "Creating Airflow directories..."
mkdir -p dags logs plugins platformq_airflow

# Set permissions
echo "Setting directory permissions..."
chmod -R 775 dags logs plugins platformq_airflow

# Check if main services are running
echo "Checking if PlatformQ services are running..."
if ! docker-compose ps | grep -q "pulsar.*Up"; then
    echo "Warning: Pulsar service is not running. Please start main services first:"
    echo "  docker-compose up -d"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Start Airflow services
echo "Starting Airflow services..."
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml up -d

# Wait for Airflow to be ready
echo "Waiting for Airflow to initialize..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -s http://localhost:8082/health | grep -q "healthy"; then
        echo "Airflow is ready!"
        break
    fi
    echo -n "."
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
    echo
    echo "Warning: Airflow may not be fully initialized yet"
fi

echo
echo "=== Airflow Setup Complete ==="
echo
echo "Airflow Web UI: http://localhost:8082"
echo "Username: airflow"
echo "Password: airflow"
echo
echo "To enable Airflow integration in workflow-service, set:"
echo "  AIRFLOW_ENABLED=true"
echo
echo "Example commands:"
echo "  # View logs"
echo "  docker-compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f airflow_scheduler"
echo
echo "  # Stop Airflow"
echo "  docker-compose -f docker-compose.yml -f docker-compose.airflow.yml down"
echo
echo "  # Restart with changes"
echo "  docker-compose -f docker-compose.yml -f docker-compose.airflow.yml restart airflow_webserver" 