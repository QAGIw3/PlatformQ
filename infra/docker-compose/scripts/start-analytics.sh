#!/bin/bash

set -e

echo "Starting PlatformQ Analytics Infrastructure..."

# Check if required directories exist
echo "Creating necessary directories..."
mkdir -p config/druid
mkdir -p config/ignite
mkdir -p config/trino/coordinator
mkdir -p config/trino/worker
mkdir -p config/trino/catalog
mkdir -p config/superset
mkdir -p init-scripts/postgres

# Set environment variables if not already set
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-analytics123}
export MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
export MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}
export MLFLOW_DB_PASSWORD=${MLFLOW_DB_PASSWORD:-mlflow123}
export SUPERSET_SECRET_KEY=${SUPERSET_SECRET_KEY:-$(openssl rand -base64 42)}

# Start the infrastructure
echo "Starting services..."
docker-compose -f docker-compose.analytics.yml up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until docker-compose -f docker-compose.analytics.yml exec -T postgres pg_isready -U analytics > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " PostgreSQL is ready!"

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
until curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " MinIO is ready!"

# Wait for Ignite to be ready
echo "Waiting for Apache Ignite to be ready..."
sleep 30  # Give Ignite time to start up

# Initialize Superset
echo "Initializing Superset..."
docker-compose -f docker-compose.analytics.yml exec -T superset superset db upgrade
docker-compose -f docker-compose.analytics.yml exec -T superset superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@platformq.io \
    --password admin
docker-compose -f docker-compose.analytics.yml exec -T superset superset init

# Create data source connections in Superset
echo "Creating data source connections..."
cat > /tmp/trino_database.json <<EOF
{
    "database_name": "Trino - Data Lake",
    "sqlalchemy_uri": "trino://trino-coordinator:8080/hive",
    "expose_in_sqllab": true,
    "allow_ctas": true,
    "allow_cvas": true,
    "allow_dml": true
}
EOF

cat > /tmp/druid_database.json <<EOF
{
    "database_name": "Druid - Time Series",
    "sqlalchemy_uri": "druid://druid-broker:8082/druid/v2/sql",
    "expose_in_sqllab": true
}
EOF

# Import databases to Superset (this would need the Superset API)
# For now, these need to be added manually through the UI

echo "Analytics infrastructure is ready!"
echo ""
echo "Access points:"
echo "  - Trino UI: http://localhost:8080"
echo "  - Druid Router: http://localhost:8888"
echo "  - Ignite REST: http://localhost:8080/ignite"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - MLflow UI: http://localhost:5000"
echo "  - Superset: http://localhost:8088 (admin/admin)"
echo ""
echo "To stop the infrastructure, run:"
echo "  docker-compose -f docker-compose.analytics.yml down"
echo ""
echo "To view logs, run:"
echo "  docker-compose -f docker-compose.analytics.yml logs -f [service-name]"

# Make the script executable
chmod +x scripts/start-analytics.sh 