#!/bin/bash
# Script to set up Cassandra schema for CAD collaboration service

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
CASSANDRA_USER="${CASSANDRA_USER:-cassandra}"
CASSANDRA_PASSWORD="${CASSANDRA_PASSWORD:-cassandra}"

echo "Setting up Cassandra schema for CAD collaboration service..."

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to be ready..."
for i in {1..30}; do
    if cqlsh -u "$CASSANDRA_USER" -p "$CASSANDRA_PASSWORD" "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e "DESC KEYSPACES;" > /dev/null 2>&1; then
        echo "Cassandra is ready!"
        break
    fi
    echo "Waiting for Cassandra... ($i/30)"
    sleep 2
done

# Execute the schema
echo "Creating schema..."
cqlsh -u "$CASSANDRA_USER" -p "$CASSANDRA_PASSWORD" "$CASSANDRA_HOST" "$CASSANDRA_PORT" -f /app/scripts/create_cassandra_schema.cql

if [ $? -eq 0 ]; then
    echo "Schema created successfully!"
else
    echo "Failed to create schema"
    exit 1
fi 