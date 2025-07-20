#!/bin/bash
# Start PlatformQ Service Mesh

echo "Starting Consul servers..."
docker-compose -f docker-compose.service-mesh.yml up -d consul-server-1 consul-server-2 consul-server-3

echo "Waiting for Consul cluster to form..."
sleep 10

echo "Starting infrastructure services..."
docker-compose -f docker-compose.service-mesh.yml up -d vault ignite pulsar cassandra elasticsearch minio

echo "Waiting for infrastructure to be ready..."
sleep 20

echo "Starting JanusGraph and OPA..."
docker-compose -f docker-compose.service-mesh.yml up -d janusgraph opa

echo "Starting all application services..."
docker-compose -f docker-compose.service-mesh.yml up -d

echo "Service mesh started! Consul UI available at http://localhost:8500"
