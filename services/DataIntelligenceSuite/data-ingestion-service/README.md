# Data Ingestion Service

Unified service for data ingestion from multiple sources including Change Data Capture (CDC), streaming, batch processing, schema management, and external data connectors.

## Overview

The Data Ingestion Service consolidates all data ingestion capabilities into a single, scalable service. It supports real-time CDC from databases, streaming data consumption, batch file imports, maintains a central schema registry, and now includes connector functionality for external systems integration.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Data Ingestion Service                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │     CDC     │  │   Streaming  │  │    Batch     │  │
│  │   Manager   │  │   Ingestion  │  │  Ingestion   │  │
│  └─────────────┘  └──────────────┘  └──────────────┘  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Schema Registry                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  Sources:                    Destinations:              │
│  • PostgreSQL               • Data Lake (MinIO)         │
│  • MySQL                    • Hot Storage (Cassandra)   │
│  • MongoDB                  • Stream Topics (Pulsar)    │
│  • Pulsar/Kafka            • Cache (Ignite)            │
│  • S3/MinIO                                            │
└─────────────────────────────────────────────────────────┘
```

## Features

### Change Data Capture (CDC)
- Real-time database change tracking
- Support for PostgreSQL, MySQL, MongoDB
- Automatic schema evolution handling
- Configurable snapshot and incremental modes

### Stream Ingestion
- Multi-source streaming (Pulsar, Kafka, Kinesis)
- Schema validation and transformation
- Dead letter queue handling
- Exactly-once semantics

### Batch Ingestion
- File upload support (CSV, JSON, Parquet, Avro)
- S3/MinIO synchronization
- Scheduled batch jobs
- Data validation and cleansing

### Schema Registry
- Centralized schema management
- Version control for schemas
- Backward compatibility checking
- Schema evolution support

### External Connectors
- CRM Integration (SuiteCRM, Metasfresh)
- API Connectors (OpenStreetMap)
- Webhook Support
- Scheduled synchronization
- Data transformation

## API Endpoints

### CDC Management
- `POST /api/v1/cdc/sources` - Create CDC source
- `GET /api/v1/cdc/sources` - List CDC sources
- `DELETE /api/v1/cdc/sources/{id}` - Delete CDC source
- `GET /api/v1/cdc/sources/{id}/status` - Get source status

### Stream Ingestion
- `POST /api/v1/streams` - Create stream ingestion
- `GET /api/v1/streams` - List stream ingestions
- `DELETE /api/v1/streams/{id}` - Delete stream

### Batch Ingestion
- `POST /api/v1/batch` - Create batch job
- `POST /api/v1/batch/upload` - Upload file for ingestion
- `GET /api/v1/batch/{job_id}/status` - Get job status

### Schema Registry
- `POST /api/v1/schemas` - Register schema
- `GET /api/v1/schemas/{id}` - Get schema
- `GET /api/v1/schemas/{id}/versions` - List schema versions
- `POST /api/v1/schemas/validate` - Validate data against schema

### Connector Management
- `GET /api/v1/connectors` - List all connectors
- `POST /api/v1/connectors` - Create new connector
- `DELETE /api/v1/connectors/{id}` - Delete connector
- `POST /api/v1/connectors/{id}/trigger` - Manually trigger connector
- `GET /api/v1/connectors/{id}/status` - Get connector status
- `POST /api/v1/connectors/webhook/{type}` - Receive webhook data

## Configuration

```yaml
# Environment variables
SERVICE_NAME: data-ingestion-service
SERVICE_PORT: 8000

# CDC Configuration
CDC_SNAPSHOT_MODE: initial
CDC_POLL_INTERVAL: 5000

# Streaming Configuration
PULSAR_URL: pulsar://pulsar:6650
KAFKA_BOOTSTRAP_SERVERS: kafka:9092

# Storage Configuration
MINIO_ENDPOINT: minio:9000
CASSANDRA_HOSTS: cassandra:9042

# Schema Registry
SCHEMA_COMPATIBILITY: BACKWARD
SCHEMA_CACHE_SIZE: 1000
```

## Usage Examples

### Create CDC Source
```python
import requests

response = requests.post('http://localhost:8010/api/v1/cdc/sources', json={
    "source_type": "postgresql",
    "connection_string": "postgresql://user:pass@host/db",
    "tables": ["users", "orders"],
    "start_position": "latest"
})
```

### Upload File for Batch Ingestion
```python
with open('data.csv', 'rb') as f:
    response = requests.post(
        'http://localhost:8010/api/v1/batch/upload',
        files={'file': f},
        data={'destination_table': 'sales_data'}
    )
```

### Create Stream Ingestion
```python
response = requests.post('http://localhost:8010/api/v1/streams', json={
    "source_type": "pulsar",
    "topics": ["user-events", "system-events"],
    "consumer_group": "ingestion-service",
    "schema_id": "user-event-v1"
})
```

### Create External Connector
```python
# Create SuiteCRM connector
response = requests.post('http://localhost:8010/api/v1/connectors', json={
    "connector_id": "crm-sync",
    "config": {
        "type": "suitecrm",
        "base_url": "https://crm.example.com",
        "username": "api_user",
        "password": "api_password",
        "schedule": "0 */2 * * *"  # Every 2 hours
    }
})

# Trigger connector manually
response = requests.post('http://localhost:8010/api/v1/connectors/crm-sync/trigger')
```

## Deployment

### Docker
```bash
docker build -t data-ingestion-service .
docker run -p 8010:8000 data-ingestion-service
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-ingestion-service
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: data-ingestion-service
        image: data-ingestion-service:latest
        ports:
        - containerPort: 8000
```

## Monitoring

The service exposes Prometheus metrics:
- `ingestion_records_total` - Total records ingested
- `ingestion_bytes_total` - Total bytes processed
- `ingestion_errors_total` - Total ingestion errors
- `cdc_lag_seconds` - CDC replication lag

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 