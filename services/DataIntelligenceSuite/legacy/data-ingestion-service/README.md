# Data Ingestion Service

Unified service for data ingestion with medallion architecture, lifecycle management, external connectors, and comprehensive data lake capabilities.

## Overview

The Data Ingestion Service is a comprehensive platform that consolidates all data ingestion, lake management, and lifecycle capabilities. It features:
- **Medallion Architecture**: Bronze, Silver, and Gold data layers with quality transitions
- **Data Lifecycle Management**: Automated tiering across hot (Ignite), warm (Cassandra), and cold (MinIO) storage
- **External Connectors**: Integration with CRM, ERP, APIs, and webhooks
- **Traditional Capabilities**: CDC, streaming, batch processing, and schema management
- **Cost Optimization**: Automated data tiering and retention policies
- **Modern Lakehouse Formats**: Native support for Apache Iceberg and Delta Lake
- **Unified Event Streaming**: Seamless integration with Apache Pulsar and Kafka

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Ingestion Service 2.0                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────── Core Ingestion ───────────────────┐ │
│  │ ┌─────────────┐  ┌──────────────┐  ┌──────────────┐     │ │
│  │ │     CDC     │  │   Streaming  │  │    Batch     │     │ │
│  │ │   Manager   │  │   Ingestion  │  │  Ingestion   │     │ │
│  │ └─────────────┘  └──────────────┘  └──────────────┘     │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────── Medallion Architecture ───────────────┐ │
│  │  Bronze Layer → Silver Layer → Gold Layer                 │ │
│  │  (Raw Data)    (Cleansed)     (Business-Ready)           │ │
│  │  ┌────────┐    ┌────────┐    ┌────────┐                 │ │
│  │  │ MinIO  │ -> │ Delta  │ -> │Iceberg │                 │ │
│  │  └────────┘    └────────┘    └────────┘                 │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────── Lifecycle Management ─────────────────┐ │
│  │  HOT (Ignite) → WARM (Cassandra) → COLD (MinIO)          │ │
│  │  < 7 days       7-30 days          > 30 days             │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────── External Connectors ──────────────────┐ │
│  │  • SuiteCRM    • OpenStreetMap   • Generic Webhooks      │ │
│  │  • Metasfresh  • Custom APIs     • Event Streams         │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              Schema Registry & Governance                │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Features

### Medallion Architecture 🏅
- **Bronze Layer**: Raw data ingestion with immutable storage
- **Silver Layer**: Cleansed and validated data with quality checks
- **Gold Layer**: Business-ready aggregated data
- **Delta Lake & Iceberg**: ACID transactions and time travel
- **Automated Transitions**: Quality-based promotions between layers

### Modern Lakehouse Support 🏔️
- **Apache Iceberg**: Advanced table format with schema evolution
- **Delta Lake**: Unified batch and streaming with Z-ordering
- **Time Travel**: Query historical data at any point in time
- **ACID Transactions**: Full transactional guarantees
- **Schema Evolution**: Seamless schema changes without rewrites

### Data Lifecycle Management ♻️
- **Automated Tiering**: Hot → Warm → Cold storage transitions
- **Cost Optimization**: Up to 90% storage cost reduction
- **Retention Policies**: Configurable per data type
- **Access Pattern Analysis**: Smart data placement
- **Archival & Deletion**: Compliance-ready data management

### External Connectors 🔌
- **CRM Integration**: SuiteCRM with unified schema
- **ERP Integration**: Metasfresh for business data
- **API Connectors**: OpenStreetMap and custom APIs
- **Webhook Support**: Real-time event ingestion
- **Scheduled Syncs**: Cron-based data pulls

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
- Unified event backend abstraction
- Dynamic backend switching

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

### Medallion Architecture 🏅
- `POST /api/v1/lake/ingest/bronze` - Ingest data to bronze layer
- `POST /api/v1/lake/transform/bronze-to-silver` - Transform to silver layer
- `POST /api/v1/lake/aggregate/silver-to-gold` - Aggregate to gold layer
- `GET /api/v1/lake/layers/{dataset}` - Get dataset layer information
- `POST /api/v1/lake/optimize/{layer}/{dataset}` - Optimize storage

### Lakehouse Operations 🏔️
- `POST /api/v1/lake/tables/create` - Create Iceberg/Delta table
- `GET /api/v1/lake/tables/{table_name}` - Get table information
- `POST /api/v1/lake/tables/{table_name}/write` - Write data to table
- `GET /api/v1/lake/tables/{table_name}/read` - Read data from table
- `POST /api/v1/lake/tables/{table_name}/time-travel` - Query historical data
- `POST /api/v1/lake/tables/{table_name}/optimize` - Optimize table storage
- `GET /api/v1/lake/formats` - Get supported lakehouse formats

### Data Lifecycle ♻️
- `POST /api/v1/lake/lifecycle/policy` - Apply tiering policy
- `GET /api/v1/lake/lifecycle/cost-report` - Get storage cost report
- `GET /api/v1/lake/tiers/{dataset}` - Get data distribution
- `POST /api/v1/lake/upload/bronze` - Direct file upload to bronze

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

### Create Lakehouse Table
```python
# Create an Iceberg table
response = requests.post('http://localhost:8010/api/v1/lake/tables/create', json={
    "table_name": "customer_events",
    "schema": {
        "customer_id": "string",
        "event_type": "string",
        "event_time": "timestamp",
        "amount": "double",
        "metadata": "string"
    },
    "format": "iceberg",
    "partition_by": ["event_time"],
    "properties": {
        "write.format.default": "parquet",
        "commit.retry.num-retries": "3"
    }
})
```

### Write Data to Lakehouse
```python
# Write data to the table
response = requests.post('http://localhost:8010/api/v1/lake/tables/customer_events/write', json={
    "data": [
        {
            "customer_id": "cust_123",
            "event_type": "purchase",
            "event_time": "2024-01-15T10:30:00Z",
            "amount": 99.99,
            "metadata": "{\"product\": \"widget\"}"
        }
    ],
    "format": "iceberg",
    "mode": "append"
})
```

### Time Travel Query
```python
# Query data from yesterday
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=1)
response = requests.post('http://localhost:8010/api/v1/lake/tables/customer_events/time-travel', json={
    "timestamp": yesterday.isoformat(),
    "format": "iceberg"
})

# Or query by version
response = requests.post('http://localhost:8010/api/v1/lake/tables/customer_events/time-travel', json={
    "version": 5,
    "format": "delta"
})
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