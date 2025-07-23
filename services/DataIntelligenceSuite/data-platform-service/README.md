# Data Platform Service v2.0

Consolidated data platform service that unifies data ingestion, storage, catalog, and lakehouse operations for DataIntelligenceSuite.

## Overview

This service consolidates functionality from multiple legacy services:
- **data-ingestion-service**: All ingestion capabilities (CDC, streaming, batch)
- **storage-service**: Unified storage abstraction with document conversion
- **data-catalog-hub**: Metadata management, lineage, and discovery
- **dih-service**: Digital Integration Hub capabilities
- **feature-store-service**: Feature management for ML pipelines
- **Lakehouse components**: Delta Lake, Iceberg, Hudi support

## Features

### Data Ingestion
- **Change Data Capture (CDC)**
  - Multi-source support: PostgreSQL, MySQL, MongoDB, Cassandra, Oracle, SQL Server, DB2
  - Real-time schema evolution
  - Automatic backpressure handling
  - ML-based optimization
  - Cost tracking and monitoring
- **Stream Ingestion**: Kafka, Pulsar, Kinesis integration
- **Batch Ingestion**: File-based, API, and scheduled ingestion
- **External Connectors**: CRM, ERP, webhooks, and custom APIs

### Storage Management
- **Multi-Backend Support**: MinIO, S3, Azure Blob, GCS
- **Document Conversion**: Automatic format conversion (PDF, DOCX, XLSX, etc.)
- **Preview Generation**: Thumbnails and text extraction
- **Intelligent Tiering**: Hot, warm, and cold storage tiers
- **Encryption & Compression**: At-rest encryption and smart compression
- **Quota Management**: Per-tenant storage limits and monitoring

### Data Catalog
- **Metadata Management**: Centralized metadata repository
- **Schema Registry**: Version control and compatibility checking
- **Data Lineage**: Track data flow and transformations
- **Business Glossary**: Map business terms to technical assets
- **Automated Classification**: PII, sensitive data detection
- **Impact Analysis**: Understand downstream effects of changes
- **Discovery & Search**: AI-powered search across all assets

### Lakehouse Operations
- **Multi-Format Support**: Iceberg, Delta Lake, Hudi
- **ACID Transactions**: Full transaction support
- **Time Travel**: Query historical versions
- **Schema Evolution**: Automatic schema migration
- **Partition Management**: Smart partitioning strategies
- **Compaction & Optimization**: Automatic file optimization

### Advanced Features
- **Multi-Engine Processing**: Spark, Ray, Dask, Flink, Beam
- **ML-Based Optimization**: Auto-scaling, adaptive batch sizing
- **Cost Management**: Resource tracking and optimization
- **Quality Integration**: Built-in data quality checks
- **Event-Driven Architecture**: React to data changes in real-time

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Data Platform Service v2.0              │
├─────────────────────────────────────────────────────────┤
│                      API Layer (v1/v2)                   │
├─────────────────────────────────────────────────────────┤
│  Ingestion  │  Storage  │  Catalog  │  Lakehouse Ops   │
├─────────────────────────────────────────────────────────┤
│              Common Processing Framework v2.0            │
├─────────────────────────────────────────────────────────┤
│  Batch Engines  │  Stream Engines  │  Quality Engines  │
├─────────────────────────────────────────────────────────┤
│  MinIO  │  Cassandra  │  JanusGraph  │  Elasticsearch  │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### V1 API (Legacy Compatibility)
- `/api/v1/ingestion/*` - Data ingestion operations
- `/api/v1/lake/*` - Lakehouse operations
- `/api/v1/connectors/*` - Connector management
- `/api/v1/schemas/*` - Schema registry
- `/api/v1/health/*` - Health checks
- `/api/v1/metrics/*` - Service metrics

### V2 API (Enhanced Features)
- `/api/v2/batch/*` - Multi-engine batch processing
- `/api/v2/stream/*` - Multi-engine stream processing
- `/api/v2/quality/*` - Data quality management
- `/api/v2/lakehouse/*` - Advanced lakehouse operations
- `/api/v2/catalog/*` - Unified catalog management
- `/api/v2/ml/*` - ML pipeline integration

## Configuration

Key environment variables:

```bash
# Service Configuration
SERVICE_NAME=data-platform-service
SERVICE_VERSION=2.0.0

# Lakehouse Paths
LAKEHOUSE_PATH=s3://platformq-lakehouse
DELTA_TABLE_PATH=${LAKEHOUSE_PATH}/delta
ICEBERG_TABLE_PATH=${LAKEHOUSE_PATH}/iceberg
HUDI_TABLE_PATH=${LAKEHOUSE_PATH}/hudi

# Processing Engines
SPARK_MASTER=spark://spark-master:7077
RAY_HEAD_ADDRESS=ray://ray-head:10001
DASK_SCHEDULER=tcp://dask-scheduler:8786
FLINK_JOBMANAGER=http://flink-jobmanager:8081

# Storage Systems
MINIO_ENDPOINT=minio:9000
CASSANDRA_HOSTS=cassandra-1,cassandra-2,cassandra-3
JANUSGRAPH_HOST=janusgraph
ELASTICSEARCH_HOSTS=elasticsearch:9200

# Messaging
PULSAR_SERVICE_URL=pulsar://pulsar-broker:6650
```

## Quick Start

### Using Docker

```bash
docker build -t data-platform-service:2.0.0 .
docker run -p 8000:8000 --env-file .env data-platform-service:2.0.0
```

### Using Kubernetes

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

## Usage Examples

### Batch Processing with Multiple Engines

```python
# Submit Spark job
POST /api/v2/batch/jobs
{
    "name": "customer-etl",
    "engine": "spark",
    "source_path": "s3://raw-data/customers",
    "target_path": "s3://processed/customers",
    "lakehouse_format": "delta",
    "transformations": [...],
    "quality_checks": ["completeness", "uniqueness"]
}

# Submit Ray job for ML preprocessing
POST /api/v2/batch/jobs
{
    "name": "feature-engineering",
    "engine": "ray",
    "source_path": "s3://processed/customers",
    "target_path": "s3://features/customers",
    "enable_ml_optimization": true
}
```

### Stream Processing

```python
# Create Flink streaming job
POST /api/v2/stream/pipelines
{
    "name": "real-time-analytics",
    "engine": "flink",
    "source": {
        "type": "pulsar",
        "topics": ["events"]
    },
    "transformations": [...],
    "sink": {
        "type": "delta",
        "path": "s3://real-time/events"
    }
}
```

### Data Quality

```python
# Run quality assessment
POST /api/v2/quality/assessments
{
    "dataset": "s3://processed/customers",
    "rules": [
        {
            "type": "completeness",
            "columns": ["email", "phone"]
        },
        {
            "type": "ml_anomaly",
            "model": "isolation_forest"
        }
    ]
}
```

## Monitoring

- **Metrics**: Prometheus metrics at `/metrics`
- **Health**: Health endpoint at `/api/v1/health`
- **Tracing**: OpenTelemetry integration with Jaeger
- **Dashboards**: Grafana dashboards available

## Development

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run with hot reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Load tests
locust -f tests/load/locustfile.py
```

## Migration from Legacy Services

This service replaces:
- data-ingestion-service
- storage-service  
- data-catalog-hub
- Parts of batch-processing-service
- Parts of analytics-service

All existing APIs are maintained in v1 for backward compatibility. New features are available in v2 API.

## License

Proprietary - PlatformQ
