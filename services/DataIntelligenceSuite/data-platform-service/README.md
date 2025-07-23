# Data Platform Service v2.0

Consolidated data platform service that unifies data ingestion, storage, catalog, and lakehouse operations for PlatformQ.

## Overview

This service consolidates functionality from multiple legacy services:
- **data-ingestion-service**: All ingestion capabilities (CDC, streaming, batch)
- **storage-service**: Unified storage abstraction
- **data-catalog-hub**: Metadata management and discovery
- **Lakehouse components**: Delta Lake, Iceberg, Hudi support

## Features

### Core Capabilities
- **Multi-Engine Batch Processing**: Spark, Ray, Dask, Pandas
- **Multi-Engine Stream Processing**: Flink, Beam, Bytewax, Native
- **Unified Lakehouse**: Delta Lake, Iceberg, Hudi with automatic optimization
- **Data Quality**: Integrated quality checks with ML-based anomaly detection
- **Metadata Catalog**: Comprehensive data discovery and lineage tracking

### Advanced Features
- **Automatic Partitioning**: ML-based partition optimization
- **Resource Management**: Dynamic resource allocation and cost optimization
- **Backpressure Handling**: Adaptive rate limiting and flow control
- **Schema Evolution**: Automatic schema migration and versioning
- **Time Travel**: Query historical data across all lakehouse formats

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
