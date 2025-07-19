# Data Platform Service

A comprehensive data platform service providing unified data management, analytics, and governance capabilities with Apache Druid integration for time-series analytics.

## Features

### Core Capabilities
- **Unified Data Lake**: Medallion architecture (Bronze/Silver/Gold zones) with Iceberg and Delta Lake support
- **Time-Series Analytics**: Apache Druid integration for OLAP queries and real-time analytics
- **Data Catalog**: Comprehensive metadata management and discovery
- **Data Quality**: Automated profiling, validation, and remediation
- **Data Governance**: Policy enforcement, access control, and compliance tracking
- **Feature Store**: ML feature management with online/offline serving
- **Data Lineage**: End-to-end data flow tracking and impact analysis
- **Pipeline Management**: SeaTunnel-based ETL/ELT pipelines

### Integrations
- **Apache Druid**: Time-series analytics and OLAP queries
- **Apache Ignite**: In-memory computing and caching
- **Apache Spark**: Large-scale data processing
- **Apache Pulsar**: Event streaming and messaging
- **Elasticsearch**: Full-text search and analytics
- **JanusGraph**: Graph database for relationships
- **MinIO**: Object storage for data lake

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Data Platform Service                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Data Lake  │  │   Analytics   │  │  Governance  │ │
│  │  (Iceberg/   │  │   (Druid)     │  │   & Quality  │ │
│  │   Delta)     │  └──────────────┘  └──────────────┘ │
│  └─────────────┘                                       │
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │  Feature     │  │   Pipeline    │  │   Lineage    │ │
│  │   Store      │  │  (SeaTunnel)  │  │   Tracking   │ │
│  └─────────────┘  └──────────────┘  └──────────────┘ │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Druid Analytics Integration

The service includes Apache Druid for advanced time-series analytics:

### Time-Series Queries
```python
# Query time-series data
POST /api/v1/analytics/timeseries
{
    "datasource": "user_events",
    "metrics": ["page_views", "clicks"],
    "granularity": "hour",
    "filter": {"country": "US"},
    "start_time": "2024-01-01T00:00:00Z",
    "end_time": "2024-01-02T00:00:00Z"
}
```

### Group-By Analytics
```python
# Aggregate by dimensions
POST /api/v1/analytics/groupby
{
    "datasource": "sales_data",
    "dimensions": ["product", "region"],
    "metrics": ["revenue", "quantity"],
    "limit": 100
}
```

### Data Ingestion
```python
# Ingest data into Druid
POST /api/v1/analytics/ingest
{
    "datasource": "events",
    "data": [
        {"timestamp": "2024-01-01T10:00:00Z", "user_id": 123, "event": "click"},
        {"timestamp": "2024-01-01T10:01:00Z", "user_id": 456, "event": "view"}
    ],
    "timestamp_column": "timestamp"
}
```

## API Endpoints

### Analytics APIs
- `POST /api/v1/analytics/timeseries` - Query time-series data
- `POST /api/v1/analytics/groupby` - Execute group-by queries
- `POST /api/v1/analytics/ingest` - Ingest data into Druid
- `GET /api/v1/analytics/datasources` - List available datasources

### Data Lake APIs
- `POST /api/v1/lake/datasets` - Create dataset
- `GET /api/v1/lake/datasets` - List datasets
- `POST /api/v1/lake/ingest` - Ingest data
- `POST /api/v1/lake/query` - Query data

### Catalog APIs
- `POST /api/v1/catalog/assets` - Register data asset
- `GET /api/v1/catalog/search` - Search catalog
- `GET /api/v1/catalog/lineage/{asset_id}` - Get lineage

### Feature Store APIs
- `POST /api/v1/features/groups` - Create feature group
- `POST /api/v1/features/serve` - Get online features
- `POST /api/v1/features/historical` - Get historical features

## Configuration

### Environment Variables
```bash
# Vault Integration
VAULT_ADDR=http://vault:8200
VAULT_ROLE_ID=your-role-id
VAULT_SECRET_ID=your-secret-id

# Consul Integration
CONSUL_HOST=consul
CONSUL_PORT=8500

# Druid Configuration
DRUID_BROKER_URL=http://druid-broker:8082
DRUID_COORDINATOR_URL=http://druid-coordinator:8081
DRUID_OVERLORD_URL=http://druid-overlord:8090

# Spark Configuration
SPARK_MASTER=spark://spark-master:7077
SPARK_EXECUTOR_MEMORY=4g

# Storage
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Services**
   ```bash
   docker-compose up -d
   ```

3. **Initialize Platform**
   ```bash
   python scripts/bootstrap_platform.py
   ```

4. **Access API**
   ```
   http://localhost:8000/docs
   ```

## Security

- **Vault Integration**: Dynamic credentials and encryption
- **Consul Integration**: Service discovery and configuration
- **mTLS**: Service-to-service encryption
- **RBAC**: Role-based access control
- **Data Encryption**: At-rest and in-transit

## Monitoring

- **Metrics**: Prometheus metrics exposed at `/metrics`
- **Tracing**: OpenTelemetry integration
- **Health Check**: Available at `/health`
- **Druid Monitoring**: Analytics performance metrics

## Development

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
# Linting
flake8 app/

# Type checking
mypy app/
```

## License

Proprietary - All rights reserved 