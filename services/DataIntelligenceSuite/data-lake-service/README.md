# Data Lake Service

A dedicated service for managing the data lake with medallion architecture, ingestion, transformation, and lifecycle management.

## Overview

The Data Lake Service provides:
- **Medallion Architecture**: Bronze, Silver, and Gold layers
- **Data Ingestion**: Batch and streaming ingestion
- **Data Transformation**: ETL/ELT processing
- **Lifecycle Management**: Retention, archival, and compaction
- **Schema Evolution**: Handle schema changes gracefully

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Data Lake Service                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │           Medallion Architecture                   │ │
│  │  ┌────────┐    ┌────────┐    ┌────────┐         │ │
│  │  │ Bronze │ -> │ Silver │ -> │  Gold  │         │ │
│  │  └────────┘    └────────┘    └────────┘         │ │
│  └───────────────────────────────────────────────────┘ │
│                                                         │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │  Ingestion   │  │ Transform   │  │  Lifecycle   │  │
│  │   Engine     │  │   Engine    │  │  Manager     │  │
│  └──────────────┘  └─────────────┘  └──────────────┘  │
│                                                         │
│  Storage: MinIO/S3 | Format: Parquet/Delta | Meta: Iceberg │
└─────────────────────────────────────────────────────────┘
```

## Features

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion, immutable storage
- **Silver Layer**: Cleansed and normalized data
- **Gold Layer**: Business-ready, aggregated data
- **Delta Lake**: ACID transactions and time travel
- **Apache Iceberg**: Table format for analytics

### Data Ingestion
- Batch file ingestion (CSV, JSON, Parquet, Avro)
- Streaming ingestion via Pulsar/Kafka
- API-based data push
- Change Data Capture (CDC)
- Schema inference and validation

### Data Transformation
- Spark-based transformations
- SQL transformations
- Data quality checks
- Deduplication
- Data enrichment

### Lifecycle Management
- Automated data retention
- Data archival to cold storage
- Compaction and optimization
- Partition management
- Cost optimization

## API Endpoints

### Data Ingestion
- `POST /api/v1/ingest/batch` - Batch data ingestion
- `POST /api/v1/ingest/stream` - Configure streaming ingestion
- `GET /api/v1/ingest/status/{job_id}` - Ingestion job status
- `POST /api/v1/ingest/validate` - Validate data before ingestion

### Data Access
- `GET /api/v1/data/{layer}/{dataset}` - List data files
- `GET /api/v1/data/{layer}/{dataset}/schema` - Get dataset schema
- `POST /api/v1/data/{layer}/{dataset}/query` - Query dataset
- `GET /api/v1/data/{layer}/{dataset}/partitions` - List partitions

### Transformation
- `POST /api/v1/transform/bronze-to-silver` - Transform to silver
- `POST /api/v1/transform/silver-to-gold` - Transform to gold
- `GET /api/v1/transform/jobs` - List transformation jobs
- `POST /api/v1/transform/custom` - Custom transformation

### Lifecycle Management
- `POST /api/v1/lifecycle/retention` - Configure retention
- `POST /api/v1/lifecycle/archive` - Archive old data
- `POST /api/v1/lifecycle/compact` - Compact small files
- `GET /api/v1/lifecycle/policies` - List policies

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: data-lake-service
SERVICE_PORT: 8000

# Storage Configuration
STORAGE_TYPE: minio
MINIO_ENDPOINT: minio:9000
MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}
MINIO_SECRET_KEY: ${MINIO_SECRET_KEY}

# Lake Configuration
BRONZE_BUCKET: lake-bronze
SILVER_BUCKET: lake-silver
GOLD_BUCKET: lake-gold

# Processing
SPARK_MASTER: spark://spark-master:7077
SPARK_EXECUTOR_MEMORY: 2g

# Table Formats
USE_DELTA_LAKE: true
USE_ICEBERG: true
ICEBERG_CATALOG: lake_catalog

# Retention Policies
BRONZE_RETENTION_DAYS: 90
SILVER_RETENTION_DAYS: 365
GOLD_RETENTION_DAYS: 1825
```

## Usage Examples

### Ingest Batch Data
```python
# Upload CSV file to bronze layer
with open('sales_data.csv', 'rb') as f:
    response = requests.post(
        'http://lake-service:8000/api/v1/ingest/batch',
        files={'file': f},
        data={
            'dataset': 'sales',
            'format': 'csv',
            'schema_inference': 'true',
            'partition_by': 'date,region'
        }
    )
```

### Transform Bronze to Silver
```python
response = requests.post('http://lake-service:8000/api/v1/transform/bronze-to-silver', json={
    "source_dataset": "sales",
    "target_dataset": "sales_cleaned",
    "transformations": [
        {
            "type": "clean",
            "config": {
                "remove_nulls": ["customer_id"],
                "standardize_dates": true,
                "deduplicate": ["order_id"]
            }
        },
        {
            "type": "quality_check",
            "config": {
                "rules": ["completeness > 0.95", "no_future_dates"]
            }
        }
    ]
})
```

### Query Gold Layer Data
```python
response = requests.post('http://lake-service:8000/api/v1/data/gold/sales_summary/query', json={
    "sql": "SELECT region, SUM(revenue) as total_revenue FROM sales_summary WHERE year = 2024 GROUP BY region",
    "output_format": "json"
})
```

## Data Formats

### Bronze Layer
- Raw formats: CSV, JSON, XML, Avro
- Compressed formats: GZIP, Snappy
- Partitioned by ingestion date

### Silver Layer
- Format: Parquet with Snappy compression
- Schema enforced
- Partitioned by business keys

### Gold Layer
- Format: Delta Lake or Iceberg tables
- Optimized for analytics
- Pre-aggregated views

## Best Practices

1. **Ingestion**: Always validate schema before ingestion
2. **Partitioning**: Choose partition keys based on query patterns
3. **Compaction**: Run regular compaction jobs for small files
4. **Retention**: Set appropriate retention for each layer
5. **Monitoring**: Track storage usage and query performance

## Integration

### With Query Service
```python
# Register dataset in query service after ingestion
await query_service.register_dataset({
    "name": "sales_gold",
    "location": "s3://gold/sales_summary",
    "format": "delta"
})
```

### With Quality Service
```python
# Run quality checks during transformation
quality_report = await quality_service.validate_dataset(
    dataset_id="sales_silver",
    rules=["completeness", "consistency", "accuracy"]
)
```

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 