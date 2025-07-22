# Data Catalog Service

A comprehensive metadata management and data discovery platform built on Apache Atlas, providing centralized governance, lineage tracking, and semantic search capabilities across the entire data platform.

## Overview

The Data Catalog Service serves as the central nervous system for data governance, providing:
- **Metadata Management**: Centralized repository for all data assets
- **Data Lineage**: End-to-end tracking of data transformations
- **Schema Registry**: Version-controlled schema management
- **Discovery & Search**: Semantic search across all data assets
- **Classification & Tagging**: Automated and manual data classification
- **Business Glossary**: Business-friendly data definitions
- **Access Control**: Fine-grained permissions on metadata

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Catalog Service                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │ Atlas Client │  │ Schema Registry │  │ Lineage Tracker  │  │
│  │  & Manager   │  │     (Avro)      │  │   & Processor    │  │
│  └──────────────┘  └─────────────────┘  └──────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Search & Discovery                      │  │
│  │  - Semantic Search  - Faceted Search  - Recommendations   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │ Classifier & │  │Business Glossary│  │  Access Control  │  │
│  │   Tagger     │  │    Manager      │  │    & Audit       │  │
│  └──────────────┘  └─────────────────┘  └──────────────────┘  │
│                                                                  │
│  Backend: Atlas | Cache: Ignite | Events: Pulsar               │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. Apache Atlas Integration
- **Type System**: Define custom types for platform entities
- **Entity Management**: Create, update, delete metadata entities
- **Relationship Tracking**: Model relationships between data assets
- **Bulk Operations**: Efficient bulk import/export
- **REST API Wrapper**: Simplified Atlas API interactions

### 2. Schema Registry
- **Multi-Format Support**: Avro, JSON Schema, Protobuf, Parquet
- **Version Control**: Track schema evolution over time
- **Compatibility Checking**: Forward/backward compatibility validation
- **Schema Inference**: Auto-detect schemas from data samples
- **Integration**: Seamless with streaming and batch services

### 3. Data Lineage
- **Automated Capture**: Hook into all data processing services
- **Visual Representation**: Graph-based lineage visualization
- **Impact Analysis**: Understand downstream effects of changes
- **Time Travel**: View lineage at different points in time
- **Cross-Service**: Track lineage across all platform services

### 4. Search & Discovery
- **Full-Text Search**: Elasticsearch-powered semantic search
- **Faceted Search**: Filter by type, owner, tags, classifications
- **Relevance Ranking**: ML-based result ranking
- **Query Suggestions**: Auto-complete and did-you-mean
- **Saved Searches**: Personal and shared search collections

### 5. Classification & Tagging
- **Auto-Classification**: ML-based sensitive data detection
- **Custom Classifiers**: Define business-specific classifications
- **Tag Hierarchies**: Nested tag structures
- **Propagation Rules**: Automatic tag inheritance
- **Compliance Tags**: GDPR, CCPA, HIPAA markers

### 6. Business Glossary
- **Term Management**: Define business terms and meanings
- **Relationships**: Link terms to technical assets
- **Approval Workflow**: Govern term definitions
- **Multi-Language**: Support for internationalization
- **Export/Import**: Bulk glossary management

### 7. Access Control
- **Role-Based**: Define roles for metadata access
- **Attribute-Based**: Fine-grained permissions
- **Audit Trail**: Complete history of metadata changes
- **Integration**: With platform's auth service
- **Delegation**: Metadata ownership management

## API Endpoints

### Entity Management
- `POST /api/v1/entities` - Create new entity
- `GET /api/v1/entities/{guid}` - Get entity by GUID
- `PUT /api/v1/entities/{guid}` - Update entity
- `DELETE /api/v1/entities/{guid}` - Delete entity
- `POST /api/v1/entities/bulk` - Bulk create/update

### Schema Registry
- `POST /api/v1/schemas` - Register new schema
- `GET /api/v1/schemas/{id}` - Get schema by ID
- `GET /api/v1/schemas/{id}/versions` - List schema versions
- `POST /api/v1/schemas/validate` - Validate schema compatibility
- `POST /api/v1/schemas/infer` - Infer schema from data

### Search & Discovery
- `GET /api/v1/search` - Full-text search
- `POST /api/v1/search/advanced` - Advanced search with filters
- `GET /api/v1/search/suggestions` - Get search suggestions
- `GET /api/v1/discovery/related/{guid}` - Find related entities
- `GET /api/v1/discovery/recommendations` - Get recommendations

### Lineage
- `GET /api/v1/lineage/{guid}` - Get lineage for entity
- `POST /api/v1/lineage` - Create lineage relationship
- `GET /api/v1/lineage/impact/{guid}` - Impact analysis
- `GET /api/v1/lineage/graph/{guid}` - Get lineage graph

### Classification & Tags
- `GET /api/v1/classifications` - List classifications
- `POST /api/v1/classifications` - Create classification
- `POST /api/v1/entities/{guid}/classifications` - Assign classification
- `POST /api/v1/entities/{guid}/tags` - Add tags
- `POST /api/v1/classify/auto` - Auto-classify data

### Business Glossary
- `GET /api/v1/glossary` - List glossary terms
- `POST /api/v1/glossary/terms` - Create term
- `PUT /api/v1/glossary/terms/{id}` - Update term
- `POST /api/v1/glossary/terms/{id}/assign` - Link term to entity

### Monitoring
- `GET /api/v1/stats` - Catalog statistics
- `GET /api/v1/audit` - Audit log
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: data-catalog-service
SERVICE_PORT: 8017
ENVIRONMENT: production

# Apache Atlas
ATLAS_URL: http://atlas:21000
ATLAS_USERNAME: admin
ATLAS_PASSWORD: ${ATLAS_PASSWORD}
ATLAS_CLIENT_TIMEOUT: 30

# Schema Registry
SCHEMA_REGISTRY_ENABLED: true
SCHEMA_COMPATIBILITY_DEFAULT: BACKWARD
SCHEMA_CACHE_SIZE: 1000

# Search Configuration
SEARCH_ENGINE: elasticsearch
ELASTICSEARCH_HOSTS: ["elasticsearch:9200"]
SEARCH_INDEX_PREFIX: catalog_
SEARCH_RESULT_LIMIT: 100

# Lineage Processing
LINEAGE_BATCH_SIZE: 100
LINEAGE_PROCESSING_INTERVAL: 60  # seconds
LINEAGE_RETENTION_DAYS: 365

# Classification
AUTO_CLASSIFICATION_ENABLED: true
CLASSIFICATION_SCAN_INTERVAL: 300  # 5 minutes
PII_DETECTION_ENABLED: true

# Storage
IGNITE_HOST: ignite
IGNITE_PORT: 10800
CACHE_TTL: 300  # 5 minutes

# Events
PULSAR_URL: pulsar://pulsar:6650
EVENT_TOPIC_PREFIX: catalog-events-

# Integration
AUTH_SERVICE_URL: http://auth-service:8001
QUALITY_SERVICE_URL: http://unified-quality-service:8015
```

## Data Model

### Core Entity Types

```python
# Dataset Entity
{
  "typeName": "dataset",
  "attributes": {
    "name": "customer_data",
    "qualifiedName": "postgres.sales.customer_data",
    "description": "Customer master data",
    "owner": "data-team",
    "format": "table",
    "location": "postgres://host/db/table",
    "schema": {...},
    "statistics": {
      "rowCount": 1000000,
      "sizeBytes": 104857600,
      "lastModified": "2024-01-01T00:00:00Z"
    }
  }
}

# Process Entity (for lineage)
{
  "typeName": "process",
  "attributes": {
    "name": "customer_etl",
    "qualifiedName": "airflow.dags.customer_etl",
    "description": "Customer data ETL pipeline",
    "inputs": [{"guid": "dataset-guid-1"}],
    "outputs": [{"guid": "dataset-guid-2"}],
    "processType": "BATCH",
    "schedule": "0 2 * * *"
  }
}

# Column Entity
{
  "typeName": "column",
  "attributes": {
    "name": "email",
    "dataType": "string",
    "isPrimaryKey": false,
    "isNullable": true,
    "classifications": ["PII", "EMAIL"]
  }
}
```

## Usage Examples

### Register a Dataset
```python
# Register a new dataset with schema
response = requests.post('http://catalog:8017/api/v1/entities', json={
    "entity": {
        "typeName": "dataset",
        "attributes": {
            "name": "orders",
            "qualifiedName": "kafka.production.orders",
            "format": "stream",
            "location": "kafka://broker:9092/orders",
            "owner": "order-service"
        }
    },
    "schema": {
        "type": "avro",
        "schema": {...}  # Avro schema definition
    }
})
```

### Search for Data Assets
```python
# Search for all customer-related datasets
response = requests.get('http://catalog:8017/api/v1/search', params={
    "query": "customer",
    "typeName": "dataset",
    "limit": 20,
    "filters": {
        "owner": "data-team",
        "classifications": ["PII"]
    }
})

# Advanced search with facets
response = requests.post('http://catalog:8017/api/v1/search/advanced', json={
    "query": "*",
    "filters": {
        "typeName": ["dataset", "table"],
        "classifications": ["SENSITIVE"],
        "tags": ["production"]
    },
    "facets": ["owner", "format", "classifications"]
})
```

### Track Data Lineage
```python
# Create lineage for ETL process
response = requests.post('http://catalog:8017/api/v1/lineage', json={
    "process": {
        "typeName": "spark_job",
        "attributes": {
            "name": "customer_aggregation",
            "qualifiedName": "spark.jobs.customer_aggregation"
        }
    },
    "inputs": [
        {"guid": "dataset-guid-1"},
        {"guid": "dataset-guid-2"}
    ],
    "outputs": [
        {"guid": "dataset-guid-3"}
    ]
})

# Get upstream lineage
response = requests.get(f'http://catalog:8017/api/v1/lineage/{dataset_guid}', params={
    "direction": "upstream",
    "depth": 3
})
```

### Auto-Classification
```python
# Trigger auto-classification for a dataset
response = requests.post('http://catalog:8017/api/v1/classify/auto', json={
    "entityGuid": dataset_guid,
    "sampleSize": 1000,
    "classifiers": ["pii", "financial", "healthcare"]
})
```

## Integration Examples

### With Data Ingestion Service
```python
# Ingestion service registers dataset after ingestion
async def register_ingested_dataset(dataset_info):
    await catalog_client.create_entity(
        type_name="dataset",
        attributes={
            "name": dataset_info.name,
            "location": dataset_info.path,
            "format": dataset_info.format,
            "ingestionTime": datetime.utcnow(),
            "source": dataset_info.source
        }
    )
```

### With Stream Processing
```python
# Stream processor updates lineage
async def update_stream_lineage(job_id, input_topics, output_topics):
    await catalog_client.create_lineage(
        process_name=f"flink_job_{job_id}",
        inputs=[f"kafka.{topic}" for topic in input_topics],
        outputs=[f"kafka.{topic}" for topic in output_topics]
    )
```

### With Quality Service
```python
# Quality service updates data quality metrics
async def update_quality_metrics(dataset_guid, quality_report):
    await catalog_client.update_entity(
        guid=dataset_guid,
        attributes={
            "dataQuality": {
                "score": quality_report.overall_score,
                "completeness": quality_report.completeness,
                "accuracy": quality_report.accuracy,
                "lastChecked": datetime.utcnow()
            }
        }
    )
```

## Best Practices

1. **Naming Conventions**: Use qualified names (e.g., `system.database.table`)
2. **Metadata Completeness**: Always provide description, owner, and classifications
3. **Schema Evolution**: Version all schema changes
4. **Lineage Granularity**: Track both dataset and column-level lineage
5. **Regular Sync**: Schedule regular metadata synchronization
6. **Tag Strategy**: Define clear tagging taxonomy
7. **Access Control**: Implement least-privilege for metadata

## Monitoring & Metrics

The service exposes various metrics:
- Entity count by type
- Search query performance
- Lineage processing lag
- Classification accuracy
- API request rates
- Cache hit rates

## Migration from Legacy Services

For migrating from individual metadata services:

```bash
# Export from legacy service
python scripts/export_metadata.py --source legacy-catalog

# Import to Atlas-based catalog
python scripts/import_to_atlas.py --data exports/metadata.json
```

## Security Considerations

- All API endpoints require authentication
- Sensitive metadata is encrypted at rest
- Audit logging for all metadata changes
- Integration with platform's RBAC
- Data masking for sensitive attributes

## Performance Optimization

1. **Caching**: Ignite-based caching for frequent queries
2. **Bulk Operations**: Batch API for large-scale updates
3. **Async Processing**: Non-blocking lineage updates
4. **Index Optimization**: Custom Atlas indexes
5. **Connection Pooling**: Efficient Atlas client management

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 