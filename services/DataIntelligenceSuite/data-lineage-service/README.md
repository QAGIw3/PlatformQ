# Data Lineage Service

A specialized service for tracking data lineage, transformations, and impact analysis across the data platform.

## Overview

The Data Lineage Service provides:
- **End-to-End Lineage**: Track data from source to consumption
- **Impact Analysis**: Understand downstream effects of changes
- **Transformation Tracking**: Document all data transformations
- **Compliance Support**: Audit trails for regulatory requirements
- **Visual Lineage**: Interactive lineage visualization

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Data Lineage Service                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │   Lineage    │  │   Impact    │  │  Compliance  │  │
│  │   Tracker    │  │  Analyzer   │  │   Engine     │  │
│  └──────────────┘  └─────────────┘  └──────────────┘  │
│                                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │              Graph Storage (JanusGraph)             │ │
│  │  • Entities    • Transformations    • Dependencies │ │
│  └────────────────────────────────────────────────────┘ │
│                                                         │
│  Events: Pulsar | Visualization: D3.js | Search: Elastic │
└─────────────────────────────────────────────────────────┘
```

## Features

### Lineage Tracking
- Automatic lineage capture from ETL/ELT jobs
- Column-level lineage granularity
- Cross-system lineage tracking
- Version history for all entities
- Real-time lineage updates

### Impact Analysis
- Forward impact analysis (what depends on this?)
- Backward impact analysis (what does this depend on?)
- Change impact predictions
- Risk assessment for modifications
- Dependency graphs

### Transformation Documentation
- Capture transformation logic
- Track business rules applied
- Document data quality checks
- Version control for transformations
- Transformation performance metrics

### Compliance & Governance
- Complete audit trails
- Data retention tracking
- GDPR compliance support
- Access history tracking
- Sensitive data flow tracking

## API Endpoints

### Lineage Operations
- `POST /api/v1/lineage/track` - Track lineage event
- `GET /api/v1/lineage/{entity_id}` - Get entity lineage
- `GET /api/v1/lineage/{entity_id}/upstream` - Get upstream lineage
- `GET /api/v1/lineage/{entity_id}/downstream` - Get downstream lineage
- `GET /api/v1/lineage/{entity_id}/graph` - Get visual graph data

### Impact Analysis
- `POST /api/v1/impact/analyze` - Analyze change impact
- `GET /api/v1/impact/{entity_id}/forward` - Forward impact
- `GET /api/v1/impact/{entity_id}/backward` - Backward impact
- `POST /api/v1/impact/simulate` - Simulate change impact

### Search & Discovery
- `GET /api/v1/search/entities` - Search entities
- `GET /api/v1/search/transformations` - Search transformations
- `GET /api/v1/search/by-tag` - Search by tags
- `GET /api/v1/search/sensitive-data` - Find sensitive data flows

### Compliance
- `GET /api/v1/compliance/audit-trail/{entity_id}` - Get audit trail
- `GET /api/v1/compliance/gdpr/data-flow` - GDPR data flow report
- `GET /api/v1/compliance/retention` - Data retention report
- `POST /api/v1/compliance/export` - Export compliance data

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: data-lineage-service
SERVICE_PORT: 8000

# Graph Database
JANUSGRAPH_URL: ws://janusgraph:8182/gremlin
CASSANDRA_HOSTS: cassandra:9042
ELASTICSEARCH_HOSTS: elasticsearch:9200

# Event Streaming
PULSAR_URL: pulsar://pulsar:6650
LINEAGE_TOPIC: persistent://data/lineage/events

# Performance
BATCH_SIZE: 100
ASYNC_PROCESSING: true
CACHE_TTL: 300

# Compliance
AUDIT_RETENTION_DAYS: 2555  # 7 years
SENSITIVE_DATA_TAGS: ["PII", "PHI", "PCI"]
```

## Usage Examples

### Track Data Transformation
```python
response = requests.post('http://lineage-service:8000/api/v1/lineage/track', json={
    "event_type": "transformation",
    "source_entities": [
        {"id": "table.sales.raw", "columns": ["customer_id", "amount"]}
    ],
    "target_entities": [
        {"id": "table.sales.aggregated", "columns": ["customer_id", "total_amount"]}
    ],
    "transformation": {
        "type": "aggregation",
        "logic": "SUM(amount) GROUP BY customer_id",
        "job_id": "spark-job-123",
        "executed_at": "2024-01-15T10:30:00Z"
    }
})
```

### Analyze Impact of Schema Change
```python
response = requests.post('http://lineage-service:8000/api/v1/impact/analyze', json={
    "entity_id": "table.customers.profile",
    "change_type": "schema_change",
    "changes": [
        {"column": "email", "from": "varchar(100)", "to": "varchar(255)"},
        {"column": "phone", "action": "drop"}
    ]
})

# Returns:
{
    "impact_summary": {
        "affected_entities": 23,
        "affected_jobs": 15,
        "risk_level": "high",
        "breaking_changes": [
            "pipeline.customer_etl - references dropped column 'phone'"
        ]
    },
    "affected_entities": [...],
    "recommendations": [...]
}
```

### Get Visual Lineage Graph
```python
response = requests.get('http://lineage-service:8000/api/v1/lineage/table.sales.gold/graph', params={
    "depth": 3,
    "direction": "both",
    "include_columns": true
})

# Returns D3.js compatible graph data
{
    "nodes": [
        {"id": "table.sales.raw", "type": "source", "label": "Raw Sales"},
        {"id": "table.sales.silver", "type": "transformation", "label": "Cleaned Sales"},
        {"id": "table.sales.gold", "type": "target", "label": "Sales Summary"}
    ],
    "edges": [
        {"source": "table.sales.raw", "target": "table.sales.silver", "label": "clean"},
        {"source": "table.sales.silver", "target": "table.sales.gold", "label": "aggregate"}
    ]
}
```

### GDPR Compliance Query
```python
# Find all data flows containing PII
response = requests.get('http://lineage-service:8000/api/v1/compliance/gdpr/data-flow', params={
    "data_subject": "customer",
    "include_deleted": false
})

# Returns complete data flow for GDPR reporting
{
    "data_sources": [...],
    "processing_activities": [...],
    "data_destinations": [...],
    "retention_periods": {...},
    "third_party_sharing": [...]
}
```

## Integration Patterns

### With ETL/ELT Tools
```python
# Apache Airflow DAG integration
from lineage_sdk import LineageClient

def track_transformation(**context):
    lineage = LineageClient()
    lineage.track_transformation(
        source=context['task'].upstream_task_ids,
        target=context['task'].task_id,
        transformation_type="airflow_task"
    )
```

### With Data Quality Service
```python
# Track quality check results in lineage
quality_result = await quality_service.validate(dataset)
await lineage_service.track({
    "event_type": "quality_check",
    "entity_id": dataset.id,
    "quality_score": quality_result.score,
    "quality_rules": quality_result.rules_applied
})
```

## Visualization

The service provides a web UI for lineage visualization:
- Interactive graph exploration
- Time-travel to see historical lineage
- Impact analysis visualization
- Search and filter capabilities

Access the UI at: `http://lineage-service:8000/ui`

## Performance Considerations

1. **Async Processing**: Lineage events are processed asynchronously
2. **Batch Updates**: Events are batched for efficiency
3. **Graph Indexing**: Optimized indexes for common queries
4. **Caching**: Frequently accessed lineage paths are cached
5. **Pruning**: Old lineage data can be archived

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 