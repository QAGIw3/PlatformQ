# Data Catalog Service

An intelligent data discovery platform built on Apache Atlas, providing automated cataloging, AI-powered search, quality integration, and advanced analytics for comprehensive metadata management.

## Overview

The Enhanced Data Catalog Service serves as the intelligent nervous system for data governance, providing:
- **Automated Discovery**: Continuous scanning of medallion architecture layers
- **AI-Powered Search**: Natural language understanding with intent detection
- **Business Glossary**: Intelligent term mapping with AI suggestions
- **Quality Integration**: Real-time data trust scores and recommendations
- **Access Analytics**: Usage pattern analysis and optimization insights
- **Metadata Management**: Centralized repository for all data assets
- **Data Lineage**: End-to-end tracking of data transformations
- **Schema Registry**: Version-controlled schema management
- **Discovery & Search**: Semantic search across all data assets
- **Classification & Tagging**: Automated and manual data classification

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  Enhanced Data Catalog Service                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │ Atlas Client │  │ Schema Registry │  │ Lineage Tracker  │  │
│  │  & Manager   │  │     (Avro)      │  │   & Processor    │  │
│  └──────────────┘  └─────────────────┘  └──────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │             Intelligent Search & Discovery                 │  │
│  │  - AI-Powered Search  - Intent Detection  - Personalized  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │  Medallion   │  │Enhanced Business│  │Quality Integration│ │
│  │  Discovery   │  │    Glossary     │  │    Engine        │  │
│  └──────────────┘  └─────────────────┘  └──────────────────┘  │
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │   Access     │  │ Classifier &    │  │  Access Control  │  │
│  │  Analytics   │  │   Tagger        │  │    & Audit       │  │
│  └──────────────┘  └─────────────────┘  └──────────────────┘  │
│                                                                  │
│  Backend: Atlas | Cache: Ignite | Events: Pulsar | Search: ES  │
└─────────────────────────────────────────────────────────────────┘
```

## Key Enhancements

### 1. Automated Medallion Discovery
- **Continuous Scanning**: Automatically discovers datasets across Bronze, Silver, and Gold layers
- **Smart Tagging**: Auto-generates tags based on content, quality, and freshness
- **Format Detection**: Supports Parquet, Delta Lake, Iceberg, CSV, JSON, and more
- **Schema Extraction**: Automatically extracts and registers schemas
- **Quality Integration**: Fetches quality scores during discovery

### 2. AI-Powered Intelligent Search
- **Natural Language**: Understands queries like "high quality customer data from last week"
- **Intent Detection**: Identifies search intent (find dataset, explore schema, track lineage)
- **Personalization**: Learns from user behavior to improve relevance
- **Hybrid Search**: Combines keyword, semantic, and graph-based search
- **Smart Recommendations**: Suggests related datasets and actions

### 3. Enhanced Business Glossary
- **AI Term Suggestions**: Automatically suggests business terms for technical names
- **Auto-Mapping**: Creates mappings between business terms and technical assets
- **Usage Analytics**: Tracks how business terms are used across the platform
- **External Sync**: Synchronizes with external business systems
- **Validation**: Ensures term mappings remain accurate over time

### 4. Quality Score Integration
- **Trust Levels**: Verified, Trusted, Acceptable, Review, Untrusted
- **Auto-Rules**: Generates quality rules based on schema
- **Trend Analysis**: Tracks quality changes over time
- **Recommendations**: Provides actionable improvement steps
- **Dashboard**: Comprehensive quality overview across catalog

### 5. Access Pattern Analytics
- **User Profiling**: Identifies user patterns (exploratory, targeted, analytical)
- **Hot Asset Detection**: Finds frequently accessed data for caching
- **Predictive Analytics**: Forecasts future access patterns
- **Optimization Reports**: Provides insights for catalog optimization
- **Real-time Insights**: Shows current catalog activity

## API Endpoints

### Core Catalog APIs
- `POST /api/v1/entities` - Create new entity
- `GET /api/v1/entities/{guid}` - Get entity by GUID
- `PUT /api/v1/entities/{guid}` - Update entity
- `DELETE /api/v1/entities/{guid}` - Delete entity
- `POST /api/v1/entities/bulk` - Bulk create/update

### Discovery APIs
- `POST /api/v1/discovery/scan` - Trigger discovery scan
- `GET /api/v1/discovery/status` - Get discovery status
- `GET /api/v1/discovery/assets/{layer}` - Get discovered assets by layer
- `POST /api/v1/discovery/assets/{layer}/{dataset}/profile` - Profile dataset
- `GET /api/v1/discovery/recommendations` - Get discovery recommendations

### Intelligent Search APIs
- `POST /api/v1/search/intelligent/search` - AI-powered search
- `GET /api/v1/search/intelligent/suggestions` - Get search suggestions
- `POST /api/v1/search/intelligent/click` - Track search clicks
- `GET /api/v1/search/intelligent/intents` - Supported search intents
- `GET /api/v1/search/intelligent/trending` - Trending searches

### Enhanced Glossary APIs
- `POST /api/v1/glossary/enhanced/suggest-terms` - AI term suggestions
- `POST /api/v1/glossary/enhanced/auto-map/{dataset}` - Auto-map terms
- `GET /api/v1/glossary/enhanced/term-usage/{term}` - Analyze term usage
- `GET /api/v1/glossary/enhanced/recommend-terms` - Recommend new terms
- `POST /api/v1/glossary/enhanced/sync-external` - Sync with external systems

### Quality Integration APIs
- `POST /api/v1/quality/assess` - Assess dataset quality
- `POST /api/v1/quality/rules/{dataset}` - Create quality rules
- `GET /api/v1/quality/trends/{dataset}` - Get quality trends
- `GET /api/v1/quality/recommendations/{dataset}` - Get recommendations
- `GET /api/v1/quality/dashboard` - Quality dashboard

### Analytics APIs
- `POST /api/v1/analytics/track` - Track access event
- `GET /api/v1/analytics/user/{user}/patterns` - User patterns
- `GET /api/v1/analytics/asset/{asset}/metrics` - Asset metrics
- `GET /api/v1/analytics/hot-assets` - Identify hot assets
- `GET /api/v1/analytics/optimization-report` - Optimization report

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

# Medallion Discovery
MINIO_ENDPOINT: minio:9000
DISCOVERY_INTERVAL_MINUTES: 60
DISCOVERY_FULL_SCAN_HOURS: 24
ENABLE_DATA_PROFILING: true

# Enhanced Search
SEARCH_SERVICE_URL: http://search-service:8031
ENABLE_AI_SEARCH: true

# Quality Integration
QUALITY_SERVICE_URL: http://unified-quality-service:8015
AUTO_QUALITY_RULES: true

# Business Glossary
GLOSSARY_AI_ENABLED: true
GLOSSARY_AUTO_MAPPING: true

# Access Analytics
ANALYTICS_BACKEND_URL: http://analytics-service:8018
ACCESS_TRACKING_ENABLED: true
```

## Usage Examples

### Automated Discovery
```python
# Trigger discovery scan for all medallion layers
response = requests.post('http://catalog:8017/api/v1/discovery/scan', json={
    "force_full_scan": True
})

# Get discovered assets in Silver layer
response = requests.get('http://catalog:8017/api/v1/discovery/assets/silver')
```

### AI-Powered Search
```python
# Natural language search
response = requests.post('http://catalog:8017/api/v1/search/intelligent/search', json={
    "query": "high quality customer data updated this week",
    "user_context": {"user_id": "analyst123", "team": "data-team"}
})

# Get search suggestions
response = requests.get('http://catalog:8017/api/v1/search/intelligent/suggestions', params={
    "prefix": "cust",
    "context": "exploring_datasets"
})
```

### Business Term Mapping
```python
# Get AI suggestions for technical column name
response = requests.post('http://catalog:8017/api/v1/glossary/enhanced/suggest-terms', json={
    "technical_name": "cust_ltv_usd_amt",
    "context": {
        "schema": "analytics",
        "table": "customer_metrics"
    }
})

# Auto-map terms for entire dataset
response = requests.post(f'http://catalog:8017/api/v1/glossary/enhanced/auto-map/{dataset_guid}', params={
    "approval_required": False
})
```

### Quality Assessment
```python
# Assess dataset quality
response = requests.post('http://catalog:8017/api/v1/quality/assess', json={
    "dataset_id": dataset_guid,
    "force_refresh": True
})

# Get quality recommendations
response = requests.get(f'http://catalog:8017/api/v1/quality/recommendations/{dataset_guid}')
```

### Access Analytics
```python
# Track data access
response = requests.post('http://catalog:8017/api/v1/analytics/track', json={
    "user_id": "analyst123",
    "asset_id": dataset_guid,
    "access_type": "query",
    "duration_ms": 1250
})

# Get optimization report
response = requests.get('http://catalog:8017/api/v1/analytics/optimization-report')
```

## Best Practices

1. **Discovery Configuration**: 
   - Schedule full scans during off-peak hours
   - Enable profiling for critical datasets only
   - Use appropriate retention policies for each layer

2. **Search Optimization**:
   - Track click-through rates to improve relevance
   - Provide search feedback for model improvement
   - Use filters to narrow results

3. **Glossary Management**:
   - Review AI-suggested terms before approval
   - Maintain consistent naming conventions
   - Regularly validate term mappings

4. **Quality Integration**:
   - Set up automated quality assessments
   - Define quality thresholds by data criticality
   - Act on quality recommendations promptly

5. **Access Analytics**:
   - Enable tracking for all critical datasets
   - Review optimization reports monthly
   - Implement caching for hot assets

## Monitoring & Metrics

The service exposes enhanced metrics:
- Discovery coverage by layer
- Search relevance scores
- Term mapping accuracy
- Quality score distribution
- Access pattern insights
- Cache hit rates
- API performance metrics

## Performance Optimization

1. **Discovery**: Parallel scanning across layers
2. **Search**: Elasticsearch caching and query optimization
3. **Quality**: Async assessments with result caching
4. **Analytics**: Real-time event streaming with buffering
5. **Glossary**: Embedding cache for term suggestions

## Security Considerations

- All enhancements respect existing RBAC policies
- Sensitive data discovery with automated classification
- Audit logging for all metadata changes
- Encrypted communication with integrated services
- API rate limiting for resource protection

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 