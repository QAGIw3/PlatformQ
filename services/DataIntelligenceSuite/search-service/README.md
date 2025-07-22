# Search Service

## Purpose

The **Search Service** provides AI-powered, unified search capabilities across the entire platformQ ecosystem. It integrates with all platform services to deliver personalized, semantic search with real-time analytics and insights.

## Key Features

### Core Capabilities
- **Unified Search**: Single API searches across ALL platform services
- **Cross-Service Integration**: Real-time indexing from 10+ services
- **Full-Text Search**: Advanced text analysis with synonyms and fuzzy matching
- **Faceted Search**: Dynamic filters by type, date, tags, and custom fields
- **Multi-tenant Isolation**: Complete data separation and security

### AI-Powered Features 🤖
- **Semantic Search**: Understands query intent and context
- **Query Enhancement**: Automatic query expansion and optimization
- **Personalized Results**: User-specific ranking based on behavior
- **Auto-Categorization**: AI-driven content classification and tagging
- **Natural Language Understanding**: Processes questions and complex queries

### Analytics & Insights 📊
- **Search Analytics Dashboard**: Real-time metrics and trends
- **Performance Tracking**: CTR, response times, zero-result queries
- **AI-Generated Insights**: Automatic identification of search patterns
- **Query Performance Analysis**: Detailed metrics per search term
- **Optimization Recommendations**: Data-driven improvement suggestions

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Search Service 3.0                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────── Unified Search Layer ─────────────────┐ │
│  │  • Cross-Service Integration  • Real-time Sync            │ │
│  │  • Service Registry           • Health Monitoring          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────── AI Enhancement Layer ─────────────────┐ │
│  │  • Query Intent Classification  • Semantic Understanding   │ │
│  │  • Personalization Engine      • Auto-Categorization      │ │
│  │  • Result Re-ranking           • NLP Processing           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────── Analytics Layer ──────────────────────┐ │
│  │  • Search Tracking    • Performance Metrics               │ │
│  │  • Trend Analysis     • Insight Generation                │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────── Core Search Engine ───────────────────┐ │
│  │  Elasticsearch v8 | Vector Search | Graph Integration     │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Service Integration Points

The Search Service automatically indexes data from:

1. **Auth Service**: Users, roles, permissions
2. **Digital Asset Service**: 3D models, images, documents
3. **Verifiable Credential Service**: Credentials, schemas
4. **Governance Service**: Proposals, votes
5. **Compliance Service**: Policies, audits
6. **Data Ingestion Service**: Datasets, schemas
7. **Analytics Service**: Reports, dashboards
8. **Blockchain Services**: Transactions, contracts
9. **Marketplace Service**: Listings, orders
10. **All Other Platform Services**

## Search Types

The service can search across:

- **Digital Assets**: 3D models, images, documents, and other files
- **Documents**: Text documents with full content search
- **Activities**: User activities and system events
- **Projects**: Collaborative projects and their metadata
- **Workflows**: Workflow definitions and execution history
- **Verifiable Credentials**: Issued credentials and their claims
- **Graph Entities**: Nodes and relationships from JanusGraph
- **Users**: User profiles and information

## API Endpoints

### Unified Search 🔍
- `POST /api/v1/unified/search`: AI-powered search across all services
- `POST /api/v1/unified/search/services/{service}`: Search specific service
- `GET /api/v1/unified/suggestions`: Smart search suggestions
- `POST /api/v1/unified/click`: Track result clicks

### Analytics & Insights 📊
- `GET /api/v1/unified/analytics/metrics`: Search performance metrics
- `GET /api/v1/unified/analytics/insights`: AI-generated insights
- `GET /api/v1/unified/analytics/query/{query}`: Query-specific analytics

### Traditional Search
- `POST /api/v1/search`: Execute a search query
- `GET /api/v1/suggest`: Get autocomplete suggestions

### Index Management
- `POST /api/v1/unified/index/service/{service}`: Index service data
- `POST /api/v1/index/{index_type}/{doc_id}`: Index single document
- `DELETE /api/v1/index/{index_type}/{doc_id}`: Delete document

## Search Query Structure

### Basic Search

```json
{
  "query": "3D model airplane",
  "search_type": "digital_asset",
  "from": 0,
  "size": 20
}
```

### Advanced Search with Filters

```json
{
  "query": "user authentication",
  "search_type": "all",
  "filters": {
    "created_at": {
      "gte": "2024-01-01",
      "lte": "2024-12-31"
    },
    "tags": ["security", "auth"],
    "file_type": "pdf"
  },
  "aggregations": ["asset_type", "source_tool"],
  "highlight": true,
  "sort_by": "created_at",
  "sort_order": "desc"
}
```

### Phrase Search

```json
{
  "query": "\"exact phrase match\"",
  "search_type": "document"
}
```

## Search Features

### 1. Full-Text Search

- **Multi-field Search**: Searches across all relevant fields
- **Field Boosting**: Prioritizes matches in important fields (title, name)
- **Fuzzy Matching**: Handles typos and variations
- **Synonym Support**: Built-in synonyms for common terms

### 2. Filtering

- **Field Filters**: Filter by any indexed field
- **Range Queries**: Date ranges, numeric ranges
- **Multi-value Filters**: Filter by multiple values
- **Nested Filters**: Complex filter combinations

### 3. Aggregations

Get statistics about your search results:

```json
{
  "aggregations": {
    "asset_type_agg": {
      "buckets": [
        {"key": "3D_MODEL", "doc_count": 150},
        {"key": "IMAGE", "doc_count": 89},
        {"key": "DOCUMENT", "doc_count": 45}
      ]
    }
  }
}
```

### 4. Highlighting

Search results include highlighted snippets:

```json
{
  "highlights": {
    "content": [
      "This document explains the <mark>authentication</mark> process...",
      "Users must <mark>authenticate</mark> before accessing..."
    ]
  }
}
```

### 5. Autocomplete

Real-time suggestions based on indexed data:

```
GET /api/v1/suggest?prefix=auth&search_type=all

Response:
{
  "suggestions": [
    "authentication",
    "authorization",
    "auth-service",
    "authenticate user"
  ]
}
```

## Index Mappings

Each entity type has a specialized mapping optimized for its use case:

### Digital Assets Mapping

- Text fields for content search
- Keyword fields for exact matching
- Completion fields for autocomplete
- Date fields for temporal queries
- Nested objects for complex metadata

### Custom Analyzers

The service uses custom analyzers for:

- **Language Detection**: Automatic language-specific analysis
- **Synonym Expansion**: Domain-specific synonyms
- **ASCII Folding**: Handle accented characters
- **Edge N-grams**: For autocomplete functionality

## Event-Driven Indexing

The service automatically indexes data from these events:

- `digital-asset-created-events`
- `document-updated-events`
- `activity-events`
- `project-created-events`
- `workflow-completed-events`

## Performance Optimization

### Indexing Strategy

1. **Bulk Indexing**: Batch multiple documents for efficiency
2. **Async Operations**: Non-blocking index operations
3. **Index Aliases**: Zero-downtime reindexing
4. **Shard Configuration**: Optimized for tenant data distribution

### Query Optimization

1. **Query Caching**: Frequently used queries are cached
2. **Filter Caching**: Filters are cached for reuse
3. **Field Data**: Optimized for aggregations
4. **Source Filtering**: Return only needed fields

## Multi-tenancy

Complete tenant isolation through:

- **Index Naming**: `{tenant_id}_{entity_type}` pattern
- **Index Templates**: Automatic mapping application
- **Query Filtering**: Tenant filter automatically applied
- **Access Control**: API-level tenant validation

## Monitoring

### Metrics Tracked

- Query latency (p50, p95, p99)
- Index rate (documents/second)
- Search rate (queries/second)
- Index size per tenant
- Cache hit rates

### Health Checks

The `/health` endpoint reports:

- Elasticsearch cluster health
- Index status
- Event consumer status

## Best Practices

### For Indexing

1. **Batch Operations**: Use bulk indexing for multiple documents
2. **Async Indexing**: Don't block on index operations
3. **Schema Design**: Plan mappings before indexing
4. **Field Selection**: Only index searchable fields

### For Searching

1. **Use Filters**: Filters are faster than queries
2. **Limit Fields**: Search specific fields when possible
3. **Pagination**: Use `from` and `size` appropriately
4. **Caching**: Leverage query result caching

### For Performance

1. **Index Aliases**: Use aliases for zero-downtime updates
2. **Shard Sizing**: Keep shards between 10-50GB
3. **Replica Count**: Balance between performance and resilience
4. **Monitor Metrics**: Track query and indexing performance

## Integration Examples

### With Digital Asset Service

```python
# After creating a digital asset
asset_data = {
    "asset_id": "uuid",
    "asset_name": "Boeing 747 Model",
    "asset_type": "3D_MODEL",
    "tags": ["aircraft", "boeing", "commercial"],
    "metadata": {...}
}

# The asset is automatically indexed via events
```

### With Graph Intelligence Service

```python
# Index graph insights
graph_data = {
    "entity_id": "user-123",
    "entity_type": "user",
    "centrality_score": 0.85,
    "community_id": "engineering",
    "properties": {...}
}
```

## Troubleshooting

### Common Issues

1. **No Results**: Check index exists and data is indexed
2. **Slow Queries**: Review query complexity and use filters
3. **Indexing Failures**: Check mapping conflicts
4. **Memory Issues**: Adjust heap size and query size

### Debugging

1. **Enable Debug Logging**: Set `LOG_LEVEL=DEBUG`
2. **Elasticsearch Logs**: Check ES cluster logs
3. **Query Profiling**: Use ES profile API
4. **Index Stats**: Check index statistics

## Configuration

Environment variables:

- `ELASTICSEARCH_URL`: Elasticsearch cluster URL
- `ELASTICSEARCH_USERNAME`: Optional username
- `ELASTICSEARCH_PASSWORD`: Optional password
- `INDEX_REFRESH_INTERVAL`: How often to refresh indices
- `BULK_INDEX_SIZE`: Batch size for bulk operations
- `QUERY_TIMEOUT`: Default query timeout 