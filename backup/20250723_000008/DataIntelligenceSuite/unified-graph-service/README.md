# Unified Graph Service

A comprehensive graph platform consolidating graph intelligence, processing, analytics, and temporal knowledge capabilities with JanusGraph, GraphX, and advanced ML algorithms.

## Overview

The Unified Graph Service combines:
- **Graph Intelligence**: Knowledge graphs, trust networks, and market intelligence
- **Graph Processing**: Real-time updates, large-scale analytics with GraphX
- **Temporal Knowledge**: Time-aware reasoning and causal inference
- **Federated Graphs**: Distributed knowledge graph integration
- **ML-Enhanced Analytics**: Advanced algorithms for predictions and insights

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Unified Graph Service                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌─────────────────┐  ┌───────────────┐ │
│  │  JanusGraph  │  │     GraphX      │  │   Temporal    │ │
│  │   Backend    │  │   Analytics     │  │   Knowledge   │ │
│  └──────────────┘  └─────────────────┘  └───────────────┘ │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                  Core Capabilities                       ││
│  │  • Trust Networks        • Market Intelligence           ││
│  │  • Lineage Tracking      • Community Detection           ││
│  │  • Causal Inference      • Federated Graphs             ││
│  └─────────────────────────────────────────────────────────┘│
│                                                             │
│  Storage: Cassandra | Index: Elasticsearch | Cache: Ignite │
└─────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. Knowledge Graph Management
- **Entity Management**: Create, update, query complex entity relationships
- **Schema Evolution**: Dynamic schema management with versioning
- **Multi-tenancy**: Isolated graph spaces per tenant
- **Property Graphs**: Rich property support for vertices and edges
- **Graph Traversals**: Complex Gremlin queries and custom algorithms

### 2. Advanced Analytics
- **PageRank & Centrality**: Identify influential nodes
- **Community Detection**: Louvain, Label Propagation, Modularity
- **Shortest Paths**: Multiple algorithms with constraints
- **Trust Propagation**: Multi-dimensional trust scoring
- **Pattern Mining**: Frequent subgraph mining

### 3. Temporal Knowledge Graphs
- **Time-Aware Queries**: Query graph state at any point in time
- **Causal Discovery**: PC Algorithm, GES, LiNGAM
- **What-If Analysis**: Simulate scenarios and predict impacts
- **Temporal Patterns**: Detect time-based patterns
- **Evolution Tracking**: Track entity and relationship changes

### 4. Trust & Reputation
- **Trust Scoring**: Calculate multi-factor trust scores
- **Trust Networks**: Analyze trust propagation
- **Reputation Tracking**: Monitor reputation over time
- **Verifiable Trust**: Integration with verifiable credentials
- **Trust-Based Recommendations**: Suggest connections

### 5. Market Intelligence
- **Market Structure Analysis**: Understand market dynamics
- **Trust-Adjusted Pricing**: Calculate fair market prices
- **Risk Assessment**: Graph-based risk analysis
- **Impact Prediction**: Predict market changes
- **Participant Analysis**: Profile market actors

### 6. Lineage & Provenance
- **Data Lineage**: Track data transformations
- **ML Model Lineage**: Track model evolution
- **Asset Provenance**: Complete asset history
- **Impact Analysis**: Understand downstream effects
- **Compliance Tracking**: Audit trails

## API Endpoints

### Graph Operations
- `POST /api/v1/graph/nodes` - Create node
- `GET /api/v1/graph/nodes/{id}` - Get node details
- `PUT /api/v1/graph/nodes/{id}` - Update node
- `DELETE /api/v1/graph/nodes/{id}` - Delete node
- `POST /api/v1/graph/edges` - Create edge
- `POST /api/v1/graph/query` - Execute Gremlin query
- `POST /api/v1/graph/batch` - Batch operations

### Analytics
- `POST /api/v1/analytics/pagerank` - Run PageRank
- `POST /api/v1/analytics/communities` - Detect communities
- `POST /api/v1/analytics/centrality` - Calculate centrality
- `POST /api/v1/analytics/shortest-path` - Find paths
- `GET /api/v1/analytics/jobs/{id}` - Get job status

### Temporal Queries
- `GET /api/v1/temporal/snapshot` - Get graph snapshot
- `GET /api/v1/temporal/evolution/{entity_id}` - Entity evolution
- `POST /api/v1/temporal/causal/discover` - Discover causality
- `POST /api/v1/temporal/scenarios/simulate` - Run scenarios

### Trust & Reputation
- `GET /api/v1/trust/{entity_id}` - Get trust score
- `POST /api/v1/trust/calculate` - Calculate trust
- `GET /api/v1/trust/network/{entity_id}` - Trust network
- `POST /api/v1/trust/propagate` - Propagate trust

### Market Intelligence
- `GET /api/v1/market/insights` - Market insights
- `POST /api/v1/market/impact-analysis` - Impact analysis
- `GET /api/v1/market/participants/{type}` - List participants
- `POST /api/v1/market/risk-assessment` - Assess risks

### Lineage
- `POST /api/v1/lineage/track` - Track lineage
- `GET /api/v1/lineage/{entity_id}` - Get lineage
- `POST /api/v1/lineage/impact-analysis` - Analyze impact

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: unified-graph-service
SERVICE_PORT: 8000
ENVIRONMENT: production

# JanusGraph Configuration
JANUSGRAPH_URL: ws://janusgraph:8182/gremlin
CASSANDRA_HOSTS: cassandra-0,cassandra-1,cassandra-2
CASSANDRA_PORT: 9042
ELASTICSEARCH_HOSTS: elasticsearch:9200

# GraphX Configuration
SPARK_MASTER: spark://spark-master:7077
SPARK_EXECUTOR_MEMORY: 4g
SPARK_EXECUTOR_CORES: 4

# Cache Configuration
IGNITE_HOST: ignite
IGNITE_PORT: 10800
CACHE_TTL: 3600

# Analytics Configuration
PAGERANK_ITERATIONS: 20
PAGERANK_DAMPING_FACTOR: 0.85
COMMUNITY_DETECTION_RESOLUTION: 1.0

# Trust Configuration
TRUST_ALGORITHM: eigentrust
TRUST_PROPAGATION_DEPTH: 3
TRUST_UPDATE_INTERVAL: 300

# Temporal Configuration
TEMPORAL_INDEX_ENABLED: true
CAUSAL_DISCOVERY_THRESHOLD: 0.05
SCENARIO_SIMULATION_THREADS: 4
```

## Usage Examples

### Create Knowledge Graph Entity
```python
# Create a user entity with properties
response = requests.post('http://graph-service:8000/api/v1/graph/nodes', json={
    "label": "user",
    "properties": {
        "id": "user-123",
        "name": "Alice",
        "skills": ["Python", "ML", "GraphDB"],
        "reputation": 0.85,
        "created_at": "2024-01-15T10:00:00Z"
    }
})

# Create relationship
response = requests.post('http://graph-service:8000/api/v1/graph/edges', json={
    "label": "TRUSTS",
    "from_id": "user-123",
    "to_id": "user-456",
    "properties": {
        "trust_level": 0.9,
        "context": "technical_expertise"
    }
})
```

### Run Temporal Analysis
```python
# Discover causal relationships
response = requests.post('http://graph-service:8000/api/v1/temporal/causal/discover', json={
    "entities": ["market-price", "trading-volume", "user-activity"],
    "time_window": "7d",
    "algorithm": "pc",
    "significance_level": 0.05
})

# Simulate what-if scenario
response = requests.post('http://graph-service:8000/api/v1/temporal/scenarios/simulate', json={
    "scenario": {
        "description": "Major provider goes offline",
        "changes": [
            {"entity": "provider-xyz", "property": "status", "value": "offline"}
        ]
    },
    "predict_horizon": "1h",
    "metrics": ["market_price", "availability", "user_satisfaction"]
})
```

### Market Intelligence Query
```python
# Get market insights with trust adjustment
response = requests.get('http://graph-service:8000/api/v1/market/insights', params={
    "market_segment": "compute",
    "include_trust": true,
    "time_range": "24h"
})

# Returns comprehensive market analysis including:
# - Market structure and concentration
# - Trust-adjusted pricing recommendations
# - Risk indicators
# - Key player influence metrics
```

### Complex Graph Query
```python
# Find trusted paths between entities
response = requests.post('http://graph-service:8000/api/v1/graph/query', json={
    "query": """
        g.V().has('id', 'user-123')
         .repeat(out('TRUSTS').simplePath())
         .until(has('id', 'user-789').or().loops().is(4))
         .path()
         .by(valueMap('id', 'name', 'trust_score'))
    """,
    "bindings": {}
})
```

## Performance Optimization

### 1. **Intelligent Caching**
- Ignite for hot data
- Query result caching
- Trust score caching with TTL
- Invalidation strategies

### 2. **Batch Processing**
- GraphX for large analytics
- Bulk import/export
- Parallel processing
- Incremental computations

### 3. **Index Optimization**
- Composite indexes for common queries
- Elasticsearch for full-text search
- Vertex-centric indexes
- Time-based partitioning

### 4. **Query Optimization**
- Query plan analysis
- Gremlin query optimization
- Pagination strategies
- Lazy evaluation

## Monitoring & Observability

### Metrics
```
# Graph metrics
graph_vertices_total{label="user"} 50000
graph_edges_total{label="TRUSTS"} 150000
graph_query_duration_seconds{query_type="traversal"} 0.05

# Analytics metrics
graph_analytics_jobs_total{algorithm="pagerank"} 42
graph_analytics_duration_seconds{algorithm="community"} 120.5

# Trust metrics
graph_trust_calculations_total 10000
graph_trust_score_distribution{bucket="0.8-0.9"} 0.45

# Temporal metrics
graph_temporal_queries_total 5000
graph_causal_discoveries_total 25
```

### Health Checks
- `/health` - Basic health
- `/health/janusgraph` - JanusGraph connectivity
- `/health/spark` - Spark cluster status
- `/ready` - Full readiness check

## Integration Examples

### With ML Platform
```python
# Track ML model lineage
await graph_service.track_lineage({
    "model_id": "model-123",
    "training_data": ["dataset-1", "dataset-2"],
    "features": ["feature-a", "feature-b"],
    "derived_from": "model-122"
})
```

### With Trading Platform
```python
# Calculate trust-adjusted risk
risk_score = await graph_service.calculate_market_risk(
    participant_id="trader-123",
    market_conditions=current_conditions,
    include_trust_network=True
)
```

## Best Practices

1. **Use Batch Operations**: For bulk updates, use batch APIs
2. **Leverage Caching**: Enable caching for frequently accessed data
3. **Optimize Queries**: Use indexes and limit traversal depth
4. **Monitor Performance**: Track query times and optimize slow queries
5. **Version Your Schema**: Use schema versioning for evolution

## Migration Guide

### From graph-intelligence-service
```bash
# Export data
python scripts/migrate_graph.py export --source graph-intelligence

# Import to unified service
python scripts/migrate_graph.py import --target unified-graph
```

### From graph-processing-service
```bash
# Migrate with mapping
python scripts/migrate_graph.py migrate --source graph-processing
```

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 