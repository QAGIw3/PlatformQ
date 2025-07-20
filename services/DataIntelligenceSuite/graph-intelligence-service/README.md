# Graph Intelligence Service

JanusGraph-based core graph infrastructure and analytics platform for the platformQ ecosystem.

## Overview

The Graph Intelligence Service provides foundational graph database capabilities and analytics algorithms that power various specialized graph-based services across the platform:

- **Knowledge Graph Management**: Store and query complex relationships between entities
- **Graph Analytics**: PageRank, community detection, shortest paths, and custom algorithms
- **Trust Networks**: Calculate and track trust scores between entities
- **Fraud Detection**: Graph-based anomaly and fraud detection patterns
- **General Lineage Tracking**: Track data transformations and relationships
- **Compute Market Intelligence**: Analyze compute market dynamics and relationships

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Graph Intelligence Service                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │  JanusGraph │   Graph      │   Trust     │  Compute   │ │
│  │   Backend   │  Analytics   │   Network   │  Markets   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │  PageRank   │   Lineage    │  Community  │   Fraud    │ │
│  │ & Centrality│   Tracker    │  Detection  │  Detection │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Core Graph Analytics
- **PageRank & Centrality**: Identify influential nodes and measure importance
- **Community Detection**: Find clusters and communities within networks
- **Shortest Paths**: Calculate optimal paths between nodes
- **Graph Structure Analysis**: Analyze network topology and properties
- **Custom Algorithms**: Execute custom Gremlin queries and algorithms

### Trust Networks
- **Trust Score Calculation**: Compute and propagate trust scores
- **Reputation Tracking**: Monitor entity reputation over time
- **Network Influence**: Analyze influence propagation through the network
- **Verifiable Trust**: Integration with verifiable credentials

### Fraud Detection
- **Anomaly Detection**: Identify unusual patterns in graph structures
- **Pattern Matching**: Detect known fraud patterns
- **Risk Scoring**: Calculate risk scores based on network behavior
- **Real-time Alerts**: Generate alerts for suspicious activities

### Compute Market Intelligence
- **Market Analysis**: Analyze compute market participant relationships
- **Trust-based Pricing**: Calculate trust-adjusted pricing
- **Market Dynamics**: Track market evolution and trends
- **Impact Prediction**: Predict market impacts of changes

## API Endpoints

### Graph Operations
- `POST /api/v1/graph/nodes` - Create node
- `GET /api/v1/graph/nodes/{node_id}` - Get node
- `PUT /api/v1/graph/nodes/{node_id}` - Update node
- `DELETE /api/v1/graph/nodes/{node_id}` - Delete node
- `POST /api/v1/graph/edges` - Create edge
- `GET /api/v1/graph/edges/{edge_id}` - Get edge
- `DELETE /api/v1/graph/edges/{edge_id}` - Delete edge
- `GET /api/v1/graph/nodes/{node_id}/neighbors` - Get node neighbors

### Graph Analytics
- `POST /api/v1/graph/analyze/pagerank` - Run PageRank analysis
- `POST /api/v1/graph/analyze/communities` - Detect communities
- `POST /api/v1/graph/analyze/shortest-paths` - Find shortest paths
- `POST /api/v1/graph/analyze/centrality` - Calculate centrality metrics
- `GET /api/v1/graph/analyze/structure` - Get graph structure statistics

### Trust & Reputation
- `GET /api/v1/graph/trust-score/{user_id}` - Get trust score
- `POST /api/v1/graph/trust-score/{user_id}/calculate-verifiable` - Calculate verifiable trust
- `GET /api/v1/graph/trust/network/{entity_id}` - Get trust network
- `POST /api/v1/graph/trust/calculate` - Calculate trust between entities

### Fraud Detection
- `POST /api/v1/graph/fraud/check` - Check for fraud patterns
- `GET /api/v1/graph/fraud/results/{job_id}` - Get fraud check results

### Compute Market Intelligence
- `GET /api/v1/graph/market/insights` - Get market insights
- `POST /api/v1/graph/market/trust-adjusted-margin` - Calculate trust-adjusted margins
- `POST /api/v1/graph/market/impact-prediction` - Predict market impact



## Configuration

### Environment Variables

```bash
# JanusGraph Configuration
GREMLIN_SERVER_URL=ws://janusgraph:8182/gremlin
CASSANDRA_HOSTS=cassandra-0,cassandra-1,cassandra-2
CASSANDRA_PORT=9042

# Service Configuration
SERVICE_NAME=graph-intelligence-service
LOG_LEVEL=INFO

# Graph Analytics Configuration
PAGERANK_DAMPING_FACTOR=0.85
COMMUNITY_DETECTION_ITERATIONS=10
MAX_PATH_LENGTH=5

# Vault/Consul
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<your-token>
CONSUL_HOST=consul
CONSUL_PORT=8500
```

## Usage Examples

### Update Trader Risk Profile

```python
import httpx

risk_update = {
    "trader_id": "trader123",
    "risk_score": 0.65,
    "exposure": 1500000,
    "leverage": 3.5,
    "margin_utilization": 0.75,
    "position_count": 12,
    "liquidity": 500000
}

response = httpx.post(
    "http://graph-intelligence:8000/api/v1/graph/trading-risk/traders/trader123/risk",
    json=risk_update
)
```

### Analyze Risk Propagation

```python
propagation_request = {
    "source_trader": "trader123",
    "risk_event": {
        "type": "liquidation",
        "severity": "high",
        "market_volatility": 0.8,
        "amount": 1000000
    }
}

response = httpx.post(
    "http://graph-intelligence:8000/api/v1/graph/trading-risk/analyze/propagation",
    json=propagation_request
)

# Response includes:
# - affected_traders: List of traders affected
# - total_exposure: Total exposure at risk
# - cascade_depth: How many hops the risk propagated
# - systemic_risk_score: Overall systemic risk (0-1)
# - mitigation_actions: Recommended actions
```

### Simulate Cascade Failure

```python
simulation_request = {
    "failing_trader": "whale_trader_001",
    "failure_type": "liquidation"
}

response = httpx.post(
    "http://graph-intelligence:8000/api/v1/graph/trading-risk/simulate/cascade",
    json=simulation_request
)

# Response shows waves of failures and recommendations
```

## Schema

### Trader Node Properties
- `trader_id`: Unique identifier
- `risk_score`: Current risk score (0-1)
- `exposure`: Total exposure amount
- `leverage`: Current leverage
- `margin_utilization`: Margin usage (0-1)
- `position_count`: Number of open positions
- `liquidity`: Available liquidity
- `last_update`: Last update timestamp

### Trading Relationship Edge Properties
- `relationship_type`: Type of relationship
- `strength`: Relationship strength (0-1)
- `exposure_amount`: Monetary exposure
- `last_interaction`: Last interaction time

## Monitoring

### Metrics
- Graph size (nodes and edges)
- Query performance and latency
- Risk propagation analysis time
- Community detection performance
- Trust score calculation rate

### Alerts
- High systemic risk detected
- Large risk clusters identified
- Cascade simulation shows high impact
- Graph query timeouts

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start JanusGraph
docker-compose up -d janusgraph

# Run service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run tests
pytest tests/

# Run integration tests
pytest tests/integration/
```

## Integration

The Graph Intelligence Service provides core graph functionality for:
- **Trading Risk Intelligence Service**: Graph infrastructure for risk analysis
- **Trading Platform Service**: Trust and reputation scoring
- **Event Router Service**: Graph event processing
- **Data Platform Service**: Graph data storage and analytics
- **Search Service**: Graph-enhanced search capabilities
- **Unified ML Platform Service**: ML model lineage tracking
- **Digital Asset Service**: Asset lineage and provenance

## Extensibility

The Graph Intelligence Service is designed as a core infrastructure service that can be extended by other specialized graph services:

### Creating Specialized Graph Services
1. **Use the Graph Intelligence Service for core operations**: Node/edge CRUD, basic traversals
2. **Build domain-specific logic**: Add specialized algorithms for your domain
3. **Leverage shared infrastructure**: Reuse JanusGraph connection, schema management
4. **Integrate via events**: Subscribe to graph update events

### Example Integration Pattern
```python
# In your specialized service
from graph_intelligence_client import GraphIntelligenceClient

client = GraphIntelligenceClient(
    base_url="http://graph-intelligence-service:8000"
)

# Use core graph operations
node = await client.create_node(
    node_type="trader",
    properties={"trader_id": "123", "name": "John"}
)

# Run graph analytics
pagerank_results = await client.analyze_pagerank(
    vertex_label="trader",
    top_k=100
)
```

## Performance Optimization

### Query Optimization
- Use indexes for frequently queried properties
- Limit traversal depth with explicit bounds
- Use vertex-centric indices for supernodes
- Batch operations when possible

### Caching Strategy
- Cache frequently accessed nodes and edges
- Use Redis for query result caching
- Implement TTL-based cache invalidation
- Cache graph statistics and aggregations

### Scaling Considerations
- JanusGraph scales horizontally with Cassandra
- Use read replicas for analytics workloads
- Separate OLTP and OLAP queries
- Consider graph partitioning for very large graphs