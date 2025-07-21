# Trading Risk Intelligence Service

Specialized graph-based service for trading risk network analysis and systemic risk detection in the platformQ ecosystem.

## Overview

The Trading Risk Intelligence Service provides advanced risk analytics for trading systems by modeling trader relationships and risk propagation through graph networks. It helps identify systemic risks, predict cascade failures, and provide early warning signals for market instability.

## Key Features

### Risk Network Analysis
- **Trader Risk Profiling**: Track individual trader risk metrics including exposure, leverage, and margin utilization
- **Relationship Mapping**: Model various types of trading relationships (direct exposure, copy trading, correlated positions, liquidity linkages)
- **Real-time Updates**: Process risk updates as they occur to maintain current network state

### Risk Propagation Modeling
- **Contagion Analysis**: Model how risk spreads through the trading network
- **Cascade Simulation**: Simulate trader failures to understand potential market impact
- **Multi-hop Analysis**: Track risk propagation through multiple degrees of separation

### Systemic Risk Detection
- **Risk Clustering**: Identify groups of highly interconnected risky traders
- **Systemic Importance**: Calculate each trader's importance to overall market stability
- **Early Warning System**: Detect emerging systemic risks before they materialize

### Mitigation Recommendations
- **Automated Suggestions**: Generate risk mitigation strategies based on network analysis
- **Circuit Breaker Triggers**: Identify when market-wide interventions may be needed
- **Position Limit Recommendations**: Suggest appropriate limits based on systemic risk

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│           Trading Risk Intelligence Service                  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Risk      │   Network    │  Cascade    │   Alert    │ │
│  │  Analysis   │   Modeling   │ Simulation  │   Engine   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    JanusGraph Backend                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Trader Nodes  │  Risk Edges  │  Time-based Index   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## API Endpoints

### Trader Risk Management

#### Update Trader Risk Profile
```http
POST /api/v1/trading-risk/traders/{trader_id}/risk
```
Updates risk metrics for a specific trader.

Request body:
```json
{
  "trader_id": "trader123",
  "risk_score": 0.75,
  "exposure": 1500000,
  "leverage": 3.5,
  "margin_utilization": 0.8,
  "position_count": 15,
  "liquidity": 500000
}
```

#### Add Trading Relationship
```http
POST /api/v1/trading-risk/relationships
```
Creates or updates a relationship between traders.

Request body:
```json
{
  "from_trader": "trader123",
  "to_trader": "trader456",
  "relationship_type": "copy_trading",
  "strength": 0.85,
  "exposure_amount": 250000
}
```

### Risk Analysis

#### Analyze Risk Propagation
```http
POST /api/v1/trading-risk/analyze/propagation
```
Analyzes how a risk event would propagate through the network.

Request body:
```json
{
  "source_trader": "trader123",
  "risk_event": {
    "type": "margin_call",
    "severity": "high",
    "amount": 1000000
  }
}
```

Response:
```json
{
  "affected_traders": ["trader456", "trader789"],
  "total_exposure": 3500000,
  "cascade_depth": 3,
  "systemic_risk_score": 0.82,
  "mitigation_actions": [
    {
      "action": "increase_margin_requirements",
      "target_traders": ["trader456"],
      "urgency": "high"
    }
  ]
}
```

#### Detect Risk Clusters
```http
GET /api/v1/trading-risk/clusters/risk
```
Identifies clusters of interconnected high-risk traders.

#### Get Systemic Importance
```http
GET /api/v1/trading-risk/traders/{trader_id}/systemic-importance
```
Calculates a trader's importance to overall market stability.

#### Simulate Cascade Failure
```http
POST /api/v1/trading-risk/simulate/cascade
```
Simulates the market impact of a specific trader failure.

## Risk Propagation Types

- **DIRECT_EXPOSURE**: Direct trading relationship or counterparty risk
- **COPY_TRADING**: One trader copies another's positions
- **CORRELATED_POSITIONS**: Traders with similar market positions
- **LIQUIDITY_LINKAGE**: Shared liquidity pools or funding sources
- **MARGIN_CASCADE**: Margin calls triggering further margin calls

## Integration Points

### Trading Platform Service
- Receives real-time trader and position updates
- Provides risk signals for trading decisions

### Event Router Service
- Consumes risk-related events
- Publishes risk alerts and warnings

### Graph Intelligence Service
- Leverages core graph algorithms
- Shares graph infrastructure

### Derivatives Engine Service
- Analyzes derivative position risks
- Provides margin requirement recommendations

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=trading-risk-intelligence-service
SERVICE_PORT=8000
LOG_LEVEL=INFO

# JanusGraph Configuration
GREMLIN_SERVER_URL=ws://janusgraph:8182/gremlin
CASSANDRA_HOSTS=cassandra-0,cassandra-1,cassandra-2
CASSANDRA_KEYSPACE=platformq

# Risk Analysis Parameters
RISK_PROPAGATION_DAMPING=0.85
MIN_PROPAGATION_STRENGTH=0.1
SYSTEMIC_RISK_THRESHOLD=0.7
CASCADE_SIMULATION_DEPTH=5

# Vault/Consul
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<your-token>
CONSUL_HOST=consul
CONSUL_PORT=8500
```

## Risk Metrics

### Trader Risk Score (0-1)
Calculated based on:
- Leverage ratio (40% weight)
- Margin utilization (30% weight) 
- Position concentration (20% weight)
- Historical volatility (10% weight)

### Systemic Importance Score
Based on network centrality measures:
- Degree centrality: Number of direct connections
- Betweenness centrality: Trader's role as a bridge
- Eigenvector centrality: Quality of connections
- Total exposure through the trader

## Monitoring & Alerts

### Metrics
- Network size (traders and relationships)
- Average risk score across network
- Number of high-risk clusters
- Risk propagation analysis time
- Query performance

### Alerts
- Systemic risk threshold breached
- Large risk cluster detected
- Cascade simulation shows high impact
- Rapid increase in network risk

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GREMLIN_SERVER_URL=ws://localhost:8182/gremlin
export VAULT_TOKEN=dev-token

# Run the service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/

# Run with coverage
pytest --cov=app tests/
```

## Performance Considerations

- Graph queries are optimized for read-heavy workloads
- Risk propagation analysis uses breadth-first search with early termination
- Caching is implemented for frequently accessed trader profiles
- Batch updates are supported for bulk risk updates

## Security

- All endpoints require authentication via platform auth service
- Sensitive trader data is encrypted at rest
- Audit logging for all risk-related operations
- Role-based access control for different risk views 