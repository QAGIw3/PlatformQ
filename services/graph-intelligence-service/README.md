# Graph Intelligence Service

JanusGraph-based knowledge graph and intelligence platform for the platformQ ecosystem with advanced trading risk analysis.

## Overview

The Graph Intelligence Service provides comprehensive graph analytics and intelligence capabilities:

- **Knowledge Graph Management**: Store and query complex relationships between entities
- **Trading Risk Networks**: Analyze risk propagation and systemic importance in trading systems
- **Trust Networks**: Calculate and track trust scores between entities
- **Community Detection**: Identify clusters and communities within the graph
- **Fraud Detection**: Graph-based anomaly and fraud detection
- **Lineage Tracking**: Track data and asset lineage
- **Compute Market Intelligence**: Analyze compute market dynamics and relationships

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Graph Intelligence Service                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │  JanusGraph │   Trading    │   Trust     │  Compute   │ │
│  │   Backend   │  Risk Network│   Network   │  Markets   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │  Analytics  │   Lineage    │  Community  │   Fraud    │ │
│  │   Engine    │   Tracker    │  Detection  │  Detection │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Trading Risk Network Analysis
- **Risk Propagation**: Analyze how risk spreads through trading networks
- **Systemic Risk Detection**: Identify systemically important traders
- **Cascade Simulation**: Simulate cascade effects of trader failures
- **Risk Clustering**: Detect clusters of high-risk traders
- **Real-time Updates**: Track risk metrics as trades occur

### Graph Analytics
- PageRank and centrality analysis
- Community detection algorithms
- Shortest path calculations
- Graph structure analysis
- Custom graph algorithms via Gremlin

### Trust Networks
- Trust score calculation and propagation
- Trust-based access control
- Reputation tracking
- Network influence analysis

### Compute Market Intelligence
- Market participant analysis
- Trust-adjusted margin calculations
- Risk mitigation recommendations
- Market impact predictions

## API Endpoints

### Trading Risk Analysis
- `POST /api/v1/graph/trading-risk/traders/{trader_id}/risk` - Update trader risk profile
- `POST /api/v1/graph/trading-risk/relationships` - Add trading relationship
- `POST /api/v1/graph/trading-risk/analyze/propagation` - Analyze risk propagation
- `GET /api/v1/graph/trading-risk/clusters/risk` - Detect risk clusters
- `GET /api/v1/graph/trading-risk/traders/{trader_id}/systemic-importance` - Get systemic importance
- `POST /api/v1/graph/trading-risk/simulate/cascade` - Simulate cascade failure
- `GET /api/v1/graph/trading-risk/network/stats` - Get network statistics

### Graph Operations
- `POST /api/v1/graph/nodes` - Create node
- `GET /api/v1/graph/nodes/{node_id}` - Get node
- `POST /api/v1/graph/edges` - Create edge
- `GET /api/v1/graph/nodes/{node_id}/neighbors` - Get neighbors

### Analytics
- `POST /api/v1/graph/analyze/pagerank` - Run PageRank analysis
- `POST /api/v1/graph/analyze/communities` - Detect communities
- `POST /api/v1/graph/analyze/shortest-paths` - Find shortest paths

### Trust & Reputation
- `GET /api/v1/graph/trust-score/{user_id}` - Get trust score
- `POST /api/v1/graph/trust-score/{user_id}/calculate-verifiable` - Calculate verifiable trust

## Trading Risk Network

### Risk Propagation Types
```python
DIRECT_EXPOSURE = "direct_exposure"      # Direct trading relationship
COPY_TRADING = "copy_trading"            # Copy trading relationship
CORRELATED_POSITIONS = "correlated_positions"  # Similar positions
LIQUIDITY_LINKAGE = "liquidity_linkage"  # Shared liquidity pools
MARGIN_CASCADE = "margin_cascade"        # Margin call cascades
```

### Risk Metrics
- **Risk Score**: Overall risk level (0-1)
- **Exposure**: Total monetary exposure
- **Leverage**: Current leverage ratio
- **Margin Utilization**: Percentage of margin used
- **Liquidity**: Available liquidity
- **Systemic Importance**: Network centrality score

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

# Risk Network Configuration
RISK_PROPAGATION_DAMPING=0.85
MIN_PROPAGATION_STRENGTH=0.1
SYSTEMIC_RISK_THRESHOLD=0.7
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

The Graph Intelligence Service integrates with:
- **Trading Platform Service**: Receives trader and position updates
- **Event Router Service**: Processes risk events
- **Derivatives Engine Service**: Analyzes derivative risk networks
- **Data Platform Service**: Provides graph data for analytics 

## ML Model Lineage

The service provides comprehensive ML model lineage tracking and analysis:

### ML Lineage Features
- **Model Genealogy**: Track model versions and derivations
- **Dataset Dependencies**: Track which datasets were used for training
- **Feature Lineage**: Track feature engineering transformations
- **Experiment Relationships**: Link models to experiments
- **Impact Analysis**: Assess impact of changes to models or data
- **Similarity Search**: Find similar models based on lineage patterns
- **Evolution Tracking**: Track model performance over versions

### Artifact Types
- **Models**: ML models with versions and metadata
- **Datasets**: Training, validation, and test datasets
- **Feature Sets**: Engineered features and transformations
- **Experiments**: ML experiments and trials
- **Code Versions**: Track code used for training
- **Pipelines**: ML pipeline definitions
- **Deployments**: Model deployment configurations

### Relationship Types
- `derived_from`: Model derived from another model
- `trained_on`: Model trained on specific dataset
- `uses_features`: Model uses specific feature set
- `part_of_experiment`: Model part of experiment
- `replaced_by`: Model replaced by newer version
- `ensemble_member`: Model part of ensemble
- `fine_tuned_from`: Model fine-tuned from base model
- `distilled_from`: Model distilled from teacher model

### ML Lineage API
```bash
# Add model to lineage
POST /api/v1/ml-lineage/models

# Add dataset to lineage
POST /api/v1/ml-lineage/datasets

# Create lineage relationship
POST /api/v1/ml-lineage/relationships

# Get model lineage
GET /api/v1/ml-lineage/models/{model_id}/lineage

# Analyze change impact
POST /api/v1/ml-lineage/impact-analysis

# Find similar models
POST /api/v1/ml-lineage/similarity-search

# Track model evolution
GET /api/v1/ml-lineage/models/{model_name}/evolution

# Visualize lineage graph
GET /api/v1/ml-lineage/models/{model_id}/visualization

# Batch operations
POST /api/v1/ml-lineage/batch/models
POST /api/v1/ml-lineage/batch/relationships

# Get lineage statistics
GET /api/v1/ml-lineage/stats
```

### Impact Analysis
When models or datasets change, the service can analyze:
- Affected downstream models
- Impacted deployments
- Risk assessment and scoring
- Recommended actions

### Visualization
Generate Cytoscape-compatible visualization data for:
- Model dependency graphs
- Feature lineage trees
- Experiment relationships
- Evolution timelines 

## Digital Asset Lineage

The service provides comprehensive digital asset lineage tracking and analysis:

### Asset Lineage Features
- **Asset Provenance**: Track asset creation and derivation chains
- **Review Tracking**: Peer reviews and quality scores
- **Transaction History**: Marketplace purchases and licenses
- **User Reputation**: Calculate reputation based on assets and activity
- **Duplicate Detection**: Find duplicate assets by content ID
- **Impact Analysis**: Assess impact of asset changes
- **Relationship Discovery**: Find related assets by tags and type

### Node Types
- **Assets**: Digital assets with metadata and scores
- **Users**: Asset creators, reviewers, and purchasers
- **Reviews**: Peer reviews with ratings and comments
- **Transactions**: Purchase and license transactions
- **Licenses**: License agreements and terms
- **Collections**: Asset collections and bundles

### Relationship Types
- `derived_from`: Asset derived from another asset
- `fork_of`: Asset forked from original
- `version_of`: New version of existing asset
- `component_of`: Asset is component of larger asset
- `references`: Asset references another
- `reviewed_by`: Asset reviewed by user
- `purchased_by`: Asset purchased by user
- `licensed_to`: Asset licensed to user
- `created_by`: Asset created by user

### Asset Lineage API
```bash
# Add asset to lineage
POST /api/v1/asset-lineage/assets

# Add derivation relationship
POST /api/v1/asset-lineage/derivations

# Add review
POST /api/v1/asset-lineage/reviews

# Add transaction
POST /api/v1/asset-lineage/transactions

# Get asset lineage
GET /api/v1/asset-lineage/assets/{asset_id}/lineage

# Analyze impact
POST /api/v1/asset-lineage/impact-analysis

# Find duplicates
GET /api/v1/asset-lineage/assets/duplicates/{cid}

# Get user reputation
GET /api/v1/asset-lineage/users/{user_id}/reputation

# Batch operations
POST /api/v1/asset-lineage/batch/assets
POST /api/v1/asset-lineage/batch/derivations

# Get statistics
GET /api/v1/asset-lineage/stats
```

### Asset Quality & Trust Scores
- **Quality Score**: Based on peer reviews and ratings
- **Trust Score**: Based on transaction history
- **Reputation Score**: User reputation calculation

### Duplicate Detection
Find potential duplicate assets by:
- Content ID (CID) matching
- Similarity analysis
- Provenance tracking 