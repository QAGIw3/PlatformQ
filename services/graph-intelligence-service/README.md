# Graph Intelligence Service

The Graph Intelligence Service provides advanced graph analytics and multi-dimensional trust scoring for PlatformQ. It leverages JanusGraph for relationship modeling and implements sophisticated algorithms for reputation calculation and network analysis.

## 🎯 Overview

This service manages:
- Multi-dimensional reputation scoring across 5 key dimensions
- Real-time trust network analysis
- Relationship mapping between users, assets, and activities
- Community detection and influence measurement
- Fraud detection through anomaly analysis
- Decentralized trust propagation

## 🏗️ Architecture

### Technology Stack
- **Framework**: FastAPI (Python)
- **Graph Database**: JanusGraph with Cassandra backend
- **Search**: Elasticsearch for graph queries
- **Analytics**: NetworkX for graph algorithms
- **Messaging**: Apache Pulsar
- **Cache**: Apache Ignite for real-time metrics

### Key Components

1. **Multi-Dimensional Reputation System**
   - Technical Prowess: Code quality, asset complexity, tool mastery
   - Collaboration Rating: Teamwork, communication, peer reviews
   - Governance Influence: Proposal quality, voting participation
   - Creativity Index: Innovation, unique solutions, artistic merit
   - Reliability Score: Timeliness, consistency, promise keeping

2. **Trust Network Analysis**
   - PageRank-based trust propagation
   - Transitive trust calculations
   - Trust path discovery
   - Network resilience metrics

3. **Graph Operations**
   - Real-time edge creation for activities
   - Batch graph updates from Flink
   - Efficient traversals with Gremlin
   - Subgraph extraction for analysis

4. **Anomaly Detection**
   - Unusual activity patterns
   - Sybil attack detection
   - Reputation gaming prevention
   - Network manipulation alerts

## 📡 API Endpoints

### Reputation Scoring
- `GET /api/v1/reputation/{user_id}` - Get user's reputation scores
- `GET /api/v1/reputation/multi-dimensional` - Get detailed breakdown
- `POST /api/v1/reputation/calculate` - Trigger recalculation
- `GET /api/v1/reputation/history/{user_id}` - Historical scores

### Trust Network
- `GET /api/v1/trust/network/{user_id}` - User's trust network
- `GET /api/v1/trust/path` - Find trust path between users
- `POST /api/v1/trust/relationship` - Create trust relationship
- `GET /api/v1/trust/score/{from_id}/{to_id}` - Pairwise trust score

### Graph Analytics
- `GET /api/v1/analytics/centrality/{user_id}` - User influence metrics
- `GET /api/v1/analytics/communities` - Detect communities
- `GET /api/v1/analytics/trends` - Network growth trends
- `POST /api/v1/analytics/subgraph` - Extract subgraph

### Anomaly Detection
- `GET /api/v1/anomalies/recent` - Recent anomalies
- `POST /api/v1/anomalies/report` - Report suspicious activity
- `GET /api/v1/anomalies/user/{user_id}` - User anomaly score

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- JanusGraph 0.6+
- Elasticsearch 7.x
- Apache Cassandra 3.x
- Apache Pulsar

### Development Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start JanusGraph**
   ```bash
   docker-compose -f docker-compose.janusgraph.yml up -d
   ```

3. **Initialize graph schema**
   ```bash
   python scripts/init_graph_schema.py
   ```

4. **Set environment variables**
   ```bash
   export JANUSGRAPH_HOST="localhost"
   export JANUSGRAPH_PORT="8182"
   export ELASTICSEARCH_URL="http://localhost:9200"
   export PULSAR_URL="pulsar://localhost:6650"
   ```

5. **Start the service**
   ```bash
   uvicorn app.main:app --reload --port 8003
   ```

## 📊 Graph Schema

### Vertices (Nodes)
- **User**: Platform users with reputation scores
- **Asset**: Digital assets with quality metrics
- **Activity**: User actions and contributions
- **Project**: Collaborative projects
- **Review**: Peer reviews and ratings

### Edges (Relationships)
- **CREATED**: User -> Asset
- **REVIEWED**: User -> Asset
- **COLLABORATED**: User -> User
- **CONTRIBUTED**: User -> Project
- **TRUSTS**: User -> User (weighted)

### Properties
```groovy
// User vertex properties
user.property('reputation_technical', 85.5)
user.property('reputation_collaboration', 92.0)
user.property('reputation_governance', 78.5)
user.property('reputation_creativity', 88.0)
user.property('reputation_reliability', 95.0)
user.property('total_score', 87.8)

// Trust edge properties
trust.property('weight', 0.85)
trust.property('context', 'technical')
trust.property('established', '2024-01-15')
```

## 🧮 Reputation Calculation

### Score Formula
```python
# Weighted average based on activity type
technical_score = (
    code_quality * 0.3 +
    asset_complexity * 0.3 +
    tool_expertise * 0.2 +
    problem_solving * 0.2
)

# Trust-adjusted score
final_score = base_score * trust_multiplier + peer_influence
```

### Dimension Weights
| Activity Type | Technical | Collaboration | Governance | Creativity | Reliability |
|--------------|-----------|---------------|------------|------------|-------------|
| Code Review | 40% | 20% | 10% | 10% | 20% |
| Asset Creation | 30% | 10% | 5% | 40% | 15% |
| DAO Voting | 10% | 20% | 50% | 5% | 15% |

## 🔐 Security

- **Graph Injection Prevention**: Parameterized Gremlin queries
- **Rate Limiting**: Prevents reputation gaming
- **Anomaly Detection**: Real-time fraud prevention
- **Access Control**: Vertex-level permissions
- **Audit Trail**: All score changes logged

## 🧪 Testing

Run the test suite:
```bash
pytest tests/ -v --cov=app
```

Graph integration tests:
```bash
pytest tests/integration/test_graph.py -v
```

Load testing:
```bash
locust -f tests/load/locustfile.py
```

## 📈 Performance Optimization

- **Vertex Caching**: Frequently accessed users cached
- **Batch Processing**: Bulk updates via Flink
- **Index Optimization**: Composite indexes on key properties
- **Query Optimization**: Prepared traversals for common paths
- **Partitioning**: Graph partitioned by tenant

## 🔧 Configuration

Key settings in `app/core/config.py`:
- `REPUTATION_UPDATE_INTERVAL`: How often to recalculate (300s)
- `TRUST_DECAY_FACTOR`: Trust decay over time (0.95)
- `MIN_ACTIVITIES_FOR_SCORE`: Minimum activities required (5)
- `ANOMALY_THRESHOLD`: Anomaly detection sensitivity (2.5 std)
- `MAX_TRUST_HOPS`: Maximum hops for trust propagation (3)

## 📊 Monitoring

Metrics exposed at `/metrics`:
- Graph size (vertices and edges)
- Query performance histograms
- Reputation calculation times
- Anomaly detection rates
- Cache hit ratios

## 🤝 Event Integration

### Events Consumed
- `UserActivityRecorded`: Updates activity graph
- `AssetCreated`: Adds asset vertices
- `PeerReviewSubmitted`: Creates review edges
- `ProjectCollaborationStarted`: Links collaborators

### Events Published
- `ReputationUpdated`: When scores change significantly
- `AnomalyDetected`: When suspicious activity found
- `TrustNetworkChanged`: When relationships change

## 📝 License

This service is part of the PlatformQ project and is licensed under the MIT License. 