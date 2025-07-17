# Real-time Graph Analytics Flink Job

This Flink job provides real-time graph analytics capabilities for the PlatformQ Graph Intelligence Platform, processing event streams to maintain up-to-date graph intelligence.

## 🎯 Overview

The job processes multiple event streams to perform:
- **Real-time Trust Score Updates**: Calculates and updates trust scores based on user activities
- **Fraud Detection**: Identifies suspicious patterns and potential fraud in real-time
- **Personalized Recommendations**: Generates graph-based recommendations for users
- **Graph Evolution Tracking**: Monitors how the graph structure changes over time

## 🏗️ Architecture

```
Event Streams → Flink Processing → Graph Updates + Analytics Results
     ↓                                      ↓              ↓
  Pulsar                               JanusGraph      Ignite Cache
```

## 🚀 Key Features

### 1. Trust Score Processing
- Processes trust-affecting events (reviews, verifications, collaborations)
- Updates trust scores with bounded growth using sigmoid functions
- Implements trust decay over time
- Propagates trust through graph paths

### 2. Fraud Detection
- Real-time pattern detection for suspicious behavior
- Identifies rapid activity bursts and automated behavior
- Analyzes graph context for new accounts
- Uses Complex Event Processing (CEP) for pattern matching

### 3. Recommendation Generation
- Collaborative filtering based on similar users
- Content-based recommendations from graph relationships
- Real-time personalization based on user context
- Caches results in Ignite for fast retrieval

### 4. Graph Evolution Tracking
- Monitors node and edge creation rates
- Calculates graph density and other metrics
- Tracks community evolution
- Provides insights for capacity planning

## 📡 Event Processing

### Input Events
The job consumes events from multiple Pulsar topics:
- `graph-update-events`: General graph updates
- `trust-events`: Trust-affecting activities
- `user-activity-events`: User interactions
- `asset-events`: Asset-related activities
- `collaboration-events`: Collaboration activities

### Event Schema
```json
{
  "event_type": "ASSET_CREATED",
  "entity_id": "entity123",
  "entity_type": "asset",
  "tenant_id": "tenant456",
  "timestamp": "2024-01-15T10:30:00Z",
  "properties": {
    "quality_score": 0.85
  },
  "relationships": []
}
```

### Output Events
Results are published to `graph-analytics-results` topic:
- Trust score updates
- Fraud alerts
- Recommendation sets
- Graph evolution metrics

## 🔧 Configuration

### Environment Variables
- `PULSAR_SERVICE_URL`: Pulsar broker URL (default: `pulsar://pulsar:6650`)
- `JANUSGRAPH_URL`: JanusGraph Gremlin server URL
- `IGNITE_NODES`: Comma-separated list of Ignite nodes
- `FLINK_PARALLELISM`: Job parallelism (default: 8)

### Tuning Parameters
- `TRUST_DECAY_FACTOR`: Rate of trust score decay (default: 0.99)
- `FRAUD_THRESHOLD`: Fraud detection sensitivity (default: 0.7)
- `RECOMMENDATION_CACHE_TTL`: Cache duration in seconds (default: 300)

## 🚀 Deployment

### Local Development
```bash
# Build the job
cd processing/flink/graph-analytics-job
pip install -r requirements.txt

# Run locally
python src/main.py
```

### Production Deployment
```bash
# Submit to Flink cluster
flink run -py src/main.py \
  --pyFiles src/ \
  --parallelism 8 \
  --jobmanager jobmanager:8081
```

## 📊 Monitoring

### Key Metrics
- **Trust Score Updates/sec**: Rate of trust score calculations
- **Fraud Alerts/min**: Number of fraud detections
- **Recommendation Latency**: Time to generate recommendations
- **Graph Update Lag**: Delay between event and graph update

### Health Checks
- Pulsar consumer lag
- JanusGraph connection status
- Ignite cache hit rate
- Memory usage and GC frequency

## 🔍 Complex Event Patterns

### Fraud Detection Patterns
1. **Multiple Failed Verifications**: 3+ failures followed by success
2. **Rapid Relationship Creation**: >10 edges created per day
3. **Suspicious Timing**: Regular intervals suggesting automation

### Trust Patterns
1. **Positive Reinforcement**: Successful collaborations boost trust
2. **Negative Impact**: Disputes and failures reduce trust
3. **Network Effect**: Trust propagates through connections

## 🛡️ Security Considerations

- All graph updates are tenant-isolated
- Trust scores are bounded between 0 and 1
- Fraud detection respects privacy boundaries
- Recommendations don't expose private relationships

## 🧪 Testing

### Unit Tests
```bash
pytest tests/test_processors.py -v
```

### Integration Tests
```bash
# Start test environment
docker-compose -f docker-compose.test.yml up -d

# Run integration tests
pytest tests/integration/ -v
```

## 📈 Performance

- Processes ~10K events/second per task
- Sub-second latency for trust updates
- 5-second SLA for recommendation generation
- Scales linearly with parallelism

## 🔗 Integration Points

- **JanusGraph**: Primary graph storage
- **Apache Ignite**: High-performance caching
- **Apache Pulsar**: Event streaming
- **Spark GraphX**: Batch analytics coordination 