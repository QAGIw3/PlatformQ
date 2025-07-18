# Unified Analytics Service

A high-performance analytics service that provides both batch and real-time analytics capabilities for the PlatformQ ecosystem.

## Overview

The Unified Analytics Service combines:
- **Batch Analytics**: Complex queries via Trino/Hive for historical analysis
- **Real-time Analytics**: Low-latency queries via Druid/Ignite for live data
- **Automatic Mode Selection**: Intelligently routes queries based on characteristics
- **ML-Powered Predictions**: Time series forecasting and anomaly detection
- **WebSocket Streaming**: Real-time metric updates for dashboards

## Features

### Query Execution Modes

1. **Batch Mode** (Trino)
   - Complex SQL queries
   - Historical data analysis
   - Large-scale aggregations
   - Data quality assessments

2. **Real-time Mode** (Druid/Ignite)
   - Sub-second query response
   - Live metric monitoring
   - Stream processing
   - In-memory caching

3. **Auto Mode**
   - Automatically selects optimal engine
   - Based on time range and query complexity
   - Transparent to the user

### Analytics Capabilities

- **Asset Analytics**: Summary statistics, lineage impact, quality scores
- **User Behavior**: Activity patterns, engagement metrics
- **Quality Trends**: Data quality over time, anomaly detection
- **Simulation Metrics**: Real-time performance monitoring
- **Predictive Analytics**: ML-based forecasting
- **Anomaly Detection**: Automated outlier identification

## API Endpoints

### Query Execution

```http
POST /api/v1/query
Content-Type: application/json

{
  "query": "SELECT * FROM assets WHERE created_at > '2024-01-01'",  // Optional: raw SQL
  "query_type": "asset_summary",  // Optional: predefined query
  "mode": "auto",  // auto, batch, or realtime
  "filters": {
    "asset_type": "3d_model"
  },
  "time_range": "7d",  // 1h, 1d, 7d, 30d, 90d
  "limit": 1000
}
```

Response:
```json
{
  "mode": "batch",
  "query_type": "asset_summary",
  "data": [...],
  "summary": {
    "asset_count": {
      "mean": 150.5,
      "min": 10,
      "max": 500,
      "std": 45.2
    }
  },
  "metadata": {
    "row_count": 1000,
    "execution_time_ms": 234.5,
    "engine": "trino"
  },
  "execution_time_ms": 234.5
}
```

### Dashboards

#### Platform Overview
```http
GET /api/v1/dashboards/overview?time_range=7d
```

Returns aggregated metrics across assets, users, quality, and real-time data.

#### Simulation Dashboard
```http
GET /api/v1/dashboards/simulation/{simulation_id}
```

Real-time dashboard for specific simulation monitoring.

### Real-time Streaming

```javascript
// WebSocket connection for live metrics
const ws = new WebSocket('ws://analytics-service:8000/api/v1/ws/metrics/{simulation_id}');

ws.onmessage = (event) => {
  const metrics = JSON.parse(event.data);
  // Update dashboard
};
```

### Machine Learning

#### Metric Prediction
```http
POST /api/v1/analytics/ml/predict
{
  "simulation_id": "sim-123",
  "metric_name": "cpu_usage",
  "horizon": 10  // Predict next 10 time points
}
```

#### Anomaly Detection
```http
GET /api/v1/analytics/anomalies?time_range=1h
```

### Report Generation

```http
POST /api/v1/analytics/reports/generate
{
  "report_type": "executive_summary",
  "parameters": {
    "include_predictions": true
  }
}
```

Supported report types:
- `executive_summary`
- `performance_analysis`
- `quality_assessment`
- `user_engagement`
- `simulation_performance`

## Configuration

### Environment Variables

```bash
# Pulsar
PULSAR_URL=pulsar://pulsar:6650

# Druid
DRUID_COORDINATOR_URL=http://druid-coordinator:8081
DRUID_BROKER_URL=http://druid-broker:8082
DRUID_OVERLORD_URL=http://druid-overlord:8090

# Ignite
IGNITE_HOST=ignite
IGNITE_PORT=10800

# Elasticsearch
ELASTICSEARCH_HOST=http://elasticsearch:9200

# Service URLs
UNIFIED_DATA_SERVICE_URL=http://unified-data-service:8000
MLOPS_SERVICE_URL=http://mlops-service:8000
```

## Query Types

### Predefined Query Types

1. **asset_summary**: Asset distribution and statistics
2. **user_behavior**: User activity and engagement metrics
3. **quality_trends**: Data quality trends over time
4. **simulation_stats**: Simulation performance statistics
5. **current_metrics**: Real-time metric snapshot

### Custom Queries

For batch mode, you can provide raw SQL queries that will be executed via Trino:

```sql
WITH monthly_assets AS (
  SELECT 
    DATE_TRUNC('month', created_at) as month,
    asset_type,
    COUNT(*) as asset_count
  FROM cassandra.platformq.digital_assets
  WHERE created_at >= CURRENT_DATE - INTERVAL '6' MONTH
  GROUP BY 1, 2
)
SELECT * FROM monthly_assets
ORDER BY month DESC, asset_count DESC
```

## Performance Optimization

### Query Routing Logic

The service automatically routes queries based on:

1. **Time Range**
   - < 1 hour: Real-time (Ignite)
   - 1-24 hours: Real-time (Druid)
   - > 24 hours: Batch (Trino)

2. **Query Complexity**
   - Simple aggregations: Real-time
   - Complex joins: Batch
   - Historical analysis: Batch

3. **Data Volume**
   - Small datasets: Real-time
   - Large datasets: Batch

### Caching Strategy

- **Ignite**: In-memory cache for hot data
- **Druid**: Time-series optimized storage
- **Result Cache**: 60-second TTL for dashboard queries

## Monitoring

### Health Check

```http
GET /api/v1/analytics/health
```

Returns status of all dependencies:
- Druid
- Ignite
- Elasticsearch
- Unified Data Service

### Metrics

The service exposes Prometheus metrics:
- Query execution times
- Cache hit rates
- Error rates by query type
- Active WebSocket connections

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start dependencies
docker-compose up -d druid ignite elasticsearch

# Run service
uvicorn app.main:app --reload --port 8000
```

### Testing

```bash
# Unit tests
pytest tests/unit

# Integration tests
pytest tests/integration

# Load tests
locust -f tests/load/locustfile.py
```

### Adding New Query Types

1. Add query builder in `AnalyticsQueryRouter`:
```python
@staticmethod
def _build_my_query_sql(query: AnalyticsQuery) -> str:
    # Build SQL
    return sql
```

2. Register in `_build_sql_from_query_type`:
```python
query_builders = {
    "my_query": AnalyticsQueryRouter._build_my_query_sql,
    # ...
}
```

3. Update documentation

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   API Gateway   │────▶│  Analytics   │────▶│   Trino     │
└─────────────────┘     │   Service    │     └─────────────┘
                        │              │     
                        │  ┌────────┐  │     ┌─────────────┐
                        │  │ Router │  │────▶│    Druid    │
                        │  └────────┘  │     └─────────────┘
                        │              │     
                        │              │     ┌─────────────┐
                        └──────────────┘────▶│   Ignite    │
                                             └─────────────┘
```

## Troubleshooting

### Common Issues

1. **Slow Queries**
   - Check query execution plan
   - Verify appropriate mode selection
   - Consider adding indexes

2. **WebSocket Disconnections**
   - Check network stability
   - Verify subscription limits
   - Monitor memory usage

3. **Mode Selection Issues**
   - Explicitly specify mode
   - Check time range format
   - Verify query complexity

### Debug Mode

Enable debug logging:
```python
import logging
logging.getLogger("analytics-service").setLevel(logging.DEBUG)
```

## Contributing

1. Follow the standardized service structure
2. Add comprehensive tests
3. Update documentation
4. Ensure backwards compatibility 