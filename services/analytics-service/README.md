# Unified Analytics Service

The Unified Analytics Service provides comprehensive analytics capabilities for platformQ, combining batch analytics, real-time processing, machine learning, and cross-service monitoring into a single powerful platform.

## Overview

This service consolidates:
- **Batch Analytics**: Complex queries via Apache Trino
- **Real-time Analytics**: Time-series analysis via Apache Druid and Apache Ignite
- **Machine Learning**: Anomaly detection, forecasting, and predictive maintenance
- **Stream Processing**: Real-time event processing with Apache Flink integration
- **Cross-Service Monitoring**: Platform-wide dashboards and insights
- **WebSocket Streaming**: Real-time data streaming for dashboards and alerts

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Unified Analytics Service                  │
├─────────────────────────────────────────────────────────────┤
│                      Query Router                            │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Batch     │  Real-time   │    Cache    │     ML     │ │
│  │  (Trino)    │   (Druid)    │  (Ignite)   │  (MLflow)  │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Stream Processing                         │
│  ┌─────────────────────┬─────────────────────────────────┐ │
│  │   Apache Pulsar     │      Apache Flink               │ │
│  └─────────────────────┴─────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                 Monitoring & Dashboards                      │
│  ┌──────────────┬────────────────┬───────────────────────┐ │
│  │   Anomaly    │   Predictive   │  Cross-Service        │ │
│  │  Detection   │  Maintenance   │   Dashboards          │ │
│  └──────────────┴────────────────┴───────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Features

### 1. Unified Query Interface
- **Intelligent Routing**: Automatically routes queries to the optimal engine
- **Multi-Engine Support**: Seamlessly switches between Trino, Druid, and Ignite
- **Query Optimization**: Caches results and optimizes execution paths
- **Federated Queries**: Split queries across engines for best performance

### 2. Real-time Analytics
- **Time-Series Analysis**: Powered by Apache Druid
- **In-Memory Computing**: Sub-millisecond queries with Apache Ignite
- **Stream Processing**: Real-time event processing and aggregation
- **WebSocket Streaming**: Live data updates for dashboards

### 3. Machine Learning Operations
- **Anomaly Detection**: Real-time anomaly detection with multiple algorithms
- **Forecasting**: Time-series forecasting with Prophet and custom models
- **Predictive Maintenance**: Predict component failures before they occur
- **Online Learning**: Continuously update models with streaming data

### 4. Monitoring & Dashboards
- **Platform Overview**: Cross-service health and performance metrics
- **Service Comparison**: Compare metrics across different services
- **Resource Utilization**: Monitor CPU, memory, and other resources
- **Custom Dashboards**: Create and manage custom dashboards

## API Endpoints

### Query Endpoints
- `POST /api/v1/query` - Unified query interface
- `POST /api/v1/query/timeseries` - Time-series specific queries

### Monitoring Endpoints
- `GET /api/v1/monitor/{scope}` - Unified monitoring (platform/service/simulation/resource)
- `GET /api/v1/dashboards/{type}` - Get dashboard by type
- `POST /api/v1/dashboards` - Create new dashboard

### ML Operations
- `POST /api/v1/ml/{operation}` - ML operations (detect-anomalies/forecast/predict-maintenance/train-online)

### WebSocket Endpoints
- `WS /api/v1/ws/{stream_type}/{stream_id}` - Real-time streaming (dashboard/metrics/anomalies/analytics)

### Data Ingestion
- `POST /api/v1/metrics/ingest` - Ingest metrics into the pipeline

### Export Endpoints
- `GET /api/v1/export/prometheus` - Export metrics in Prometheus format

### Utility Endpoints
- `GET /health` - Health check
- `GET /api/v1/analytics/capabilities` - Get service capabilities
- `GET /api/v1/analytics/metadata/{datasource}` - Get datasource metadata

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=analytics-service
LOG_LEVEL=INFO

# Apache Pulsar
PULSAR_URL=pulsar://pulsar:6650

# Apache Druid
DRUID_BROKER_URL=http://druid-broker:8082
DRUID_COORDINATOR_URL=http://druid-coordinator:8081
DRUID_OVERLORD_URL=http://druid-overlord:8090

# Apache Ignite
IGNITE_HOST=ignite
IGNITE_PORT=10800

# Elasticsearch
ELASTICSEARCH_HOST=http://elasticsearch:9200

# Apache Trino
TRINO_HOST=trino
TRINO_PORT=8080

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000
```

## Query Examples

### 1. Unified Query (Auto-routing)
```json
POST /api/v1/query
{
    "metrics": ["cpu_usage", "memory_usage"],
    "time_range": "1h",
    "group_by": ["service_name"],
    "mode": "auto"
}
```

### 2. Time-Series Query
```json
POST /api/v1/query/timeseries
{
    "datasource": "platform_metrics",
    "metrics": ["request_rate", "error_rate"],
    "intervals": ["2024-01-01/2024-01-02"],
    "granularity": "hour"
}
```

### 3. Anomaly Detection
```json
POST /api/v1/ml/detect-anomalies
{
    "simulation_id": "sim-123",
    "metrics": {
        "cpu": 95.0,
        "memory": 87.5,
        "disk_io": 1200
    },
    "config": {
        "method": "isolation_forest",
        "sensitivity": 0.95
    }
}
```

### 4. Create Dashboard
```json
POST /api/v1/dashboards
{
    "name": "Service Performance",
    "type": "service-metrics",
    "config": {
        "services": ["auth-service", "analytics-service"],
        "metrics": ["latency", "throughput", "error_rate"],
        "time_range": "24h"
    },
    "refresh_interval": 30
}
```

## WebSocket Streaming

### Connect to Real-time Metrics
```javascript
const ws = new WebSocket('ws://analytics.platformq.local/api/v1/ws/metrics/my-stream');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log('Metrics update:', data);
};
```

### Subscribe to Anomaly Alerts
```javascript
const ws = new WebSocket('ws://analytics.platformq.local/api/v1/ws/anomalies/sim-123');

ws.onmessage = (event) => {
    const alert = JSON.parse(event.data);
    console.log('Anomaly detected:', alert);
};
```

## Development

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Running Tests
```bash
# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=app tests/
```

### Building Docker Image
```bash
docker build -t platformq/analytics-service:latest .
```

## Deployment

### Kubernetes Deployment
```bash
# Deploy using Helm
helm install analytics-service ./iac/kubernetes/charts/analytics-service

# Upgrade deployment
helm upgrade analytics-service ./iac/kubernetes/charts/analytics-service

# Check status
kubectl get pods -l app.kubernetes.io/name=analytics-service
```

### Scaling
The service supports horizontal scaling:
- Minimum replicas: 2
- Maximum replicas: 10
- Auto-scaling based on CPU/Memory utilization

## Performance Optimization

### Caching Strategy
- **Query Results**: Cached in Ignite with configurable TTL
- **ML Predictions**: 5-minute TTL for real-time predictions
- **Dashboard Data**: 30-second TTL for live dashboards
- **Anomaly Results**: Permanently cached for historical analysis

### Query Optimization
- **Automatic Mode Selection**: Routes to optimal engine
- **Federated Execution**: Splits queries across engines
- **Result Aggregation**: Merges results from multiple sources
- **Parallel Processing**: Executes independent queries in parallel

## Monitoring

### Metrics
- Query execution times by engine
- Cache hit/miss rates
- ML model performance metrics
- Stream processing lag
- WebSocket connection counts

### Alerts
- High query latency (> 5s for batch, > 500ms for real-time)
- Low cache hit rate (< 70%)
- Anomaly detection rate spike
- Stream processing backlog
- Engine connection failures

## Integration

### With Other Services
- **MLOps Service**: Model management and deployment
- **Seatunnel Service**: Data synchronization
- **Digital Asset Service**: Asset metadata
- **Simulation Service**: Real-time simulation metrics

### Event Topics
- `simulation.metrics.*` - Incoming simulation metrics
- `anomalies.*` - Outgoing anomaly alerts
- `predictions.*` - ML prediction results
- `dashboard.updates.*` - Dashboard update events

## Troubleshooting

### Common Issues

1. **Slow Queries**
   - Check if using correct mode (batch vs real-time)
   - Verify Ignite cache is operational
   - Review query complexity

2. **Missing Real-time Data**
   - Verify Pulsar connection
   - Check stream processor status
   - Ensure Druid ingestion is running

3. **ML Operations Failing**
   - Check MLflow connectivity
   - Verify model registration
   - Review input data format

4. **WebSocket Disconnections**
   - Check nginx timeout settings
   - Verify ingress WebSocket support
   - Monitor connection limits

## License

This service is part of the platformQ project and follows the project's licensing terms. 