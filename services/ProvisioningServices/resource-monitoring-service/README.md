# Resource Monitoring Service

The Resource Monitoring Service provides real-time monitoring, metrics collection, and anomaly detection for all resources in the Platform Q ecosystem.

## Overview

This service enables:
- Real-time resource metrics collection
- Multi-source monitoring (Kubernetes, cloud providers, infrastructure)
- Anomaly detection and alerting
- Performance trend analysis
- Predictive analytics for resource usage

## Features

- **Multi-Source Monitoring**: Kubernetes, AWS, CloudStack, and custom metrics
- **Real-time Metrics**: CPU, memory, network, disk, and custom metrics
- **Anomaly Detection**: ML-based anomaly detection for early warning
- **Historical Analysis**: Time-series data storage and querying
- **Predictive Analytics**: Forecast resource usage and bottlenecks
- **Event Streaming**: Real-time metric events via Pulsar

## API Endpoints

### Metrics Collection
- `GET /api/v1/metrics/current` - Get current metrics for all services
- `GET /api/v1/metrics/{service_name}` - Get metrics for specific service
- `GET /api/v1/metrics/cluster` - Get cluster-wide metrics
- `POST /api/v1/metrics/custom` - Submit custom metrics

### Historical Data
- `GET /api/v1/metrics/history` - Get historical metrics
- `GET /api/v1/metrics/aggregate` - Get aggregated metrics
- `GET /api/v1/metrics/trends` - Get metric trends

### Anomaly Detection
- `GET /api/v1/anomalies` - Get detected anomalies
- `GET /api/v1/anomalies/{service_name}` - Get service-specific anomalies
- `POST /api/v1/anomalies/acknowledge` - Acknowledge anomaly

### Predictions
- `GET /api/v1/predictions/usage` - Get usage predictions
- `GET /api/v1/predictions/capacity` - Get capacity predictions

### Health & Status
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8006)
- `KUBERNETES_CONFIG` - Kubernetes configuration
- `PROMETHEUS_URL` - Prometheus server URL
- `ELASTICSEARCH_HOSTS` - Elasticsearch for metrics storage
- `PULSAR_URL` - Pulsar broker URL
- `COLLECTION_INTERVAL` - Metrics collection interval (seconds)
- `ANOMALY_DETECTION_ENABLED` - Enable anomaly detection

## Monitored Metrics

### System Metrics
- **CPU**: Usage percentage, throttling, load average
- **Memory**: Usage, available, swap, pressure
- **Disk**: IOPS, throughput, latency, usage
- **Network**: Bandwidth, packets, errors, latency

### Application Metrics
- **Request Rate**: Requests per second
- **Response Time**: P50, P95, P99 latencies
- **Error Rate**: 4xx, 5xx errors
- **Queue Depth**: Message queue sizes
- **Connection Pool**: Active, idle connections

### Custom Metrics
- Business-specific metrics
- Service-level indicators (SLIs)
- Key performance indicators (KPIs)

## Anomaly Detection

### Detection Methods
- **Statistical**: Z-score, moving average
- **Machine Learning**: Isolation Forest, LSTM
- **Rule-based**: Threshold violations
- **Pattern-based**: Recurring anomalies

### Anomaly Types
- **Resource Spikes**: Sudden usage increases
- **Performance Degradation**: Gradual slowdowns
- **Error Patterns**: Unusual error rates
- **Capacity Issues**: Resource exhaustion

## Data Collection

### Kubernetes Metrics
- Metrics Server API
- cAdvisor metrics
- Custom Resource Definitions (CRDs)
- Pod and node metrics

### Cloud Provider Metrics
- AWS CloudWatch
- CloudStack metrics API
- Provider-specific APIs

### Application Metrics
- Prometheus exporters
- StatsD protocol
- Custom metric APIs

## Storage Strategy

### Time-series Storage
- Elasticsearch for long-term storage
- Ignite for hot data cache
- Configurable retention policies

### Data Aggregation
- 1-minute resolution for recent data
- 5-minute resolution for weekly data
- Hourly resolution for monthly data
- Daily resolution for yearly data

## Integration

### Service Integration
- Resource Scaling Service for auto-scaling
- Cost Optimization Service for cost metrics
- Quota Management Service for usage limits

### Monitoring Stack
- Prometheus for metrics collection
- Grafana for visualization
- AlertManager for alerting

## Development

### Running Locally
```bash
cd services/resource-monitoring-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8006
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t resource-monitoring-service:latest -f services/resource-monitoring-service/Dockerfile .
```

## Architecture

The service consists of:

- **Metric Collectors**: Gather metrics from various sources
- **Aggregator**: Process and aggregate metrics
- **Anomaly Detector**: Identify unusual patterns
- **Time-series Store**: Persist historical data
- **Event Publisher**: Stream metric events

## Performance Optimization

- **Batch Collection**: Collect metrics in batches
- **Async Processing**: Non-blocking metric processing
- **Caching**: Cache frequently accessed metrics
- **Compression**: Compress historical data
- **Sampling**: Configurable metric sampling

## Monitoring

The service exposes Prometheus metrics for:
- Metrics collection rate and latency
- Anomaly detection performance
- Storage operations
- API response times
- Event publishing throughput 