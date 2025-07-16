# Real-time Analytics Service

## Purpose

The **Real-time Analytics Service** provides low-latency, high-throughput analytics capabilities for the platformQ ecosystem. It leverages Apache Ignite's in-memory computing capabilities to deliver instant insights on streaming data, enabling real-time dashboards, alerts, and operational intelligence.

## Key Features

- **In-Memory Analytics**: Sub-millisecond query response times using Ignite
- **Real-time Metrics**: Process and aggregate streaming metrics
- **WebSocket API**: Live data push to dashboards and applications
- **Time Series Data**: Efficient storage and querying of time-series metrics
- **Continuous Queries**: Subscribe to metric changes in real-time
- **Multi-dimensional Analytics**: Slice and dice data by multiple dimensions
- **Dashboard Management**: Create and manage real-time dashboards
- **Prometheus Export**: Export metrics in Prometheus format
- **Auto-scaling Metrics**: Automatic metric computation from events

## Architecture

The service consists of:

1. **Ignite Cache Layer**: In-memory data grids for metrics and aggregations
2. **Stream Processor**: Consumes events and computes metrics
3. **WebSocket Server**: Real-time data delivery to clients
4. **Query Engine**: Executes analytical queries on cached data
5. **Dashboard Manager**: Stores and manages dashboard configurations

## Metric Types

The service supports various metric types:

- **Count**: Total number of events
- **Sum**: Sum of numeric values
- **Average**: Mean of values over time
- **Min/Max**: Minimum and maximum values
- **Percentile**: 50th, 95th, 99th percentiles
- **Rate**: Events per second/minute
- **Unique**: Distinct count of values

## API Endpoints

### Metrics Ingestion

- `POST /api/v1/metrics`: Ingest metrics into the system
- `GET /api/v1/metrics/{metric_name}/current`: Get current metric value

### Analytics Queries

- `POST /api/v1/query`: Execute analytical queries
- `GET /api/v1/export/prometheus`: Export metrics in Prometheus format

### Dashboard Management

- `POST /api/v1/dashboards`: Create a dashboard
- `GET /api/v1/dashboards`: List dashboards
- `GET /api/v1/dashboards/{dashboard_id}`: Get dashboard details

### WebSocket

- `WS /ws/metrics/{client_id}`: Real-time metric updates

## Metric Ingestion

### Single Metric

```json
POST /api/v1/metrics
{
  "metrics": [{
    "name": "api.requests.count",
    "type": "count",
    "value": 1,
    "tags": {
      "endpoint": "/api/v1/users",
      "method": "GET",
      "status": "200"
    }
  }]
}
```

### Batch Metrics

```json
POST /api/v1/metrics
{
  "metrics": [
    {
      "name": "system.cpu.usage",
      "type": "average",
      "value": 45.2,
      "window": "1m"
    },
    {
      "name": "system.memory.used",
      "type": "average",
      "value": 2147483648,
      "window": "1m"
    },
    {
      "name": "api.response.time",
      "type": "percentile",
      "value": 125,
      "tags": {"endpoint": "/api/v1/search"}
    }
  ]
}
```

## Analytics Queries

### Time Series Query

```json
POST /api/v1/query
{
  "metrics": ["api.requests.count", "api.errors.count"],
  "window": "5m",
  "aggregation": "sum",
  "start_time": "2024-01-01T00:00:00Z",
  "end_time": "2024-01-01T12:00:00Z",
  "group_by": ["endpoint"]
}
```

### Real-time Query

```json
POST /api/v1/query
{
  "metrics": ["users.active"],
  "window": "1m",
  "aggregation": "unique",
  "filters": {
    "tags.platform": "web"
  }
}
```

## WebSocket API

### Connection

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/metrics/my-client-id?tenant_id=tenant1');
```

### Subscribe to Metrics

```javascript
ws.send(JSON.stringify({
  type: 'subscribe',
  metrics: ['api.requests.count', 'api.response.time']
}));
```

### Receive Updates

```javascript
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'metric_data') {
    // Update dashboard with new data
    updateChart(data.data);
  }
};
```

## Dashboard Configuration

### Create Dashboard

```json
POST /api/v1/dashboards
{
  "name": "API Performance Dashboard",
  "description": "Real-time API performance metrics",
  "refresh_interval": 5,
  "widgets": [
    {
      "id": "requests-chart",
      "type": "line_chart",
      "title": "API Requests",
      "metric_query": {
        "metrics": ["api.requests.count"],
        "window": "1m",
        "aggregation": "sum"
      },
      "position": {"x": 0, "y": 0, "width": 6, "height": 4}
    },
    {
      "id": "response-time-gauge",
      "type": "gauge",
      "title": "Response Time (95th percentile)",
      "metric_query": {
        "metrics": ["api.response.time"],
        "window": "5m",
        "aggregation": "percentile"
      },
      "visualization_config": {
        "min": 0,
        "max": 1000,
        "thresholds": [
          {"value": 100, "color": "green"},
          {"value": 500, "color": "yellow"},
          {"value": 1000, "color": "red"}
        ]
      },
      "position": {"x": 6, "y": 0, "width": 6, "height": 4}
    }
  ]
}
```

## Automatic Metrics

The service automatically computes metrics from platform events:

### Event Metrics

- `events.{type}.count`: Count of events by type
- `events.processing.time`: Event processing latency

### User Metrics

- `users.active`: Active users per time window
- `users.actions.count`: User actions by type

### Performance Metrics

- `api.response.time`: API response times
- `api.requests.count`: Request counts by endpoint
- `api.errors.rate`: Error rate by endpoint

### System Metrics

- `system.throughput`: Events processed per second
- `system.queue.size`: Event queue depth

## Ignite Cache Configuration

The service uses multiple Ignite caches:

### Metrics Cache

- **Name**: `realtime_metrics`
- **TTL**: 1 hour
- **Purpose**: Store raw metric values

### Time Series Cache

- **Name**: `timeseries_data`
- **TTL**: 24 hours
- **Purpose**: Store time-series data points

### Aggregations Cache

- **Name**: `aggregated_metrics`
- **TTL**: Permanent
- **Purpose**: Pre-computed aggregations

### Dashboard Cache

- **Name**: `dashboards`
- **TTL**: Permanent
- **Purpose**: Dashboard configurations

## Performance Optimization

### Caching Strategy

1. **Hot Data**: Recent metrics in memory
2. **Warm Data**: Aggregated data in Ignite
3. **Cold Data**: Historical data in Cassandra

### Query Optimization

1. **Pre-aggregation**: Common queries pre-computed
2. **Partitioning**: Data partitioned by time and tenant
3. **Indexing**: Secondary indices on tags
4. **Parallel Processing**: Queries distributed across Ignite nodes

## Integration Examples

### With Flink Jobs

```python
# Flink job publishes metrics
metrics = [
    {
        "name": "flink.job.records.processed",
        "type": "count",
        "value": 1000000,
        "tags": {"job_id": "activity-stream"}
    }
]
```

### With Airflow DAGs

```python
# Airflow operator tracks execution time
def track_dag_metrics(**context):
    duration = context['task_instance'].duration
    metrics = [{
        "name": "airflow.dag.duration",
        "type": "average",
        "value": duration,
        "tags": {
            "dag_id": context['dag'].dag_id,
            "task_id": context['task'].task_id
        }
    }]
    # Send to analytics service
```

### With Business Services

```python
# Track business metrics
async def process_order(order):
    # Process order
    result = await process_order_logic(order)
    
    # Track metrics
    metrics = [
        {
            "name": "business.orders.processed",
            "type": "count",
            "value": 1,
            "tags": {"status": result.status}
        },
        {
            "name": "business.order.value",
            "type": "sum",
            "value": order.total_amount
        }
    ]
    
    await analytics_client.ingest_metrics(metrics)
```

## Monitoring & Alerting

### Health Monitoring

The service exposes health metrics:

- Ignite cluster status
- Cache sizes and hit rates
- WebSocket connection count
- Stream processor lag

### Alert Configuration

Configure alerts based on metrics:

```json
{
  "alert_name": "High Error Rate",
  "metric": "api.errors.rate",
  "condition": {
    "operator": ">",
    "threshold": 0.05,
    "duration": "5m"
  },
  "actions": ["email", "slack"]
}
```

## Best Practices

### Metric Naming

1. Use dot notation: `service.resource.metric`
2. Be consistent: `api.requests.count` not `requests_api_total`
3. Include units: `response.time.ms` not just `response.time`

### Tagging Strategy

1. Keep cardinality low
2. Use standard tags: `service`, `endpoint`, `status`
3. Avoid user-specific tags

### Query Patterns

1. Use appropriate windows for aggregations
2. Limit time ranges for large queries
3. Use filters to reduce data scanned

## Troubleshooting

### Common Issues

1. **Missing Metrics**: Check event consumer is running
2. **Slow Queries**: Reduce time range or use pre-aggregations
3. **WebSocket Disconnects**: Check client keep-alive
4. **Memory Issues**: Adjust Ignite cache sizes

### Debugging

1. Enable debug logging: `LOG_LEVEL=DEBUG`
2. Check Ignite metrics: Cache hit rates, memory usage
3. Monitor event lag: Consumer group lag in Pulsar
4. Profile queries: Use Ignite's query profiling

## Configuration

Environment variables:

- `IGNITE_HOST`: Ignite cluster hostname
- `IGNITE_PORT`: Ignite client port (default: 10800)
- `PULSAR_URL`: Pulsar broker URL
- `CACHE_TTL`: Default cache TTL in seconds
- `MAX_QUERY_SIZE`: Maximum query result size 