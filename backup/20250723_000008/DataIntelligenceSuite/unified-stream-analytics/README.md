# Unified Stream Analytics Service

A consolidated service that combines stream processing and real-time analytics capabilities for the DataIntelligenceSuite.

## Overview

This service merges the functionality of:
- **stream-processing-service**: Real-time event processing with Apache Flink
- **analytics-service (real-time components)**: Real-time analytics and aggregations

## Features

### Stream Processing
- Apache Flink integration for complex stream processing
- Event-time and processing-time semantics
- Windowing operations (tumbling, sliding, session)
- Complex Event Processing (CEP)
- Exactly-once processing guarantees
- State management with RocksDB

### Real-Time Analytics
- Real-time aggregations and metrics
- Streaming SQL support
- Time-series analysis
- Anomaly detection
- Real-time dashboards
- Low-latency query processing

### Data Sources & Sinks
- Apache Pulsar integration
- Kafka compatibility
- WebSocket support for real-time updates
- REST API for stream management
- Integration with data stores (Ignite, Cassandra, Elasticsearch)

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Event Sources  │────▶│  Stream Engine   │────▶│  Analytics      │
│  (Pulsar/Kafka) │     │  (Apache Flink)  │     │  Processing     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                          │
                               ▼                          ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │  State Store     │     │  Results Store  │
                        │  (RocksDB)       │     │  (Ignite)       │
                        └──────────────────┘     └─────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.8+
- Apache Flink 1.17+
- Apache Pulsar or Kafka
- Apache Ignite

### Installation

```bash
cd services/DataIntelligenceSuite/unified-stream-analytics
pip install -r requirements.txt
```

### Configuration

```yaml
# config.yaml
service:
  name: unified-stream-analytics
  port: 8084

flink:
  parallelism: 4
  checkpoint_interval: 60000
  state_backend: rocksdb

pulsar:
  url: pulsar://localhost:6650
  
analytics:
  window_size: 300  # 5 minutes
  aggregation_interval: 10
```

### Running the Service

```bash
python -m app.main
```

## API Endpoints

### Stream Management
- `POST /api/v1/streams` - Create a new stream processing job
- `GET /api/v1/streams/{job_id}` - Get stream job status
- `DELETE /api/v1/streams/{job_id}` - Cancel stream job

### Analytics Queries
- `POST /api/v1/analytics/query` - Execute streaming SQL query
- `GET /api/v1/analytics/metrics` - Get real-time metrics
- `WS /api/v1/analytics/subscribe` - Subscribe to real-time updates

### Pipeline Management
- `POST /api/v1/pipelines` - Create analytics pipeline
- `GET /api/v1/pipelines/{pipeline_id}` - Get pipeline status

## Usage Examples

### Creating a Stream Processing Job

```python
from data_intelligence_common import StreamProcessor, StreamConfig

# Configure stream processor
config = StreamConfig(
    name="order_analytics",
    source_topics=["orders"],
    sink_topics=["order_aggregates"],
    window_type=WindowType.TUMBLING,
    window_size=timedelta(minutes=5)
)

# Create processor
processor = StreamProcessor(config)

# Define processing logic
async def process_orders(order):
    return {
        "order_id": order["id"],
        "amount": order["amount"],
        "timestamp": order["timestamp"],
        "region": order["region"]
    }

# Submit job
job_id = await processor.submit_job(process_orders)
```

### Real-Time Analytics Pipeline

```python
from data_intelligence_common import PipelineBuilder

# Build analytics pipeline
pipeline = PipelineBuilder("real_time_sales")
    .source(stream_processor, ["sales_events"])
    .transform(lambda x: x.filter("amount > 100"))
    .window(WindowType.SLIDING, timedelta(minutes=10), timedelta(minutes=1))
    .aggregate({
        "total_sales": "sum(amount)",
        "avg_sale": "avg(amount)",
        "transaction_count": "count(*)"
    })
    .sink(analytics_store, "sales_metrics")
    .build()

# Execute pipeline
results = await pipeline.execute()
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `stream_events_processed_total` - Total events processed
- `stream_processing_latency_seconds` - Processing latency histogram
- `stream_checkpoints_completed` - Successful checkpoints
- `analytics_queries_total` - Total analytics queries
- `analytics_query_duration_seconds` - Query execution time

## Development

### Project Structure

```
unified-stream-analytics/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── api/
│   │   ├── streams.py
│   │   └── analytics.py
│   ├── core/
│   │   ├── stream_engine.py
│   │   ├── analytics_engine.py
│   │   └── state_manager.py
│   └── models/
│       ├── stream_job.py
│       └── analytics_query.py
├── tests/
├── config.yaml
└── requirements.txt
```

### Testing

```bash
pytest tests/ -v --cov=app
```

## Migration Guide

### From stream-processing-service

1. Update imports:
   ```python
   # Old
   from stream_processing import StreamProcessor
   
   # New
   from data_intelligence_common import StreamProcessor
   ```

2. Update configuration to use unified format

3. Migrate custom processors to use new base classes

### From analytics-service (real-time)

1. Update API endpoints to new unified endpoints

2. Migrate real-time queries to streaming SQL format

3. Update WebSocket subscriptions to new format

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 