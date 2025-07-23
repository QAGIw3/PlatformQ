# Feature Store Service

High-performance feature management service for ML pipelines, providing centralized feature storage, versioning, and serving capabilities.

## Overview

The Feature Store Service is a critical component of the ML platform that provides:
- **Online Feature Serving**: Low-latency (<5ms) feature retrieval using Apache Ignite
- **Feature Registry**: Centralized catalog of feature definitions with versioning
- **Real-time Updates**: Stream processing integration via Apache Pulsar
- **Feature Statistics**: Automatic computation of feature distributions and quality metrics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Feature Store Service                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Feature   │  │   Feature    │  │    Statistics    │  │
│  │  Registry   │  │    Cache     │  │     Engine       │  │
│  └──────┬──────┘  └──────┬───────┘  └────────┬─────────┘  │
│         │                 │                    │             │
│  ┌──────┴─────────────────┴───────────────────┴─────────┐  │
│  │              Apache Ignite (In-Memory)                │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │                                   │
│  ┌───────────────────────┴───────────────────────────────┐  │
│  │              Apache Pulsar (Streaming)                │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Feature Registry
- Feature definition with metadata, versioning, and ownership
- Support for various feature types (numeric, categorical, embeddings, etc.)
- Feature lifecycle management (draft, active, deprecated, archived)
- Tagging and search capabilities

### Online Serving
- Sub-millisecond latency using Apache Ignite in-memory cache
- Batch feature retrieval for multiple entities
- Default value support for missing features
- TTL-based cache expiration

### Real-time Processing
- Stream processing integration via Apache Pulsar
- Automatic feature statistics computation
- Event-driven feature updates
- Background synchronization tasks

### Feature Types Supported
- **Numeric**: Integer, float values
- **Categorical**: String categories
- **Binary**: Boolean features
- **Embeddings**: Vector representations
- **Text**: Text features
- **Image**: Image data references
- **Time Series**: Sequential data

## API Endpoints

### Feature Management

#### Register Feature
```http
POST /api/v1/features/register
Content-Type: application/json

{
  "name": "user_purchase_count_30d",
  "description": "Number of purchases in last 30 days",
  "feature_type": "numeric",
  "data_type": "int",
  "default_value": 0,
  "tags": ["user", "purchase", "behavioral"],
  "owner": "data-team"
}
```

#### List Features
```http
GET /api/v1/features?tags=user&status=active
```

#### Get Feature Definition
```http
GET /api/v1/features/{feature_name}
```

### Feature Serving

#### Set Features
```http
POST /api/v1/features/set
Content-Type: application/json

{
  "entity_id": "user_123",
  "features": {
    "user_purchase_count_30d": 5,
    "user_avg_order_value": 125.50,
    "user_preferred_category": "electronics"
  },
  "event_timestamp": "2024-01-15T10:30:00Z"
}
```

#### Get Features
```http
POST /api/v1/features/get
Content-Type: application/json

{
  "entity_ids": ["user_123", "user_456"],
  "feature_names": ["user_purchase_count_30d", "user_avg_order_value"],
  "use_default": true
}
```

#### Get Feature Statistics
```http
GET /api/v1/features/{feature_name}/statistics?window_hours=24
```

### Health Check
```http
GET /api/v1/health
```

## Configuration

### Environment Variables

```bash
# Apache Ignite
IGNITE_HOST=ignite
IGNITE_PORT=10800

# Apache Pulsar
PULSAR_URL=pulsar://pulsar:6650

# Service Configuration
LOG_LEVEL=INFO
```

## Deployment

### Docker

```bash
# Build image
docker build -t feature-store-service:latest .

# Run container
docker run -d \
  --name feature-store-service \
  -p 8000:8000 \
  -e IGNITE_HOST=ignite \
  -e PULSAR_URL=pulsar://pulsar:6650 \
  feature-store-service:latest
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: feature-store-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: feature-store-service
  template:
    metadata:
      labels:
        app: feature-store-service
    spec:
      containers:
      - name: feature-store-service
        image: feature-store-service:latest
        ports:
        - containerPort: 8000
        env:
        - name: IGNITE_HOST
          value: "ignite"
        - name: PULSAR_URL
          value: "pulsar://pulsar:6650"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

## Usage Examples

### Python Client

```python
import httpx
import asyncio

class FeatureStoreClient:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient()
    
    async def register_feature(self, feature_def):
        response = await self.client.post(
            f"{self.base_url}/api/v1/features/register",
            json=feature_def
        )
        return response.json()
    
    async def set_features(self, entity_id, features):
        response = await self.client.post(
            f"{self.base_url}/api/v1/features/set",
            json={
                "entity_id": entity_id,
                "features": features
            }
        )
        return response.json()
    
    async def get_features(self, entity_ids, feature_names):
        response = await self.client.post(
            f"{self.base_url}/api/v1/features/get",
            json={
                "entity_ids": entity_ids,
                "feature_names": feature_names,
                "use_default": True
            }
        )
        return response.json()

# Example usage
async def main():
    client = FeatureStoreClient()
    
    # Register a feature
    await client.register_feature({
        "name": "user_lifetime_value",
        "description": "Total revenue from user",
        "feature_type": "numeric",
        "data_type": "float",
        "default_value": 0.0,
        "tags": ["user", "revenue"]
    })
    
    # Set features for a user
    await client.set_features("user_123", {
        "user_lifetime_value": 1250.75,
        "user_segment": "premium"
    })
    
    # Get features
    result = await client.get_features(
        ["user_123"],
        ["user_lifetime_value", "user_segment"]
    )
    print(result)

asyncio.run(main())
```

## Performance Optimization

### Caching Strategy
- Features are cached in Apache Ignite with configurable TTL
- Hot features are kept in memory for sub-millisecond access
- Background refresh for frequently accessed features

### Batch Operations
- Bulk feature retrieval reduces network overhead
- Batch writes for efficient updates
- Parallel processing for multi-entity queries

### Connection Pooling
- Persistent connections to Ignite cluster
- Pulsar producer batching for efficient messaging
- Async processing for non-blocking operations

## Monitoring

### Metrics
- Feature retrieval latency
- Cache hit/miss rates
- Feature update throughput
- Registry size and growth
- Error rates by operation

### Health Checks
- Ignite cluster connectivity
- Pulsar broker availability
- Feature registry status
- Background task health

## Development

### Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run integration tests
pytest tests/integration/
```

## Troubleshooting

### Common Issues

1. **Connection to Ignite failed**
   - Verify Ignite cluster is running
   - Check network connectivity
   - Ensure correct host/port configuration

2. **Pulsar connection errors**
   - Verify Pulsar broker is accessible
   - Check topic permissions
   - Review Pulsar logs

3. **Feature not found**
   - Ensure feature is registered
   - Check feature status (not archived)
   - Verify correct feature name

4. **High latency**
   - Check Ignite cache statistics
   - Monitor network latency
   - Review cache eviction policies

## License

This service is part of the PlatformQ project and follows the project's licensing terms. 