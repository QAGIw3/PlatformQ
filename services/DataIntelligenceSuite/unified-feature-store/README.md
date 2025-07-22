# Unified Feature Store

A centralized feature store service for managing, serving, and monitoring ML features across the platform.

## Overview

The Unified Feature Store provides:
- **Feature Registry**: Central catalog of all features
- **Online & Offline Serving**: Low-latency and batch access
- **Feature Versioning**: Track feature evolution
- **Feature Monitoring**: Data drift and quality tracking
- **Feature Discovery**: Search and explore features

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Unified Feature Store                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │   Feature    │  │   Feature   │  │   Feature    │  │
│  │   Registry   │  │   Compute   │  │  Monitoring  │  │
│  └──────────────┘  └─────────────┘  └──────────────┘  │
│                                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │              Serving Infrastructure                 │ │
│  │  ┌──────────┐              ┌──────────────────┐   │ │
│  │  │  Online  │              │     Offline      │   │ │
│  │  │ (Ignite) │              │ (MinIO/Parquet)  │   │ │
│  │  └──────────┘              └──────────────────┘   │ │
│  └────────────────────────────────────────────────────┘ │
│                                                         │
│  Compute: Spark/Flink | Events: Pulsar | API: REST/gRPC │
└─────────────────────────────────────────────────────────┘
```

## Features

### Feature Management
- **Feature Definition**: Declarative feature specifications
- **Feature Groups**: Organize related features
- **Feature Versioning**: Track changes over time
- **Feature Lineage**: Understand feature dependencies
- **Feature Validation**: Schema and value validation

### Serving Capabilities
- **Online Serving**: Sub-millisecond latency via Ignite
- **Offline Serving**: Batch access via Parquet/Delta
- **Point-in-Time Queries**: Historical feature values
- **Feature Joins**: Combine features from multiple sources
- **Streaming Features**: Real-time feature computation

### Feature Engineering
- **Feature Pipelines**: Automated feature computation
- **Feature Transformations**: Built-in transformations
- **Custom Functions**: User-defined transformations
- **Backfill Support**: Historical feature computation
- **Incremental Updates**: Efficient updates

### Monitoring & Quality
- **Data Drift Detection**: Monitor feature distributions
- **Feature Quality Metrics**: Completeness, uniqueness
- **Usage Tracking**: Know which features are used
- **Performance Monitoring**: Latency and throughput
- **Alerts**: Automated alerting on issues

## API Endpoints

### Feature Management
- `POST /api/v1/features` - Register new feature
- `GET /api/v1/features/{name}` - Get feature details
- `PUT /api/v1/features/{name}` - Update feature
- `GET /api/v1/features` - List all features
- `POST /api/v1/feature-groups` - Create feature group

### Online Serving
- `POST /api/v1/get-online-features` - Get features for entities
- `POST /api/v1/get-online-features-bulk` - Bulk feature retrieval
- `GET /api/v1/features/{name}/value/{entity_id}` - Single feature value

### Offline Serving
- `POST /api/v1/get-training-data` - Get training dataset
- `POST /api/v1/get-batch-features` - Batch feature retrieval
- `POST /api/v1/point-in-time-query` - Historical features

### Feature Engineering
- `POST /api/v1/feature-pipelines` - Create pipeline
- `POST /api/v1/backfill` - Backfill features
- `GET /api/v1/pipelines/{id}/status` - Pipeline status

### Monitoring
- `GET /api/v1/monitoring/drift/{feature}` - Drift metrics
- `GET /api/v1/monitoring/quality/{feature}` - Quality metrics
- `GET /api/v1/monitoring/usage` - Usage statistics

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: unified-feature-store
SERVICE_PORT: 8000

# Storage Configuration
# Online Store
IGNITE_HOST: ignite
IGNITE_PORT: 10800
ONLINE_STORE_CACHE: feature_online

# Offline Store
MINIO_ENDPOINT: minio:9000
OFFLINE_STORE_BUCKET: feature-store
OFFLINE_STORE_FORMAT: delta

# Compute
SPARK_MASTER: spark://spark-master:7077
FLINK_JOBMANAGER: flink-jobmanager:8081

# Monitoring
ENABLE_DRIFT_DETECTION: true
DRIFT_CHECK_INTERVAL: 3600
QUALITY_CHECK_INTERVAL: 1800

# Performance
ONLINE_SERVING_TIMEOUT_MS: 100
BATCH_SIZE_LIMIT: 10000
CACHE_TTL_SECONDS: 86400
```

## Usage Examples

### Register a Feature
```python
response = requests.post('http://feature-store:8000/api/v1/features', json={
    "name": "user_purchase_count_7d",
    "description": "Number of purchases in last 7 days",
    "feature_group": "user_activity",
    "value_type": "int64",
    "entities": ["user_id"],
    "tags": ["purchase", "activity", "real-time"],
    "transformation": {
        "type": "window_aggregate",
        "source": "transactions",
        "window": "7d",
        "aggregation": "count",
        "group_by": "user_id"
    }
})
```

### Get Online Features
```python
# Get features for model inference
response = requests.post('http://feature-store:8000/api/v1/get-online-features', json={
    "entities": {
        "user_id": ["user_123", "user_456"],
        "merchant_id": ["merchant_789"]
    },
    "features": [
        "user_purchase_count_7d",
        "user_avg_transaction_amount",
        "merchant_risk_score"
    ]
})

# Returns:
{
    "results": [
        {
            "user_id": "user_123",
            "merchant_id": "merchant_789",
            "user_purchase_count_7d": 5,
            "user_avg_transaction_amount": 125.50,
            "merchant_risk_score": 0.12
        },
        ...
    ]
}
```

### Create Training Dataset
```python
# Get point-in-time correct features for training
response = requests.post('http://feature-store:8000/api/v1/get-training-data', json={
    "entity_dataframe": {
        "format": "parquet",
        "path": "s3://data/training_entities.parquet"
    },
    "features": [
        "user_activity:user_purchase_count_7d",
        "user_profile:user_age",
        "user_profile:user_segment",
        "merchant:merchant_category"
    ],
    "timestamp_column": "event_timestamp",
    "output_path": "s3://data/training_dataset.parquet"
})
```

### Monitor Feature Drift
```python
# Check if feature distribution has changed
response = requests.get('http://feature-store:8000/api/v1/monitoring/drift/user_purchase_count_7d')

# Returns:
{
    "feature": "user_purchase_count_7d",
    "drift_detected": true,
    "metrics": {
        "kl_divergence": 0.23,
        "psi": 0.18,
        "mean_shift": 2.5,
        "std_shift": 0.8
    },
    "baseline_stats": {...},
    "current_stats": {...},
    "recommendation": "Consider retraining models using this feature"
}
```

## Feature Definition Format

```yaml
# features/user_activity.yaml
feature_group:
  name: user_activity
  description: User activity features
  entities: [user_id]
  
features:
  - name: purchase_count_7d
    description: Purchases in last 7 days
    value_type: int64
    default_value: 0
    transformation:
      sql: |
        SELECT 
          user_id,
          COUNT(*) as value,
          MAX(timestamp) as feature_timestamp
        FROM transactions
        WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '7' DAY
        GROUP BY user_id
    
  - name: avg_purchase_amount_30d
    description: Average purchase amount in last 30 days
    value_type: float64
    default_value: null
    transformation:
      sql: |
        SELECT 
          user_id,
          AVG(amount) as value,
          MAX(timestamp) as feature_timestamp
        FROM transactions
        WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '30' DAY
        GROUP BY user_id
```

## Integration

### With ML Platform
```python
# ML Platform can directly fetch features
from feature_store_sdk import FeatureStore

fs = FeatureStore()
training_df = fs.get_training_data(
    entity_df=entity_df,
    features=["user_activity:*", "merchant:risk_score"]
)

# For model serving
features = fs.get_online_features(
    entities={"user_id": user_id},
    features=feature_list
)
```

### With Stream Processing
```python
# Compute streaming features with Flink
@streaming_feature(
    source="transactions_stream",
    feature_group="user_activity"
)
def purchase_velocity(stream):
    return stream \
        .key_by("user_id") \
        .window(TumblingWindow(minutes=5)) \
        .aggregate(CountAggregator()) \
        .map(lambda x: Feature(
            entity_id=x.user_id,
            name="purchase_velocity_5m",
            value=x.count
        ))
```

## Best Practices

1. **Feature Naming**: Use descriptive, consistent naming
2. **Versioning**: Version features when logic changes
3. **Documentation**: Document feature logic and usage
4. **Monitoring**: Set up alerts for drift and quality
5. **Testing**: Test feature pipelines before production

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 