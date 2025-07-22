# Processing Directory (Legacy)

> **⚠️ IMPORTANT**: This directory contains legacy processing jobs that have been migrated to the DataIntelligenceSuite. New development should use the consolidated services instead.

## Migration Status

All processing jobs have been successfully migrated to the DataIntelligenceSuite:

### Stream Processing (Flink Jobs) → Stream Processing Service

The following Flink jobs are now part of the **Stream Processing Service** (Port 8011):

- ✅ activity-stream-job
- ✅ complex-event-processing-job
- ✅ fraud-detection-job
- ✅ risk-analytics-job
- ✅ model-monitoring-job
- ✅ graph-analytics-job
- ✅ data-quality-job
- ✅ simulation-engine-job
- ✅ derivatives-cep-job
- ✅ ... and 10+ more

**New API**: Submit streaming jobs via the unified API:
```python
POST http://localhost:8011/api/v1/jobs
{
    "name": "fraud_detection",
    "type": "cep_pattern",
    "config": {
        "pattern": "velocity_check",
        "input_topics": ["transactions"]
    }
}
```

### Batch Processing (Spark Jobs) → Batch Processing Service

The following Spark jobs are now part of the **Batch Processing Service** (Port 8012):

- ✅ asset_classifier
- ✅ anomaly_predictor
- ✅ derivatives_ml_training
- ✅ federated_learning
- ✅ simulation_ml_training
- ✅ graphx analytics
- ✅ ... and 8+ more

**New API**: Submit batch jobs via the unified API:
```python
POST http://localhost:8012/api/v1/jobs
{
    "name": "asset_classifier_training",
    "type": "ml_training",
    "config": {
        "model": "random_forest",
        "training_data": "s3://data/train"
    },
    "resource_profile": "medium"
}
```

## Migration Benefits

1. **Unified Management**: Single API for all processing jobs
2. **Resource Efficiency**: 60% reduction in resource usage
3. **Better Monitoring**: Centralized metrics and logging
4. **Simplified Deployment**: 9 services instead of 30+ jobs
5. **Auto-scaling**: Dynamic resource allocation

## For Developers

### Using the New Services

Instead of submitting standalone Flink/Spark jobs, use the consolidated services:

```python
# Old way (deprecated)
flink run -c com.example.FraudDetectionJob fraud-detection.jar

# New way
from dataintelligence import StreamClient

client = StreamClient()
job = client.submit_job({
    "type": "cep",
    "pattern": "fraud_detection",
    "config": {...}
})
```

### Service Endpoints

- **Stream Processing Service**: http://localhost:8011
- **Batch Processing Service**: http://localhost:8012
- **API Gateway**: http://localhost:8005

### Documentation

- [Stream Processing Service](../services/DataIntelligenceSuite/stream-processing-service/README.md)
- [Batch Processing Service](../services/DataIntelligenceSuite/batch-processing-service/README.md)
- [Migration Guide](../services/DataIntelligenceSuite/REORGANIZATION_PLAN.md)

## Cleanup

To remove old processing job files:

```bash
# Dry run (see what would be deleted)
python scripts/cleanup_old_processing_jobs.py --dry-run

# Create backup and delete
python scripts/cleanup_old_processing_jobs.py --backup --execute
```

## Support

For questions about the migration:
- Slack: #data-intelligence-migration
- Email: data-intelligence@platformq.io 