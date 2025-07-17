# Data Lake Service

A comprehensive medallion architecture data lake service implementing Bronze, Silver, and Gold layers with advanced data quality framework.

## Features

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion from multiple sources
- **Silver Layer**: Cleansed and standardized data
- **Gold Layer**: Business-ready aggregated datasets

### Data Ingestion
- **Multiple Sources**: Apache Pulsar, APIs, Databases, Files (MinIO/S3), IoT devices
- **Ingestion Modes**: Batch, Streaming, Incremental
- **Built-in Transformations**: Data cleansing, enrichment, validation

### Data Quality Framework
- **Multi-dimensional Quality Checks**: Accuracy, Completeness, Consistency, Validity, Uniqueness
- **Quality Profiling**: Column statistics, anomaly detection, data lineage
- **Integration**: Great Expectations, Deequ, custom statistical checks

### Processing Pipeline
- **Automated Layer Transitions**: Bronze → Silver → Gold
- **Transformation Registry**: Reusable transformation functions
- **Optimization**: Partitioning strategies, Delta Lake optimization

## API Endpoints

### Ingestion
- `POST /api/v1/ingest` - Start data ingestion job
- `GET /api/v1/ingest/{job_id}` - Get ingestion job status

### Processing
- `POST /api/v1/process` - Start layer processing job
- `GET /api/v1/process/{job_id}` - Get processing job status

### Quality
- `POST /api/v1/quality/check` - Run quality check on dataset
- `POST /api/v1/quality/rules` - Add custom quality rule

### Catalog & Lineage
- `GET /api/v1/catalog` - Get data catalog
- `GET /api/v1/lineage/{dataset_id}` - Get data lineage

### Operations
- `POST /api/v1/compact/{layer}` - Compact Delta tables
- `GET /api/v1/jobs` - List active jobs

## Usage Examples

### 1. Ingest Data from Apache Pulsar

```python
POST /api/v1/ingest
{
    "source_type": "pulsar",
    "source_config": {
        "service_url": "pulsar://localhost:6650",
        "topics": ["sensor-data"],
        "subscription_name": "data-lake-consumer",
        "schema_type": "avro"
    },
    "target_dataset": "sensor_telemetry",
    "ingestion_mode": "streaming",
    "transformations": [
        {
            "name": "handle_nulls",
            "params": {"strategy": "drop"}
        }
    ]
}
```

### 2. Process Bronze to Silver

```python
POST /api/v1/process
{
    "source_layer": "bronze",
    "target_layer": "silver",
    "dataset_name": "sensor_telemetry",
    "transformations": [
        {
            "name": "remove_duplicates",
            "params": {"key_columns": ["sensor_id", "timestamp"]}
        },
        {
            "name": "standardize_strings",
            "params": {"case": "lower"}
        },
        {
            "name": "fix_timestamps",
            "params": {"target_timezone": "UTC"}
        }
    ],
    "quality_threshold": 0.95
}
```

### 3. Create Gold Layer Aggregation

```python
POST /api/v1/process
{
    "source_layer": "silver",
    "target_layer": "gold",
    "dataset_name": "sensor_analytics",
    "transformations": [
        {
            "name": "time_window_aggregation",
            "params": {
                "window_duration": "1 hour",
                "group_by": ["sensor_type", "location"],
                "aggregations": {
                    "temperature": ["avg", "max", "min"],
                    "pressure": ["avg", "stddev"]
                }
            }
        }
    ],
    "optimization_config": {
        "sort_columns": ["window_start", "sensor_type"],
        "compression": "snappy"
    }
}
```

### 4. Run Quality Check

```python
POST /api/v1/quality/check
{
    "dataset_name": "sensor_telemetry",
    "layer": "silver",
    "rules": ["completeness_critical_fields", "statistical_outlier_detection"]
}
```

## Configuration

### Environment Variables

```bash
# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DATA_LAKE_BUCKET=platformq-data-lake

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=2g
```

### Quality Rules Configuration

Quality rules can be customized by adding new rules via the API:

```python
POST /api/v1/quality/rules
{
    "rule_id": "custom_date_range",
    "name": "Valid Date Range",
    "description": "Ensure dates are within business range",
    "layer": "silver",
    "check_function": "lambda df: df.filter(F.col('date').between('2020-01-01', '2025-12-31'))",
    "severity": "error"
}
```

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Data Sources   │     │  Bronze Layer    │     │  Silver Layer   │
│                 │     │                  │     │                 │
│ • Pulsar        │────▶│ • Raw Data       │────▶│ • Cleansed      │
│ • APIs          │     │ • Immutable      │     │ • Standardized  │
│ • Databases     │     │ • Partitioned    │     │ • Validated     │
│ • Files         │     │                  │     │                 │
│ • IoT           │     └──────────────────┘     └─────────────────┘
└─────────────────┘              │                        │
                                 ▼                        ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │ Quality Framework│     │   Gold Layer    │
                        │                  │     │                 │
                        │ • Profiling      │     │ • Aggregated    │
                        │ • Validation     │────▶│ • Business Ready│
                        │ • Anomaly Det.   │     │ • Optimized     │
                        │                  │     │                 │
                        └──────────────────┘     └─────────────────┘
```

## Performance Optimization

1. **Partitioning**: Data is automatically partitioned by date and key columns
2. **Delta Lake**: Provides ACID transactions, time travel, and optimization
3. **Spark Adaptive Query**: Enabled for dynamic optimization
4. **Compression**: Configurable compression for storage efficiency
5. **Caching**: Frequently accessed Gold datasets can be cached

## Monitoring

The service provides comprehensive monitoring through:
- Job status tracking
- Quality score metrics
- Data lineage visualization
- Anomaly detection alerts
- Processing performance metrics

## Future Enhancements

1. **ML-based Quality Rules**: Automatic quality rule generation using machine learning
2. **Real-time Dashboards**: Integration with Superset for visualization
3. **Cost Optimization**: Storage tiering and lifecycle management
4. **Advanced Lineage**: Column-level lineage tracking
5. **Data Versioning**: Full history and rollback capabilities 