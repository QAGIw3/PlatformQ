# Unified Batch Analytics Service

A consolidated service that combines batch processing and batch analytics capabilities for the DataIntelligenceSuite.

## Overview

This service merges the functionality of:
- **batch-processing-service**: Large-scale batch data processing with Apache Spark
- **analytics-service (batch components)**: Batch analytics, reporting, and ML processing

## Features

### Batch Processing
- Apache Spark integration for distributed processing
- Support for multiple data formats (Parquet, CSV, JSON, Avro)
- Incremental processing capabilities
- Data partitioning and optimization
- Fault-tolerant processing with checkpointing
- Resource-aware scheduling

### Batch Analytics
- Complex aggregations and transformations
- SQL-based analytics with Spark SQL
- Machine learning pipelines with MLlib
- Report generation and scheduling
- Data profiling and statistics
- Historical trend analysis

### Data Integration
- Integration with data lakes (MinIO)
- Connectivity to data warehouses
- Support for multiple data sources
- ETL/ELT pipeline capabilities
- Data quality validation
- Schema evolution handling

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Data Sources   │────▶│  Spark Engine    │────▶│  Analytics      │
│  (Files/DBs)    │     │  (Distributed)   │     │  Processing     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                          │
                               ▼                          ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │  Data Lake       │     │  Results Store  │
                        │  (MinIO)         │     │  (Cassandra)    │
                        └──────────────────┘     └─────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.8+
- Apache Spark 3.4+
- MinIO or S3-compatible storage
- Cassandra or compatible database

### Installation

```bash
cd services/DataIntelligenceSuite/unified-batch-analytics
pip install -r requirements.txt
```

### Configuration

```yaml
# config.yaml
service:
  name: unified-batch-analytics
  port: 8085

spark:
  master: local[*]
  app_name: UnifiedBatchAnalytics
  config:
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true

storage:
  data_lake:
    endpoint: http://localhost:9000
    access_key: minioadmin
    secret_key: minioadmin
    
analytics:
  default_batch_size: 10000
  max_parallel_jobs: 5
```

### Running the Service

```bash
python -m app.main
```

## API Endpoints

### Batch Job Management
- `POST /api/v1/batch/jobs` - Submit a batch processing job
- `GET /api/v1/batch/jobs/{job_id}` - Get job status
- `GET /api/v1/batch/jobs/{job_id}/results` - Get job results

### Analytics Operations
- `POST /api/v1/analytics/sql` - Execute Spark SQL query
- `POST /api/v1/analytics/aggregate` - Perform aggregations
- `GET /api/v1/analytics/reports/{report_id}` - Get generated report

### Pipeline Management
- `POST /api/v1/pipelines` - Create batch pipeline
- `GET /api/v1/pipelines/{pipeline_id}` - Get pipeline status
- `POST /api/v1/pipelines/{pipeline_id}/schedule` - Schedule pipeline

## Usage Examples

### Creating a Batch Processing Job

```python
from data_intelligence_common import BatchProcessor, BatchConfig

# Configure batch processor
config = BatchConfig(
    name="sales_analysis",
    input_format="parquet",
    output_format="parquet",
    partition_columns=["year", "month"],
    enable_broadcast_join=True
)

# Create processor
processor = BatchProcessor(config)

# Submit job
result = await processor.process("s3://data/sales/2024/*")
```

### Batch Analytics Pipeline

```python
from data_intelligence_common import PipelineBuilder

# Build analytics pipeline
pipeline = PipelineBuilder("monthly_sales_report")
    .source(batch_processor, "s3://data/sales/")
    .transform(lambda df: df.filter("status = 'completed'"))
    .quality(quality_processor, [
        NullCheck("amount"),
        RangeCheck("amount", min=0, max=1000000)
    ])
    .transform(lambda df: df.groupBy("region", "product")
                            .agg({"amount": "sum", "quantity": "sum"}))
    .sink(batch_processor, "s3://reports/monthly_sales/")
    .build()

# Execute pipeline
results = await pipeline.execute()
```

### Incremental Processing

```python
# Process only new data since last run
result = await processor.process_incremental(
    source_path="s3://data/events/",
    last_processed_timestamp=datetime(2024, 1, 1),
    timestamp_column="event_time"
)
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `batch_jobs_total` - Total batch jobs submitted
- `batch_job_duration_seconds` - Job execution time histogram
- `batch_records_processed_total` - Total records processed
- `spark_executor_count` - Active Spark executors
- `spark_memory_usage_bytes` - Spark memory usage

## Development

### Project Structure

```
unified-batch-analytics/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── api/
│   │   ├── batch.py
│   │   └── analytics.py
│   ├── core/
│   │   ├── spark_engine.py
│   │   ├── analytics_processor.py
│   │   └── job_scheduler.py
│   └── models/
│       ├── batch_job.py
│       └── analytics_result.py
├── tests/
├── config.yaml
└── requirements.txt
```

### Testing

```bash
pytest tests/ -v --cov=app
```

## Performance Optimization

### Spark Tuning
- Use adaptive query execution for automatic optimization
- Enable dynamic allocation for resource efficiency
- Partition data appropriately for parallelism
- Use broadcast joins for small tables

### Data Optimization
- Use columnar formats (Parquet) for analytics
- Implement partition pruning strategies
- Cache frequently accessed datasets
- Use data skipping indexes

## Migration Guide

### From batch-processing-service

1. Update job submission APIs to new format
2. Migrate custom transformations to new processor classes
3. Update configuration files

### From analytics-service (batch)

1. Convert SQL queries to Spark SQL format
2. Update report generation logic
3. Migrate scheduled jobs to new scheduler

## Advanced Features

### Machine Learning Integration

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

# Create ML pipeline
ml_pipeline = Pipeline(stages=[
    VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features"),
    RandomForestRegressor(featuresCol="features", labelCol="target")
])

# Train model
model = ml_pipeline.fit(training_data)

# Make predictions
predictions = model.transform(test_data)
```

### Custom UDFs

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define custom function
def categorize_amount(amount):
    if amount < 100:
        return "small"
    elif amount < 1000:
        return "medium"
    else:
        return "large"

# Register UDF
categorize_udf = udf(categorize_amount, StringType())

# Use in transformations
df = df.withColumn("category", categorize_udf("amount"))
```

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 