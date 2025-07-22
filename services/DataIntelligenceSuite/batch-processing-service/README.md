# Batch Processing Service

A unified service for all batch processing needs, consolidating multiple Spark jobs into a single, scalable service with enhanced file processing capabilities.

## Overview

The Batch Processing Service provides:
- Large-scale data processing
- ML model training
- ETL/ELT pipelines
- Distributed computing
- Feature engineering
- Batch analytics
- Specialized file processing (3D models, CAD, multimedia, simulations)
- Distributed rendering and processing
- Format conversion and optimization

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                Batch Processing Service                  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Job       │  │  Pipeline   │  │  Resource   │    │
│  │ Scheduler   │  │  Manager    │  │  Manager    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Spark     │  │   ML        │  │  Distributed│    │
│  │  Runtime    │  │  Libraries  │  │  Storage    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Consolidated Jobs

### 1. ML Training Jobs
- **Asset Classifier**: Digital asset classification
- **Anomaly Predictor**: Predictive anomaly detection
- **Derivatives ML**: Trading model training
- **Simulation ML**: Simulation optimization models

### 2. Data Processing Jobs
- **Feature Selection**: Automated feature engineering
- **Trust Ranking**: Entity trust score calculation
- **DAG Optimization**: Pipeline optimization

### 3. Distributed Processing
- **Blender Distributed**: Parallel rendering
- **GraphX Analytics**: Large-scale graph processing
- **Federated Learning**: Privacy-preserving ML

### 4. File Processing (Enhanced)
#### CAD Processing (FreeCAD Processor)
- **Supported Formats**: FCStd, STEP, IGES, STL, OBJ, DXF, DWG
- **Capabilities**: Format conversion, geometry analysis, mesh generation
- **Distributed**: Parallel processing of large assemblies

#### 3D Rendering (Blender Processor)
- **Supported Formats**: .blend, .blend1, .blend2
- **Capabilities**: Distributed GPU rendering, animation processing
- **Features**: Scene optimization, texture baking, format export

#### Multimedia Processing (Unified Processor)
- **Image Formats**: PNG, JPG, TIFF, BMP, RAW, PSD, SVG
- **Audio Formats**: MP3, WAV, FLAC, OGG, AAC, M4A
- **Video Formats**: MP4, AVI, MOV, MKV, WebM, FLV
- **Capabilities**: Transcoding, filtering, analysis, thumbnail generation

#### Simulation Processing
- **OpenFOAM**: CFD simulations, mesh processing, result visualization
- **FlightGear**: Flight data analysis, scenario processing
- **Features**: Distributed solver runs, result aggregation

## API Endpoints

```yaml
POST   /api/v1/jobs                    # Submit batch job
GET    /api/v1/jobs                    # List jobs
GET    /api/v1/jobs/{id}               # Get job status
DELETE /api/v1/jobs/{id}               # Cancel job
GET    /api/v1/jobs/{id}/logs          # Get job logs
POST   /api/v1/pipelines               # Create pipeline
GET    /api/v1/pipelines               # List pipelines
PUT    /api/v1/pipelines/{id}          # Update pipeline

# File Processing Endpoints
GET    /api/v1/processors/formats      # List supported formats
GET    /api/v1/processors/{type}       # Get processor info
POST   /api/v1/processors/process      # Process single file
POST   /api/v1/processors/process/batch # Process multiple files
POST   /api/v1/processors/upload/process # Upload and process
```

## Job Types

### 1. Spark SQL Jobs
```python
@batch_job("daily_aggregation")
def daily_aggregation_job(spark: SparkSession, params: Dict):
    """Daily aggregation job"""
    df = spark.read.parquet(f"s3a://data-lake/{params['date']}")
    
    aggregated = df.groupBy("tenant_id", "asset_type") \
        .agg(
            F.count("*").alias("count"),
            F.sum("size_bytes").alias("total_size"),
            F.avg("processing_time").alias("avg_time")
        )
    
    aggregated.write.mode("overwrite") \
        .partitionBy("tenant_id") \
        .parquet(f"s3a://aggregated/{params['date']}")
```

### 2. File Processing Jobs
```python
from batch_processing import ProcessorClient

client = ProcessorClient()

# Process a single CAD file
response = client.process_file(
    file_path="s3://models/part123.step",
    processor_type="freecad",
    options={
        "extract_metadata": True,
        "generate_thumbnail": True,
        "output_format": "stl"
    }
)

# Batch process multiple files
response = client.process_batch(
    file_paths=[
        "s3://renders/scene1.blend",
        "s3://renders/scene2.blend"
    ],
    options={
        "resolution": "1920x1080",
        "samples": 256,
        "use_gpu": True
    }
)
```

### 3. ML Training Jobs
```python
@ml_training_job("asset_classifier")
class AssetClassifierJob(MLJobBase):
    def prepare_features(self, df: DataFrame) -> DataFrame:
        # Feature engineering
        return df.withColumn("features", 
            vector_assembler(["size", "type", "metadata"]))
    
    def train_model(self, df: DataFrame) -> MLModel:
        rf = RandomForestClassifier(numTrees=100)
        return rf.fit(df)
    
    def evaluate(self, model: MLModel, test_df: DataFrame) -> Dict:
        predictions = model.transform(test_df)
        return {"accuracy": 0.95, "f1": 0.93}
```

### 3. ETL Pipeline Jobs
```yaml
pipeline:
  name: data_enrichment
  schedule: "0 2 * * *"  # Daily at 2 AM
  
  stages:
    - name: extract
      type: spark_sql
      query: |
        SELECT * FROM raw_data
        WHERE date = '{{ ds }}'
    
    - name: transform
      type: python
      function: transform_data
      params:
        deduplicate: true
        normalize: true
    
    - name: load
      type: spark_write
      format: parquet
      mode: append
      path: s3a://processed/{{ ds }}
```

## Resource Management

```yaml
resource-profiles:
  small:
    executor-memory: 2g
    executor-cores: 2
    max-executors: 10
  
  medium:
    executor-memory: 4g
    executor-cores: 4
    max-executors: 20
    
  large:
    executor-memory: 8g
    executor-cores: 8
    max-executors: 50
    gpu: true
```

## Job Submission

### 1. Via API
```bash
curl -X POST /jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "ml_training",
    "name": "asset_classifier",
    "params": {
      "training_data": "s3a://data/train",
      "model_name": "asset_classifier_v2"
    },
    "resource_profile": "medium"
  }'
```

### 2. Via Python SDK
```python
from batch_processing import BatchClient

client = BatchClient()
job = client.submit_job(
    type="spark_sql",
    sql="SELECT * FROM events WHERE date > '2024-01-01'",
    output_path="s3a://results/events"
)
print(f"Job {job.id} submitted")
```

## Monitoring & Optimization

### Performance Metrics
- Job execution time
- Resource utilization
- Data skew detection
- Shuffle optimization

### Auto-optimization
```python
@auto_optimize
def complex_job(spark: SparkSession):
    # Service automatically:
    # - Adjusts partition sizes
    # - Enables adaptive query execution
    # - Optimizes join strategies
    # - Manages broadcast thresholds
    pass
```

## Integration Points

- **MinIO**: Data lake storage
- **MLflow**: Model tracking
- **Airflow**: Workflow orchestration
- **Ignite**: Intermediate results caching
- **Pulsar**: Event notifications

## Migration Guide

### From Standalone Spark Job
```python
# Old: Standalone script
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MyJob").getOrCreate()
    # Job logic
    
# New: Service module
@batch_job("my_job")
def my_job(spark: SparkSession, params: Dict):
    # Same job logic
    pass
```

### From Airflow DAG
```python
# Old: Airflow DAG with SparkSubmitOperator
spark_task = SparkSubmitOperator(
    task_id='spark_job',
    application='job.py'
)

# New: BatchProcessingOperator
batch_task = BatchProcessingOperator(
    task_id='spark_job',
    job_type='spark_sql',
    query='SELECT * FROM table'
)
``` 