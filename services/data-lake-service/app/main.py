"""
Data Lake Service

Provides medallion architecture data lake with Bronze, Silver, Gold layers.
Integrates with Apache Spark, MinIO, and various data sources.
"""

import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession
from minio import Minio
import uvicorn

from .models.medallion_architecture import MedallionArchitecture, DataLayer, DataQualityRule
from .ingestion.data_ingestion import DataIngestionPipeline, IngestionConfig
from .quality.data_quality_framework import DataQualityFramework
from .processing.layer_processing import LayerProcessingPipeline, ProcessingConfig
from .api.endpoints import router as api_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
spark: Optional[SparkSession] = None
minio_client: Optional[Minio] = None
medallion: Optional[MedallionArchitecture] = None
ingestion_pipeline: Optional[DataIngestionPipeline] = None
quality_framework: Optional[DataQualityFramework] = None
processing_pipeline: Optional[LayerProcessingPipeline] = None


# Pydantic models
class IngestionRequest(BaseModel):
    source_type: str = Field(..., description="Type of data source: pulsar, api, database, file, iot")
    source_config: Dict[str, Any] = Field(..., description="Source-specific configuration")
    target_dataset: str = Field(..., description="Target dataset name")
    ingestion_mode: str = Field(..., description="Ingestion mode: batch, streaming, incremental")
    schedule: Optional[str] = Field(None, description="Cron expression for scheduled runs")
    transformations: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    quality_checks: Optional[List[str]] = Field(default_factory=list)


class ProcessingRequest(BaseModel):
    source_layer: str = Field(..., description="Source layer: bronze, silver")
    target_layer: str = Field(..., description="Target layer: silver, gold")
    dataset_name: str = Field(..., description="Dataset name")
    transformations: List[Dict[str, Any]] = Field(..., description="List of transformations to apply")
    quality_threshold: float = Field(0.90, description="Minimum quality score required")
    partition_strategy: Optional[Dict[str, Any]] = Field(None)
    optimization_config: Optional[Dict[str, Any]] = Field(None)
    schedule: Optional[str] = Field(None, description="Cron expression for scheduled runs")


class QualityCheckRequest(BaseModel):
    dataset_name: str = Field(..., description="Dataset name")
    layer: str = Field(..., description="Data layer: bronze, silver, gold")
    rules: Optional[List[str]] = Field(None, description="Specific rules to apply")


class DataCatalogQuery(BaseModel):
    layer: Optional[str] = Field(None, description="Filter by layer")
    dataset_pattern: Optional[str] = Field(None, description="Dataset name pattern")
    include_schema: bool = Field(True, description="Include schema information")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    await startup_event()
    yield
    # Shutdown
    await shutdown_event()


# Create FastAPI app
app = FastAPI(
    title="PlatformQ Data Lake Service",
    description="Medallion architecture data lake with quality framework",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def startup_event():
    """Initialize services on startup"""
    global spark, minio_client, medallion, ingestion_pipeline, quality_framework, processing_pipeline
    
    logger.info("Initializing Data Lake Service...")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PlatformQ-DataLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
        
    # Initialize MinIO client
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )
    
    # Initialize medallion architecture
    medallion = MedallionArchitecture(
        spark_session=spark,
        minio_client=minio_client,
        bucket_name=os.getenv("DATA_LAKE_BUCKET", "platformq-data-lake"),
        delta_enabled=True
    )
    
    # Initialize quality framework
    quality_framework = DataQualityFramework(spark=spark)
    
    # Initialize pipelines
    ingestion_pipeline = DataIngestionPipeline(
        spark=spark,
        medallion=medallion
    )
    
    processing_pipeline = LayerProcessingPipeline(
        spark=spark,
        medallion=medallion,
        quality_framework=quality_framework
    )
    
    logger.info("Data Lake Service initialized successfully")


async def shutdown_event():
    """Cleanup on shutdown"""
    global spark
    
    logger.info("Shutting down Data Lake Service...")
    
    if spark:
        spark.stop()
        
    logger.info("Data Lake Service shutdown complete")


# Include API router
app.include_router(api_router, prefix="/api/v1")


# API Endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "data-lake-service",
        "timestamp": datetime.utcnow().isoformat(),
        "spark_active": spark is not None,
        "minio_active": minio_client is not None
    }


@app.post("/api/v1/ingest")
async def start_ingestion(
    request: IngestionRequest,
    background_tasks: BackgroundTasks
):
    """Start a new data ingestion job"""
    if not ingestion_pipeline:
        raise HTTPException(status_code=503, detail="Ingestion pipeline not initialized")
        
    try:
        config = IngestionConfig(
            source_type=request.source_type,
            source_config=request.source_config,
            target_dataset=request.target_dataset,
            ingestion_mode=request.ingestion_mode,
            schedule=request.schedule,
            transformations=request.transformations,
            quality_checks=request.quality_checks
        )
        
        job_id = await ingestion_pipeline.start_ingestion(config)
        
        return {
            "job_id": job_id,
            "status": "started",
            "config": request.dict()
        }
        
    except Exception as e:
        logger.error(f"Failed to start ingestion: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/ingest/{job_id}")
async def get_ingestion_status(job_id: str):
    """Get status of an ingestion job"""
    if not ingestion_pipeline:
        raise HTTPException(status_code=503, detail="Ingestion pipeline not initialized")
        
    job = ingestion_pipeline.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {
        "job_id": job.job_id,
        "status": job.status,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "records_processed": job.records_processed,
        "bronze_path": job.bronze_path,
        "errors": job.errors
    }


@app.post("/api/v1/process")
async def start_processing(
    request: ProcessingRequest,
    background_tasks: BackgroundTasks
):
    """Start a layer processing job"""
    if not processing_pipeline:
        raise HTTPException(status_code=503, detail="Processing pipeline not initialized")
        
    try:
        config = ProcessingConfig(
            source_layer=DataLayer(request.source_layer),
            target_layer=DataLayer(request.target_layer),
            dataset_name=request.dataset_name,
            transformations=request.transformations,
            quality_threshold=request.quality_threshold,
            partition_strategy=request.partition_strategy,
            optimization_config=request.optimization_config,
            schedule=request.schedule
        )
        
        job_id = await processing_pipeline.start_processing(config)
        
        return {
            "job_id": job_id,
            "status": "started",
            "config": request.dict()
        }
        
    except Exception as e:
        logger.error(f"Failed to start processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/process/{job_id}")
async def get_processing_status(job_id: str):
    """Get status of a processing job"""
    if not processing_pipeline:
        raise HTTPException(status_code=503, detail="Processing pipeline not initialized")
        
    job = processing_pipeline.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {
        "job_id": job.job_id,
        "status": job.status,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "source_paths": job.source_paths,
        "target_path": job.target_path,
        "quality_score": job.quality_score,
        "records_processed": job.records_processed,
        "errors": job.errors
    }


@app.post("/api/v1/quality/check")
async def run_quality_check(request: QualityCheckRequest):
    """Run quality check on a dataset"""
    if not quality_framework or not medallion:
        raise HTTPException(status_code=503, detail="Quality framework not initialized")
        
    try:
        # Get latest data path
        layer = DataLayer(request.layer)
        paths = processing_pipeline._get_latest_paths(layer, request.dataset_name)
        
        if not paths:
            raise HTTPException(status_code=404, detail="Dataset not found")
            
        # Read data
        df = processing_pipeline._read_and_union_data(paths)
        
        # Run quality profile
        profile = quality_framework.profile_dataset(
            df,
            f"{request.dataset_name}_{layer.value}",
            rules=request.rules
        )
        
        # Generate report
        report = quality_framework.generate_quality_report(
            request.dataset_name,
            profile
        )
        
        return report
        
    except Exception as e:
        logger.error(f"Failed to run quality check: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/catalog")
async def get_data_catalog(
    layer: Optional[str] = None,
    dataset_pattern: Optional[str] = None,
    include_schema: bool = True
):
    """Get data catalog"""
    if not medallion:
        raise HTTPException(status_code=503, detail="Medallion architecture not initialized")
        
    try:
        catalog = medallion.generate_data_catalog()
        
        # Filter by layer if specified
        if layer:
            catalog["layers"] = {
                k: v for k, v in catalog["layers"].items()
                if k == layer
            }
            
        # Filter by dataset pattern
        if dataset_pattern:
            for layer_name, layer_data in catalog["layers"].items():
                layer_data["datasets"] = [
                    d for d in layer_data["datasets"]
                    if dataset_pattern in d["name"]
                ]
                
        # Remove schema if not requested
        if not include_schema:
            for layer_name, layer_data in catalog["layers"].items():
                for dataset in layer_data["datasets"]:
                    dataset.pop("schema", None)
                    
        return catalog
        
    except Exception as e:
        logger.error(f"Failed to get catalog: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/lineage/{dataset_id}")
async def get_data_lineage(dataset_id: str):
    """Get data lineage for a dataset"""
    if not medallion:
        raise HTTPException(status_code=503, detail="Medallion architecture not initialized")
        
    lineage = medallion.get_lineage(dataset_id)
    
    return {
        "dataset_id": dataset_id,
        "lineage": [
            {
                "dataset_id": l.dataset_id,
                "source_layer": l.source_layer.value,
                "target_layer": l.target_layer.value,
                "transformation_id": l.transformation_id,
                "timestamp": l.timestamp.isoformat(),
                "input_datasets": l.input_datasets,
                "output_dataset": l.output_dataset,
                "quality_checks_passed": l.quality_checks_passed,
                "quality_checks_failed": l.quality_checks_failed
            }
            for l in lineage
        ]
    }


@app.post("/api/v1/quality/rules")
async def add_quality_rule(rule: Dict[str, Any]):
    """Add a custom quality rule"""
    if not medallion:
        raise HTTPException(status_code=503, detail="Medallion architecture not initialized")
        
    try:
        quality_rule = DataQualityRule(
            rule_id=rule["rule_id"],
            name=rule["name"],
            description=rule["description"],
            check_function=eval(rule["check_function"]),  # In production, use safe evaluation
            severity=rule.get("severity", "warning"),
            active=rule.get("active", True)
        )
        
        layer = DataLayer(rule["layer"])
        medallion.add_quality_rule(layer, quality_rule)
        
        return {"message": "Quality rule added successfully", "rule_id": rule["rule_id"]}
        
    except Exception as e:
        logger.error(f"Failed to add quality rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/compact/{layer}")
async def compact_delta_tables(
    layer: str,
    background_tasks: BackgroundTasks
):
    """Compact Delta tables in a layer"""
    if not medallion:
        raise HTTPException(status_code=503, detail="Medallion architecture not initialized")
        
    try:
        data_layer = DataLayer(layer)
        background_tasks.add_task(medallion.compact_delta_tables, data_layer)
        
        return {
            "message": f"Compaction started for {layer} layer",
            "status": "started"
        }
        
    except Exception as e:
        logger.error(f"Failed to start compaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/jobs")
async def list_active_jobs():
    """List all active jobs"""
    ingestion_jobs = []
    processing_jobs = []
    
    if ingestion_pipeline:
        ingestion_jobs = [
            {
                "job_id": job.job_id,
                "type": "ingestion",
                "status": job.status,
                "dataset": job.config.target_dataset,
                "started_at": job.started_at.isoformat() if job.started_at else None
            }
            for job in ingestion_pipeline.list_active_jobs()
        ]
        
    if processing_pipeline:
        processing_jobs = [
            {
                "job_id": job.job_id,
                "type": "processing",
                "status": job.status,
                "dataset": job.config.dataset_name,
                "transition": f"{job.config.source_layer.value} -> {job.config.target_layer.value}",
                "started_at": job.started_at.isoformat() if job.started_at else None
            }
            for job in processing_pipeline.list_active_jobs()
        ]
        
    return {
        "total_active": len(ingestion_jobs) + len(processing_jobs),
        "ingestion_jobs": ingestion_jobs,
        "processing_jobs": processing_jobs
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 