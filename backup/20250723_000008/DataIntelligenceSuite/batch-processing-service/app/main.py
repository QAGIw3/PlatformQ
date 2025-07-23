"""
Batch Processing Service

Unified service for all batch processing needs,
consolidating multiple Spark jobs into a single, scalable service.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import hvac
import consul.aio

from data_intelligence_common import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    StructuredLogger,
    create_data_intelligence_app
)

from app.core.config import settings
from app.core.spark_manager import SparkManager
from app.core.job_scheduler import JobScheduler
from app.core.pipeline_manager import PipelineManager
from app.core.ml_training_manager import MLTrainingManager
from app.core.processor_manager import ProcessorManager
from app.api import jobs, pipelines, ml_training, monitoring, health, processors

# Configure logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="batch-processing-service",
    version="1.0.0",
    description="Unified batch processing with Apache Spark for ETL and ML",
    dependencies=["spark", "minio", "cassandra", "elasticsearch", "mlflow"],
    health_checks=["spark", "scheduler", "storage"],
    capabilities=["batch-processing", "ml-training", "etl", "analytics"],
    data_sources=["s3", "cassandra", "postgres", "parquet"],
    data_outputs=["s3", "cassandra", "elasticsearch", "mlflow"],
    min_memory_mb=8192,
    min_cpu_cores=4
)

# Global instances
spark_manager: Optional[SparkManager] = None
job_scheduler: Optional[JobScheduler] = None
pipeline_manager: Optional[PipelineManager] = None
ml_training_manager: Optional[MLTrainingManager] = None
processor_manager: Optional[ProcessorManager] = None


class BatchProcessingService(DataIntelligenceBaseService):
    """Batch Processing Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.spark_manager = None
        self.job_scheduler = None
        self.pipeline_manager = None
        self.ml_training_manager = None
        self.processor_manager = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global spark_manager, job_scheduler, pipeline_manager, ml_training_manager, processor_manager
        
        logger.info("Initializing Batch Processing Service components...")
        
        # Get Spark configuration from Vault
        spark_config = await self.vault_consul.get_secret("compute/spark")
        if spark_config:
            settings.spark_master = spark_config.get("master", settings.spark_master)
            os.environ["SPARK_HOME"] = spark_config.get("spark_home", "/opt/spark")
        
        # Get MinIO credentials from Vault
        minio_creds = await self.vault_consul.get_secret("storage/minio")
        if minio_creds:
            settings.minio_access_key = minio_creds.get("access_key", settings.minio_access_key)
            settings.minio_secret_key = minio_creds.get("secret_key", settings.minio_secret_key)
            # Set AWS credentials for Spark S3 access
            os.environ["AWS_ACCESS_KEY_ID"] = minio_creds.get("access_key", "")
            os.environ["AWS_SECRET_ACCESS_KEY"] = minio_creds.get("secret_key", "")
        
        # Get Cassandra credentials from Vault
        cassandra_creds = await self.vault_consul.get_database_credentials("cassandra")
        if cassandra_creds:
            os.environ["CASSANDRA_USERNAME"] = cassandra_creds.get("username", "")
            os.environ["CASSANDRA_PASSWORD"] = cassandra_creds.get("password", "")
        
        # Get MLflow configuration from Vault
        mlflow_config = await self.vault_consul.get_secret("ml/mlflow")
        if mlflow_config:
            settings.mlflow_tracking_uri = mlflow_config.get("tracking_uri", settings.mlflow_tracking_uri)
            os.environ["MLFLOW_TRACKING_URI"] = settings.mlflow_tracking_uri
        
        # Initialize components
        spark_manager = SparkManager(settings)
        await spark_manager.initialize()
        self.spark_manager = spark_manager
        
        job_scheduler = JobScheduler(settings, spark_manager)
        await job_scheduler.start()
        self.job_scheduler = job_scheduler
        
        pipeline_manager = PipelineManager(settings, spark_manager, job_scheduler)
        await pipeline_manager.initialize()
        self.pipeline_manager = pipeline_manager
        
        ml_training_manager = MLTrainingManager(settings, spark_manager)
        await ml_training_manager.initialize()
        self.ml_training_manager = ml_training_manager
        
        processor_manager = ProcessorManager(settings, job_scheduler)
        self.processor_manager = processor_manager
        
        # Inject dependencies into API routers
        jobs.spark_manager = spark_manager
        jobs.job_scheduler = job_scheduler
        pipelines.pipeline_manager = pipeline_manager
        ml_training.ml_training_manager = ml_training_manager
        processors.processor_manager = processor_manager
        monitoring.spark_manager = spark_manager
        monitoring.job_scheduler = job_scheduler
        health.spark_manager = spark_manager
        health.job_scheduler = job_scheduler
        
        # Register health checks
        self.health_manager.register_check("spark", spark_manager.health_check, critical=True)
        self.health_manager.register_check("scheduler", job_scheduler.health_check)
        self.health_manager.register_check("storage", self._check_storage_health)
        
        logger.info("Batch Processing Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Batch Processing Service...")
        
        if self.job_scheduler:
            await self.job_scheduler.stop()
        
        if self.spark_manager:
            await self.spark_manager.cleanup()
        
        logger.info("Batch Processing Service cleaned up")
    
    async def _check_storage_health(self) -> bool:
        """Check storage connectivity"""
        # Check MinIO connectivity
        try:
            from minio import Minio
            client = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure
            )
            # Try to list buckets
            buckets = client.list_buckets()
            return True
        except Exception:
            return False


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Get configuration from environment
    vault_addr = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_host = os.getenv("CONSUL_HOST", "localhost")
    consul_port = int(os.getenv("CONSUL_PORT", "8500"))
    consul_token = os.getenv("CONSUL_TOKEN")
    
    # Create Vault client
    vault_client = hvac.Client(url=vault_addr, token=vault_token)
    
    # Create Consul client
    consul_client = consul.aio.Consul(
        host=consul_host,
        port=consul_port,
        token=consul_token
    )
    
    # Create service instance
    service = BatchProcessingService(
        vault_client=vault_client,
        consul_client=consul_client
    )
    
    # Create app with common setup
    app = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        service_instance=service,
        title="Batch Processing Service API",
        include_common_middleware=True
    )
    
    # Include API routers
    app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
    app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
    app.include_router(ml_training.router, prefix="/api/v1/ml", tags=["ml-training"])
    app.include_router(processors.router, tags=["processors"])
    app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
    app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
    
    # Add root endpoint
    @app.get("/")
    async def root():
        return {
            "service": SERVICE_METADATA.name,
            "version": SERVICE_METADATA.version,
            "status": "running",
            "endpoints": {
                "jobs": "/api/v1/jobs",
                "pipelines": "/api/v1/pipelines",
                "ml_training": "/api/v1/ml",
                "processors": "/api/v1/processors",
                "monitoring": "/api/v1/monitoring",
                "health": "/api/v1/health"
            }
        }
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", settings.api_port))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    ) 