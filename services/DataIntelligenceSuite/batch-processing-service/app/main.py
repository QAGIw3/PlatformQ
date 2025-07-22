"""
Batch Processing Service

Unified service for all batch processing needs,
consolidating multiple Spark jobs into a single scalable service.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
from pyspark.sql import SparkSession

from app.core.config import settings
from app.core.spark_manager import SparkManager
from app.core.job_scheduler import JobScheduler
from app.core.resource_manager import ResourceManager
from app.api import jobs, pipelines, health, metrics
from app.middleware.error_handler import error_handler_middleware
from app.middleware.logging import logging_middleware

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
spark_manager: Optional[SparkManager] = None
job_scheduler: Optional[JobScheduler] = None
resource_manager: Optional[ResourceManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global spark_manager, job_scheduler, resource_manager
    
    # Startup
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    # Initialize Spark
    spark_manager = SparkManager(settings)
    await spark_manager.initialize()
    
    # Initialize job scheduler
    job_scheduler = JobScheduler(settings, spark_manager)
    await job_scheduler.start()
    
    # Initialize resource manager
    resource_manager = ResourceManager(settings)
    await resource_manager.start()
    
    # Register with service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import register_service
        await register_service(settings)
    
    logger.info("Batch Processing Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Batch Processing Service")
    
    # Stop components
    await job_scheduler.stop()
    await resource_manager.stop()
    await spark_manager.cleanup()
    
    # Deregister from service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import deregister_service
        await deregister_service(settings)
    
    logger.info("Batch Processing Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    description="Unified batch processing service for large-scale data processing",
    version=settings.service_version,
    lifespan=lifespan
)

# Add middleware
app.middleware("http")(error_handler_middleware)
app.middleware("http")(logging_middleware)

# Include routers
app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/v1/info")
async def service_info():
    """Get service information"""
    return {
        "service": {
            "name": settings.service_name,
            "version": settings.service_version,
            "environment": settings.environment
        },
        "capabilities": {
            "spark_sql": True,
            "ml_training": True,
            "etl_pipelines": True,
            "distributed_processing": True,
            "graphx": True
        },
        "job_types": [
            "spark_sql",
            "ml_training",
            "etl_pipeline",
            "feature_engineering",
            "graph_processing"
        ],
        "resource_profiles": ["small", "medium", "large", "xlarge"],
        "integrations": {
            "storage": ["s3", "minio", "hdfs"],
            "databases": ["cassandra", "elasticsearch", "postgres"],
            "ml": ["mlflow", "tensorflow", "pytorch"]
        }
    }


class JobSubmitRequest(BaseModel):
    """Job submission request"""
    name: str
    type: str
    config: Dict[str, Any]
    resource_profile: Optional[str] = "medium"
    priority: Optional[int] = 5
    schedule: Optional[str] = None  # Cron expression for scheduled jobs
    

@app.post("/api/v1/submit")
async def submit_job(request: JobSubmitRequest, background_tasks: BackgroundTasks):
    """Submit a new batch job"""
    try:
        # Validate job type
        valid_types = ["spark_sql", "ml_training", "etl_pipeline", 
                      "feature_engineering", "graph_processing"]
        if request.type not in valid_types:
            raise HTTPException(400, f"Invalid job type. Must be one of: {valid_types}")
        
        # Submit job
        job_id = await job_scheduler.submit_job(
            name=request.name,
            job_type=request.type,
            config=request.config,
            resource_profile=request.resource_profile,
            priority=request.priority,
            schedule=request.schedule
        )
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": f"Job {request.name} submitted successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to submit job: {e}")
        raise HTTPException(500, f"Failed to submit job: {str(e)}")


@app.get("/api/v1/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Get job status"""
    try:
        status = await job_scheduler.get_job_status(job_id)
        if not status:
            raise HTTPException(404, f"Job {job_id} not found")
        return status
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(500, f"Failed to get job status: {str(e)}")


@app.delete("/api/v1/jobs/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running job"""
    try:
        result = await job_scheduler.cancel_job(job_id)
        if not result:
            raise HTTPException(404, f"Job {job_id} not found")
        return {"message": f"Job {job_id} cancelled successfully"}
    except Exception as e:
        logger.error(f"Failed to cancel job: {e}")
        raise HTTPException(500, f"Failed to cancel job: {str(e)}")


@app.get("/api/v1/jobs/{job_id}/logs")
async def get_job_logs(job_id: str, lines: int = 100):
    """Get job logs"""
    try:
        logs = await job_scheduler.get_job_logs(job_id, lines)
        if logs is None:
            raise HTTPException(404, f"Job {job_id} not found")
        return {"job_id": job_id, "logs": logs}
    except Exception as e:
        logger.error(f"Failed to get job logs: {e}")
        raise HTTPException(500, f"Failed to get job logs: {str(e)}")


class SparkSQLRequest(BaseModel):
    """Spark SQL execution request"""
    query: str
    output_format: Optional[str] = "json"
    limit: Optional[int] = 1000
    

@app.post("/api/v1/sql")
async def execute_sql(request: SparkSQLRequest):
    """Execute Spark SQL query"""
    try:
        result = await spark_manager.execute_sql(
            query=request.query,
            output_format=request.output_format,
            limit=request.limit
        )
        return {
            "status": "success",
            "row_count": len(result["data"]),
            "schema": result["schema"],
            "data": result["data"]
        }
    except Exception as e:
        logger.error(f"Failed to execute SQL: {e}")
        raise HTTPException(500, f"Failed to execute SQL: {str(e)}")


@app.get("/api/v1/resources")
async def get_resource_status():
    """Get cluster resource status"""
    try:
        status = await resource_manager.get_cluster_status()
        return status
    except Exception as e:
        logger.error(f"Failed to get resource status: {e}")
        raise HTTPException(500, f"Failed to get resource status: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    ) 